import sys
import signal
import time
from consumers.rabbitmq import RabbitMQConsumer
import requests
from setup import creds

from setup.error_handler import ProcessInErrorHandler, ProcessOutErrorHandler, LeadFormErrorHandler
from traceback import format_exc as tb
from datetime import datetime

from integration.draft_orders import on_draft_created, on_draft_updated
from customer_tools.customers import lookup_customer, add_new_customer
import threading
from setup.sms_engine import SMSEngine
from setup.utilities import PhoneNumber
from integration.shopify_api import Shopify
from integration.orders import Order as ShopifyOrder
from setup.print_engine import Printer
from setup.email_engine import Email
from database import Database
from docxtpl import DocxTemplate
import os
import json
from integrator import Integrator


def sync_on_demand(phone_number):
    error_handler = ProcessOutErrorHandler.error_handler
    phone_number = PhoneNumber(phone_number).to_twilio()
    SMSEngine.send_text(
        origin='SERVER', campaign='SYNC_ON_DEMAND', to_phone=phone_number, message='Syncing data. Please wait...'
    )
    phone_response = None

    try:
        integrator = Integrator()
        integrator.sync()

    except Exception as e:
        error_handler.add_error_v(error=f'Error: {e}', origin='sync_on_demand')
        phone_response = 'Sync failed. Please check logs.'

    else:
        phone_response = f'Sync completed successfully at {datetime.now():%m/%d/%Y %H:%M:%S}'
    finally:
        SMSEngine.send_text(origin='sync_on_demand', to_phone=phone_number, message=phone_response)


def process_design_lead(body, eh=LeadFormErrorHandler, test_mode=False):
    logger = eh.logger
    error_handler = eh.error_handler
    json_body = json.loads(body)
    first_name = json_body['first_name']
    last_name = json_body['last_name']
    email = json_body['email']
    phone = PhoneNumber(json_body['phone']).to_cp()
    timeline = json_body['timeline']
    interested_in = json_body['interested_in']
    street = str(json_body['street']).replace(',', '')
    city = str(json_body['city']).replace(',', '')
    state = json_body['state'] if json_body['state'] != 'State' else ''
    zip_code = str(json_body['zip_code']).replace(',', '')
    comments = str(json_body['comments']).replace('"', '""')
    # Concat the address
    address = f'{street}, {city}, {state}, {zip_code}'

    # Concatenate user interests (for text and spreadsheet use)
    interests = ''
    if interested_in is not None:
        for x in interested_in:
            interests += x
            if len(interested_in) > 1:
                interests += ', '
        if len(interested_in) > 1:
            # remove last trailing characters (", ")
            interests = interests[:-2]

    logger.info(f'Received message from {first_name} {last_name}. Beginning Processing...')
    # Check if this is a current customer
    cust_no = lookup_customer(phone_number=phone, email_address=email)

    if not cust_no:
        # Add new customer if not found
        try:
            cust_no = add_new_customer(
                first_name=first_name,
                last_name=last_name,
                phone_number=phone,
                email_address=email,
                street_address=street,
                city=city,
                state=state,
                zip_code=zip_code,
            )
        except Exception as err:
            error_handler.add_error_v(f'Error - Add New Customer: {err}', origin='design_lead')
            cust_no = 'Unknown'

    # establish start time for consistent logging
    now = datetime.now()
    now_log_format = f'{now:%Y-%m-%d %H:%M:%S}'

    Database.DesignLead.insert(
        date=now_log_format,
        cust_no=cust_no,
        first_name=first_name,
        last_name=last_name,
        email=email,
        phone=phone,
        interested_in=interested_in,
        timeline=timeline,
        street=street,
        city=city,
        state=state,
        zip_code=zip_code,
        comments=comments,
    )

    # Send text notification To sales team manager
    logger.info('Sending SMS Message to Sales Team')
    try:
        SMSEngine.design_text(
            first_name, last_name, email, phone, interests, timeline, address, comments, test_mode=test_mode
        )
    except Exception as err:
        error_handler.add_error_v(f'Error (sms): {err}', origin='design_lead')
    else:
        logger.success(f'SMS Sent at {datetime.now():%H:%M:%S}')

    # Send email to client
    logger.info('Sending Email to Lead')
    try:
        Email.Customer.DesignLead.send(first_name, email)
    except Exception as err:
        error_handler.add_error_v(error=f'Error (email): {err}', origin='design_lead')
    else:
        logger.success(f'Email Sent at {datetime.now():%H:%M:%S}')
    # Print lead details for in-store use

    # Create the Word document
    logger.info('Rendering Word Document')
    try:
        doc = DocxTemplate('./templates/design_lead/lead_print_template.docx')

        context = {
            # Product Details
            'date': now_log_format,
            'name': first_name + ' ' + last_name,
            'email': email,
            'phone': phone,
            'interested_in': interested_in,
            'timeline': timeline,
            'address': address,
            'comments': comments.replace('""', '"'),
        }

        doc.render(context)
        ticket_name = f'lead_{now:%H_%M_%S}.docx'
        # Save the rendered file for printing
        doc.save(f'./{ticket_name}')
        # Print the file to default printer
        logger.info('Printing Word Document')
        if test_mode:
            logger.info('Test Mode: Skipping Print')
        else:
            os.startfile(ticket_name, 'print')
        # Delay while print job executes
        time.sleep(4)
        logger.info('Deleting Word Document')
        os.remove(ticket_name)
    except Exception as err:
        error_handler.add_error_v(error=f'Error (word): {err}', origin='design_lead')
    else:
        logger.success(f'Word Document created, printed, and deleted at {datetime.now():%H:%M:%S}')

    # Upload to sheety API for spreadsheet use
    logger.info('Sending Details to Google Sheets')
    sheety_post_body = {
        'sheet1': {
            'date': now_log_format,
            'first': first_name,
            'last': last_name,
            'phone': phone,
            'email': email,
            'interested': interests,
            'timeline': timeline,
            'street': street,
            'city': city,
            'state': state,
            'zip': zip_code,
            'comments': comments,
        }
    }
    try:
        # Try block stands to decouple our implementation from API changes that might impact app.
        requests.post(
            url=creds.Sheety.design_url, headers={'authorization': creds.Sheety.token}, json=sheety_post_body
        )
    except Exception as err:
        error_handler.add_error_v(error=f'Error (sheety): {err}', origin='design_lead')
    else:
        logger.success(f'Sent to Google Sheets at {datetime.now():%H:%M:%S}')
    # Done
    logger.success(f'Processing Completed at {datetime.now():%H:%M:%S}\n')


def process_shopify_order(order_id, eh=ProcessInErrorHandler):
    eh.logger.info(f'Beginning processing for Order #{order_id}')
    time.sleep(5)  # <-- This is to give payment processor time to complete
    order = Shopify.Order.as_bc_order(order_id=order_id)  # Convert order to BC Order dictionary
    shopify_order = ShopifyOrder(order_id)
    shopify_order.post_shopify_order()
    eh.logger.info(f'Order {order_id} processed successfully')

    # PRINTING - Filter out DECLINED payments
    if order['status'] == 'UNFULFILLED' or order['status'] == 'FULFILLED':
        Printer.print_order(order_id)

    elif order['status'] == 'Partially Refunded':
        eh.error_handler.add_error_v(
            error=f'Order {order_id} was partially refunded. Skipping...', origin='Design Consumer'
        )
    elif order['status'] == 'ON_HOLD':
        eh.logger.info(message=f'Order {order_id} is on hold. Skipping...for now...')
    else:
        eh.logger.info(message=f'Order {order_id} status is {order['status']}. Skipping...')


def shutdown_handler(signum, frame):
    print('Received shutdown signal, stopping consumers...')
    for consumer in consumers:
        consumer.stop_consuming()
    for thread in threads:
        thread.join()
    sys.exit(0)


if __name__ == '__main__':
    try:
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        threads = []
        consumers = []

        queues = [
            {
                'queue_name': creds.Consumer.draft_create,
                'callback': on_draft_created,
                'error_handler': ProcessInErrorHandler,
            },
            {
                'queue_name': creds.Consumer.draft_update,
                'callback': on_draft_updated,
                'error_handler': ProcessInErrorHandler,
            },
            {
                'queue_name': creds.Consumer.sync_on_demand,
                'callback': sync_on_demand,
                'error_handler': ProcessOutErrorHandler,
            },
            {
                'queue_name': creds.Consumer.orders,
                'callback': process_shopify_order,
                'error_handler': ProcessInErrorHandler,
            },
            {
                'queue_name': creds.Consumer.design_lead_form,
                'callback': process_design_lead,
                'error_handler': LeadFormErrorHandler,
            },
        ]

        for queue in queues:
            consumer = RabbitMQConsumer(
                queue_name=queue['queue_name'], callback_func=queue['callback'], eh=queue['error_handler']
            )
            consumers.append(consumer)
            thread = threading.Thread(target=consumer.start_consuming)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Unhandled exception: {e}', origin='consumers.py->main', traceback=tb()
        )
        shutdown_handler(None, None)


# if __name__ == '__main__':
#     process_shopify_order('5672071102631')
