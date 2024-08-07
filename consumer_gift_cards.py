import json
import os
import sys
import time
from datetime import datetime

import pika
import requests
from docxtpl import DocxTemplate

from setup import creds, email_engine
from setup.utilities import format_phone
from setup.sms_engine import SMSEngine
from integration.database import Database
from setup.error_handler import LeadFormErrorHandler

test_mode = False


class RabbitMQConsumer:
    def __init__(self, queue_name, host='localhost'):
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None

    def connect(self):
        parameters = pika.ConnectionParameters(self.host)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def callback(self, ch, method, properties, body):
        json_body = json.loads(body.decode())
        print(json_body)
        first_name = json_body['first_name']
        last_name = json_body['last_name']
        email = json_body['email']
        phone = format_phone(json_body['phone'], mode='counterpoint')
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

        LeadFormErrorHandler.logger.info(f'Received message from {first_name} {last_name}. Beginning Processing...')
        # establish start time for consistent logging
        now = datetime.now()
        now_log_format = f'{now:%Y-%m-%d %H:%M:%S}'

        Database.DesignLead.insert(
            date=now_log_format,
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
        LeadFormErrorHandler.logger.info('Sending SMS Message to Sales Team')
        try:
            SMSEngine.design_text(
                first_name, last_name, email, phone, interests, timeline, address, comments, test_mode=test_mode
            )
        except Exception as err:
            LeadFormErrorHandler.error_handler.add_error_v(f'Error (sms): {err}', origin='design_lead')
        else:
            LeadFormErrorHandler.logger.success(f'SMS Sent at {datetime.now():%H:%M:%S}')

        # Send email to client
        LeadFormErrorHandler.logger.info('Sending Email to Lead')
        try:
            email_engine.design_email(first_name, email)
        except Exception as err:
            LeadFormErrorHandler.error_handler.add_error_v(error=f'Error (email): {err}', origin='design_lead')
        else:
            LeadFormErrorHandler.logger.success(f'Email Sent at {datetime.now():%H:%M:%S}')
        # Print lead details for in-store use

        # Create the Word document
        LeadFormErrorHandler.logger.info('Rendering Word Document')
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
            LeadFormErrorHandler.logger.info('Printing Word Document')
            if not test_mode:
                os.startfile(ticket_name, 'print')
            # Delay while print job executes
            time.sleep(4)
            LeadFormErrorHandler.logger.info('Deleting Word Document')
            os.remove(ticket_name)
        except Exception as err:
            LeadFormErrorHandler.error_handler.add_error_v(error=f'Error (word): {err}', origin='design_lead')
        else:
            LeadFormErrorHandler.logger.success(
                f'Word Document created, printed, and deleted at {datetime.now():%H:%M:%S}'
            )

        # Upload to sheety API for spreadsheet use
        LeadFormErrorHandler.logger.info('Sending Details to Google Sheets')
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
            requests.post(url=creds.sheety_design_url, headers=creds.sheety_header, json=sheety_post_body)
        except Exception as err:
            LeadFormErrorHandler.error_handler.add_error_v(error=f'Error (sheety): {err}', origin='design_lead')
        else:
            LeadFormErrorHandler.logger.success(f'Sent to Google Sheets at {datetime.now():%H:%M:%S}')
        # Done
        LeadFormErrorHandler.logger.success(f'Processing Completed at {datetime.now():%H:%M:%S}\n')
        # Send acknowledgement for RabbitMQ to delete from Queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        while True:
            try:
                self.connect()
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
                print('Waiting for messages. To exit press CTRL+C')
                self.channel.start_consuming()
            except KeyboardInterrupt:
                sys.exit(0)
            except pika.exceptions.AMQPConnectionError:
                LeadFormErrorHandler.error_handler.add_error_v(
                    error='Connection lost. Reconnecting...', origin='design_lead'
                )
                time.sleep(5)  # Wait before attempting reconnection
            except Exception as err:
                LeadFormErrorHandler.error_handler.add_error_v(
                    error=f'Error (General Catch): {err}', origin='design_lead'
                )
                time.sleep(5)  # Wait before attempting reconnection


if __name__ == '__main__':
    consumer = RabbitMQConsumer(queue_name='design_info')
    consumer.start_consuming()
