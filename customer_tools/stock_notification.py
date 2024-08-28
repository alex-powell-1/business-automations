import datetime
from email import utils

import os

import pandas
from dateutil.relativedelta import relativedelta
from jinja2 import Template

import customer_tools
from shop.coupons import generate_random_code, shopify_create_coupon, cp_create_coupon
import customer_tools.customers
from product_tools.products import Product
from setup import creds
from setup.email_engine import Email
from setup.error_handler import ScheduledTasksErrorHandler as error_handler

from setup import barcode_engine as barcode_engine

from setup.utilities import PhoneNumber
from setup.sms_engine import SMSEngine

from integration.database import Database


coupon_offer = 'save $10 on an order of $100 or more'


def send_email(greeting, email, item_number, coupon_code, photo=None):
    """Send PDF attachment to customer"""
    recipient = {'': email}
    # Generate HTML
    # ---------------

    # Get Item Details by creating new Product Object
    item = Product(item_number)

    # Create Subject
    email_subject = f'{item.web_title.title()} is back in stock!'

    with open('./templates/stock_notification.html', 'r') as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    email_data = {
        'title': email_subject,
        'greeting': greeting,
        'item': item.web_title,
        'qty': item.buffered_quantity_available,
        'company': creds.company_name,
        'item_description': item.web_description,
        'item_url': item.item_url,
        'coupon_code': coupon_code,
        'coupon_offer': coupon_offer,
        'signature_name': creds.signature_name,
        'signature_title': creds.signature_title,
        'company_phone': creds.company_phone,
        'company_url': creds.company_url,
        'company_reviews': creds.company_reviews,
        'company_address_line_1': creds.company_address_html_1,
        'company_address_line_2': creds.company_address_html_2,
    }

    email_content = jinja_template.render(email_data)

    Email.send(
        recipients_list=recipient,
        subject=email_subject,
        content=email_content,
        image=photo,
        image_name='coupon.png',
        barcode=f'./{coupon_code}.png',
    )


def send_sms(greeting, phone, item, qty, webtitle, coupon_code, photo=None):
    """Send SMS text message to customer"""
    phone = PhoneNumber(phone).to_twilio()

    def form_string(str: str):
        return str.strip().replace(' ', '-').replace('"', '').replace('&', '-').replace('.', '-').lower()

    link = f'https://settlemyrenursery.com/products/{form_string(webtitle)}'

    coupon_message = ''

    if coupon_code is not None:
        coupon_message = f'\n\nUse code: { coupon_code } online or in-store to { coupon_offer }!'

    message = f'{greeting}!\n\nYou requested to be notified when { item } was back in stock. We are excited to share that we have { int(qty) } available now!{ coupon_message }\n\n{ link }'
    SMSEngine.send_text(
        origin='SERVER', campaign='STOCK NOTIFY', to_phone=phone, message=message, url=photo, username='Automation'
    )


def send_stock_notifications():
    """Sends stock notification text updates for items that were out of stock
    but now have stock > 0. Cleans table so contacts are only notified once"""

    error_handler.logger.info('Starting: Send Stock Notification Text')

    messages_sent = 0

    cols = 'EMAIL, PHONE, ITEM_NO, DESCR, QTY_AVAIL'
    query = f'SELECT {cols} FROM VI_STOCK_NOTIFY WHERE QTY_AVAIL > 0'

    try:
        response = Database.db.query(query)
    except:
        error_handler.error_handler.add_error_v('Error querying database', origin='stock_notification.py')
        return

    if response is None:
        error_handler.logger.info('No rows to send')
        return

    for row in response:
        email = row[0]
        phone = row[1]
        item_no = row[2]
        description = row[3]
        qty = row[4]

        query = f"""
        DELETE FROM SN_STOCK_NOTIFY
        WHERE ITEM_NO = '{item_no}'
        """
        ################################################################
        ############## Create variables for SMS and Email ##############
        ################################################################

        cust_no = customer_tools.customers.lookup_customer(email_address=email, phone_number=phone)

        greeting = 'Hey there'

        if cust_no is not None:
            customer = customer_tools.customers.Customer(cust_no)
            greeting = f'Hey {customer.first_name}'

        coupon_code = None
        coupon_exclusions = ['45', '804', 'HB', 'BOSTON']
        included = item_no not in coupon_exclusions

        if included:
            coupon_code = generate_random_code(10)
            barcode_engine.generate_barcode(data=coupon_code, filename=coupon_code)

            # Create CounterPoint Coupons
            try:
                cp_create_coupon(
                    description=f'{customer.first_name.title()} ' f'{customer.last_name.title()}-Stock:{item_no}',
                    code=coupon_code,
                    amount=10,
                    min_purchase=100,
                )
            except Exception as e:
                error_handler.error_handler.add_error_v(
                    f'CP Coupon Creation Error: {e}', origin='stock_notification.py'
                )
            else:
                error_handler.logger.info(f'CP Coupon Creation Success! Code: {coupon_code}')

            # Create Coupon Expiration Date
            expiration_date = utils.format_datetime(datetime.datetime.now() + relativedelta(days=+5))

            # Send to Shopify. Create Coupon.
            shopify_create_coupon(
                name=f'Back in Stock({item_no}, {email})',
                coupon_type='per_total_discount',
                amount=10,
                min_purchase=100,
                code=coupon_code,
                max_uses_per_customer=1,
                max_uses=1,
                expiration=expiration_date,
            )

        product_photo = creds.photo_path + f'/{item_no}.jpg'

        ###############################################################
        ###################### Send Text / Email ######################
        ###############################################################

        if phone is not None:
            query += f" AND PHONE = '{phone}'"
            send_sms(
                greeting=greeting,
                phone=phone,
                item=description,
                qty=qty,
                coupon_code=coupon_code,
                webtitle=Product(item_no).web_title,
                photo=product_photo,
            )
            messages_sent += 1

        if email is not None:
            query += f" AND EMAIL = '{email}'"
            send_email(
                greeting=greeting, email=email, item_number=item_no, coupon_code=coupon_code, photo=product_photo
            )
            if phone is None:
                messages_sent += 1

        #################################################################
        ##################### Remove Database Entry #####################
        #################################################################

        try:
            response = Database.db.query(query)

            if response['code'] == 200:
                error_handler.logger.success('Successfully removed.')
            else:
                error_handler.error_handler.add_error_v(
                    'Error removing from database', origin='stock_notification.py'
                )
        except:
            error_handler.error_handler.add_error_v('Error querying database', origin='stock_notification.py')

        if included:
            os.remove(f'./{coupon_code}.png')

    error_handler.logger.success('Completed: Send Stock Notification Text')
    error_handler.logger.info(f'Total Messages Sent: {messages_sent}')


if __name__ == '__main__':
    send_stock_notifications()
