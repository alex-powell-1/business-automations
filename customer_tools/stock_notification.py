import datetime
import os

from dateutil.relativedelta import relativedelta
from jinja2 import Template

import customer_tools
from shop.coupons import generate_random_coupon, delete_expired_coupons
import customer_tools.customers
from setup.utilities import generate_random_code
from product_tools.products import Product
from setup import creds
from setup.creds import API
from setup.email_engine import Email
from sms.sms_messages import SMSMessages
from setup.error_handler import ScheduledTasksErrorHandler as error_handler

from setup import barcode_engine as barcode_engine

from setup.utilities import PhoneNumber, combine_images
from setup.sms_engine import SMSEngine
from integration.shopify_api import Shopify
import random
from database import Database

from shutil import copy

from time import sleep



def remove_file(file_path):
    error_handler.logger.info(f'Removing file: {file_path}')
    try:
        os.remove(file_path)
    except FileNotFoundError:
        error_handler.error_handler.add_error_v(f'File not found: {file_path}', origin='stock_notification.py')
    except Exception as e:
        error_handler.error_handler.add_error_v(f'Error removing file: {e}', origin='stock_notification.py')


def copy_file(source, destination):
    error_handler.logger.info(f'Copying file: {source} to {destination}')
    try:
        copy(source, destination)
    except FileNotFoundError:
        error_handler.error_handler.add_error_v(f'File not found: {source}', origin='stock_notification.py')
    except Exception as e:
        error_handler.error_handler.add_error_v(f'Error copying file: {e}', origin='stock_notification.py')



def send_sms(greeting, phone, item, qty, webtitle, coupon_code, expiration_str, photo=None):
    """Send SMS text message to customer"""
    phone = PhoneNumber(phone).to_twilio()

    def form_string(str: str):
        return (
            str.strip()
            .replace(' ', '-')
            .replace('"', '')
            .replace('&', '-')
            .replace('.', '-')
            .replace('/', '-')
            .lower()
        )

    link = f'https://settlemyrenursery.com/products/{form_string(webtitle)}'

    coupon_message = ''

    if coupon_code is not None:
        coupon_message = f'\n\nUse code: { coupon_code } online or in-store to { creds.Marketing.StockNotification.offer }! { expiration_str }'

    message = f'{greeting}!\n\nYou requested to be notified when { item } was back in stock. We are excited to share that we have { int(qty) } available now!{ coupon_message } { link }'
    SMSEngine.send_text(
        origin='SERVER',
        campaign='STOCK NOTIFY',
        to_phone=phone,
        message=message,
        url=photo,
        username='Automation',
    )


def create_coupon(item_no, customer):
    coupon_code = generate_random_coupon()
    barcode_engine.generate_barcode(data=coupon_code, filename=coupon_code)

    # Create CounterPoint Coupons
    try:
        first_name = 'WEB'
        last_name = generate_random_code(4) + generate_random_code(4)

        if customer is not None:
            first_name = customer.first_name.title()
            last_name = customer.last_name.title()

        cp_id = Database.CP.Discount.create(
            description=f'{first_name} ' f'{last_name}-Stock:{item_no}',
            code=coupon_code,
            amount=creds.Marketing.StockNotification.discount,
            min_purchase=creds.Marketing.StockNotification.min_amt,
        )

        if cp_id is None:
            error_handler.error_handler.add_error_v(
                f'Coupon Code Could Not Be Generated for {item_no}', origin='stock_notification.py'
            )
    except Exception as e:
        error_handler.error_handler.add_error_v(f'CP Coupon Creation Error: {e}', origin='stock_notification.py')
    else:
        error_handler.logger.success(f'CP Coupon Creation Success! Code: {coupon_code}')
        error_handler.logger.info('Sending to Shopify...')

        # Create Coupon Expiration Date
        expiration_date = datetime.datetime.now() + relativedelta(days=+5)

        try:
            # Send to Shopify. Create Coupon.
            shop_id = Shopify.Discount.Code.Basic.create(
                name=f'Back in Stock({item_no}, {first_name+' '+last_name})',
                amount=creds.Marketing.StockNotification.discount,
                min_purchase=creds.Marketing.StockNotification.min_amt,
                code=coupon_code,
                max_uses=1,
                expiration=expiration_date,
                eh=error_handler,
            )
        except Exception as e:
            error_handler.error_handler.add_error_v(
                f'Shopify Coupon Creation Error: {e}', origin='stock_notification.py'
            )
        else:
            query = f"""
            INSERT INTO SN_SHOP_DISC
            (SHOP_ID, DISC_ID)
            VALUES
            ('{shop_id}', '{cp_id}')
            """

            try:
                response = Database.query(query)
                if response['code'] == 200:
                    error_handler.logger.success('Shopify Coupon Added Successfully!')
                else:
                    error_handler.error_handler.add_error_v(
                        'Error adding shopify coupon to database', origin='stock_notification.py'
                    )
            except:
                error_handler.error_handler.add_error_v('Error querying database', origin='stock_notification.py')
            else:
                return coupon_code

    return None


def send_stock_notifications():
    """Sends stock notification text updates for items that were out of stock
    but now have stock > 0. Cleans table so contacts are only notified once"""

    error_handler.logger.info('Starting: Send Stock Notification Text')

    messages_sent = 0

    photos_to_remove = []

    try:
        cols = 'EMAIL, PHONE, ITEM_NO, DESCR, QTY_AVAIL'
        query = f'SELECT {cols} FROM VI_STOCK_NOTIFY WHERE QTY_AVAIL > 0'

        try:
            response = Database.query(query)
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

            greeting = random.choice(SMSMessages.greetings)

            customer = None

            if cust_no is not None:
                customer = Database.CP.Customer(cust_no)
                greeting = f'{greeting} {customer.FST_NAM}'

            coupon_code = None
            coupon_eligible = False

            if item_no not in creds.Marketing.StockNotification.exclusions:
                coupon_eligible = True
            
            if coupon_eligible:
                coupon_code = create_coupon(item_no, customer)
                if coupon_code is None:
                    error_handler.error_handler.add_error_v(f'Coupon Code Could Not Be Generated for {item_no}')
                    continue

            item_photo = creds.Company.product_images + f'/{item_no}.jpg'

            if coupon_eligible:
                expiration_date = datetime.datetime.now() + relativedelta(days=+5)

                combine_images(
                    item_photo,
                    f'{coupon_code}.png',
                    combined_image_path=creds.API.public_files_local_path + f'/{item_no}-BARCODE.jpg',
                    barcode_text=coupon_code,
                    expires_text=f'Expires {expiration_date:%b %d, %Y}',
                )

            copy_file(item_photo, creds.API.public_files_local_path)

            local_photo = creds.API.public_files_local_path + f'/{item_no}.jpg'
            product_photo = API.public_files + f'/{item_no}.jpg'

            photos_to_remove.append(local_photo)

            if coupon_eligible:
                photos_to_remove.append(creds.API.public_files_local_path + f'/{item_no}-BARCODE.jpg')
                product_photo = API.public_files + f'/{item_no}-BARCODE.jpg'

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
                    expiration_str=f'Expires {expiration_date:%b %d, %Y}.',
                )
                messages_sent += 1

            if email is not None:
                query += f" AND EMAIL = '{email}'"
                
                Email.Customer.StockNotification.send(
                    greeting=greeting, email=email, item_number=item_no, coupon_code=coupon_code, photo=local_photo
                )
                if phone is None:
                    messages_sent += 1

            #################################################################
            ##################### Remove Database Entry #####################
            #################################################################

            try:
                response = Database.query(query)

                if response['code'] == 200:
                    error_handler.logger.success('Successfully removed.')
                else:
                    error_handler.error_handler.add_error_v(
                        'Error removing from database', origin='stock_notification.py'
                    )
            except:
                error_handler.error_handler.add_error_v('Error querying database', origin='stock_notification.py')

            if coupon_eligible:
                os.remove(f'./{coupon_code}.png')

        error_handler.logger.success('Completed: Send Stock Notification Text')
        error_handler.logger.info(f'Total Messages Sent: {messages_sent}')

        delete_expired_coupons()

    except Exception as e:
        error_handler.error_handler.add_error_v('Error sending stock notifications')
        error_handler.error_handler.add_error_v(f'Error: {e}', origin='stock_notification.py')

    if len(photos_to_remove) > 0:
        error_handler.logger.info('Removing photos')
        sleep(3)
        for photo in photos_to_remove:
            remove_file(photo)
    else:
        error_handler.logger.info('No photos to remove.')

    error_handler.logger.success('Completed: Send Stock Notification Text')


if __name__ == '__main__':
    send_stock_notifications()
