import json
import time
import hmac
import base64
import hashlib
import urllib.parse
from datetime import datetime
import bleach
import flask
import pika
import requests
from flask import request, jsonify, abort, send_from_directory, url_for
from werkzeug.exceptions import NotFound, BadRequest
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from jinja2 import Template
from jsonschema import validate, ValidationError
from twilio.twiml.messaging_response import MessagingResponse
from waitress import serve
from setup.utilities import convert_utc_to_local

from setup import creds, authorization
from setup.creds import Route
from setup.email_engine import Email
from setup.error_handler import ProcessInErrorHandler, ProcessOutErrorHandler, LeadFormErrorHandler, Logger
from integration.shopify_api import Shopify
from qr.qr_codes import QR

from database import Database
from traceback import format_exc as tb

import uuid_utils as uuidu

from setup.utilities import PhoneNumber, EmailAddress

from integration.shopify_customers import Customers

app = flask.Flask(__name__)

limiter = Limiter(get_remote_address, app=app)

CORS(app)

# When False, app is served by Waitress
dev = False


class EventID:
    """Shopify Event ID class to prevent duplicate processing of webhooks."""

    draft_update = 0
    draft_create = 0
    order_create = 0
    customer_create = 0
    customer_update = 0
    product_update = 0


@app.before_request
def log_request():
    """Log incoming requests."""
    if request.is_json:
        preview = f'- {request.get_data().decode('utf-8')[:30]}'
    else:
        preview = ''
    logger = Logger(log_directory=creds.Logs.server)
    logger.info(f'{request.method} - {request.url} {preview}')


# Error handling functions
@app.errorhandler(ValidationError)
def handle_validation_error(e):
    # Return a JSON response with a message indicating that the input data is invalid
    ProcessInErrorHandler.error_handler.add_error_v(
        error=f'Invalid input data: {e}', origin='validation_error', traceback=tb()
    )
    return jsonify({'error': 'Invalid input data'}), 400


@app.errorhandler(Exception)
def handle_exception(e):
    url = request.url
    # Return a JSON response with a generic error message
    ProcessInErrorHandler.error_handler.add_error_v(
        error=f'An error occurred: {e}', origin=f'exception - {url}', traceback=tb()
    )
    return jsonify({'error': f'An error occurred {e}'}), 500


def verify_webhook(data, hmac_header):
    """
    Compare the computed HMAC digest based on the client secret and the request contents
    to the reported HMAC in the headers.
    """
    calculated_hmac = base64.b64encode(hmac.new(creds.shopify_secret_key.encode(), data, hashlib.sha256).digest())
    return hmac.compare_digest(calculated_hmac, hmac_header.encode())


@app.route(Route.design, methods=['POST'])
@limiter.limit('20/minute')  # 10 requests per minute
def get_service_information():
    """Route for information request about company service. Sends JSON to RabbitMQ for asynchronous processing."""
    LeadFormErrorHandler.logger.log_file = f'design_leads_{datetime.now().strftime("%m_%d_%y")}.log'
    token = request.headers.get('Authorization').split(' ')[1]
    url = 'https://www.google.com/recaptcha/api/siteverify'
    payload = {'secret': creds.recaptcha_secret, 'response': token}
    response = requests.post(url, data=payload)
    if not response.json()['success']:
        return 'Could not verify captcha.', 400

    data = request.json
    # Validate the input data
    try:
        validate(instance=data, schema=creds.design_schema)
    except ValidationError as e:
        LeadFormErrorHandler.error_handler.add_error_v(error=f'Invalid input data: {e}', origin='design_info')
        abort(400, description='Invalid input data')
    else:
        payload = json.dumps(data)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

        channel = connection.channel()

        channel.queue_declare(queue=creds.consumer_design_lead_form, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.consumer_design_lead_form,
            body=payload,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

        connection.close()
    except Exception as e:
        LeadFormErrorHandler.error_handler.add_error_v(
            error=f'Error sending design request to RabbitMQ: {e}', origin=Route.design, traceback=tb()
        )
        return jsonify({'error': 'Internal server error'}), 500
    else:
        return 'Your information has been received. Please check your email for more information from our team.'


@app.route(Route.design_admin, methods=['POST'])
@limiter.limit('20/minute')  # 10 requests per minute
def get_service_information_admin():
    """Route for information request about company service. Sends JSON to RabbitMQ for asynchronous processing."""
    LeadFormErrorHandler.logger.log_file = f'design_leads_{datetime.now().strftime("%m_%d_%y")}.log'
    token = request.headers.get('Authorization').split(' ')[1]
    # Base64 decode the token
    decoded_token = base64.b64decode(token).decode()
    signature, message, timestamp = decoded_token.split(':')
    current_time = int(time.time() * 1000)
    time_difference = current_time - int(timestamp)
    # Verify timestamp
    if time_difference > 1000 * 60 * 10:
        return jsonify({'error': 'Invalid token'}), 401

    def verify_hmac(key, message, sig):
        computed_digest = hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(computed_digest, sig)

    # Validate the SHA hash
    if not verify_hmac(creds.design_admin_key, message, signature):
        return jsonify({'error': 'Invalid token'}), 401
    data = request.json
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=creds.consumer_design_lead_form, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=creds.consumer_design_lead_form,
            body=json.dumps(data),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        LeadFormErrorHandler.error_handler.add_error_v(
            error=f'Error sending design request to RabbitMQ: {e}', origin=Route.design_admin, traceback=tb()
        )
        return jsonify({'error': 'Internal server error'}), 500
    else:
        return 'Your information has been received. Please check your email for more information from our team.'


@app.route(Route.stock_notify, methods=['POST'])
@limiter.limit('20/minute')  # 10 requests per minute
def stock_notification():
    """Get contact and product information from user who wants notification of when
    a product comes back into stock."""
    ProcessInErrorHandler.logger.log_file = f'log_{datetime.now():%m_%d_%y}.log'

    data = request.json
    # Sanitize the input data
    sanitized_data = {k: bleach.clean(v) for k, v in data.items()}

    token = request.headers.get('Authorization').split(' ')[1]
    url = 'https://www.google.com/recaptcha/api/siteverify'
    payload = {'secret': creds.recaptcha_secret, 'response': token}
    response = requests.post(url, data=payload)
    if not response.json()['success']:
        return 'Could not verify captcha.', 400
    # Validate the input data
    try:
        email_empty = sanitized_data['email'] == ''
        phone_empty = sanitized_data['phone'] == ''
        email_valid = EmailAddress.is_valid(sanitized_data['email'])
        phone_valid = PhoneNumber.is_valid(sanitized_data['phone'])

        if (email_empty and phone_empty) or (not email_valid and not phone_valid):
            return 'Invalid email or phone number.', 400
        elif not phone_valid and not phone_empty:
            return 'Invalid phone number.', 400
        elif not email_valid and not email_empty:
            return 'Invalid email address.', 400
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Invalid input data: {e}', origin=Route.stock_notify, traceback=tb()
        )
        return f'An error occurred: {str(e)}', 500
    else:
        # Fix empty strings
        email = None if email_empty else sanitized_data['email']
        phone = None if phone_empty else sanitized_data['phone']
        item_no = sanitized_data['sku']

        if phone is not None:
            phone = PhoneNumber(phone).to_cp()

        if Database.StockNotification.has_info(item_no=item_no, email=email, phone=phone):
            nouns1 = []

            indefinite_article = 'a'

            nouns2 = []

            if email is not None:
                nouns1.append('email address')
                indefinite_article = 'an'
                nouns2.append('email')

            if phone is not None:
                nouns1.append('phone number')
                nouns2.append('text')

            noun1 = ' or '.join(nouns1)
            noun2 = ' & '.join(nouns2)

            return (
                f"""This {noun1} is already on file for this item. We will send you
                {indefinite_article} {noun2} when your item is back in stock. 
                Please contact our office at <a href='tel:8288740679'>(828) 874-0679</a>
                if you need an alternative item. Thank you!"""
            ), 400
        else:
            try:
                response = Database.StockNotification.insert(item_no=item_no, email=email, phone=phone)
                if response['code'] == 200:
                    return (
                        'Thanks for your submission! We will send you an email when your item is back in stock.',
                        200,
                    )
                else:
                    return 'An error occurred. Please try again.', 500
            except:
                return 'An error occurred. Please try again.', 500


@app.route('/gift-card-recipient', methods=['POST'])
@limiter.limit('20 per minute')
def gift_card_recipient():
    data = request.json
    recipient = data['recipient']

    if not recipient:
        return jsonify({'error': 'No recipient provided'}), 400

    if not request.headers.get('Authorization'):
        new_uuid = str(uuidu.uuid4())

        # Create new database entry for new_uuid -> gift_card_recipient, lst_maint_dt
        try:
            query = f"""
                INSERT INTO SN_GFC_RECPS
                (UUID, RECPT_EMAIL, RECPT_NAM, LST_MAINT_DT)
                VALUES
                ('{new_uuid}', 
                {f"'{recipient['email']}'" if recipient['email'] != '' else 'NULL'}, 
                {f"'{recipient['name']}'" if recipient['name'] != '' else 'NULL'}, 
                GETDATE())
                """
            response = Database.query(query=query)
            if response['code'] != 200:
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f"""Error creating recipient.\n
                    Query: {query} \n
                    Response: Code: {response['code']}, Message: {response['message']}""",
                    origin='gift_card_recipient',
                )
                return jsonify({'error': 'Error creating recipient'}), 400
        except:
            ProcessInErrorHandler.error_handler.add_error_v(
                error='Error creating recipient', origin='gift_card_recipient'
            )
            return jsonify({'error': 'Error creating recipient'}), 400

        return jsonify({'uuid': new_uuid, 'message': 'Recipient updated.'}), 200
    else:
        uuid = request.headers.get('Authorization')

        # Update database entry for uuid -> gift_card_recipient, lst_maint_dt

        try:
            response = Database.query(
                f"""
                UPDATE SN_GFC_RECPS
                SET RECPT_EMAIL = {f"'{recipient['email']}'" if recipient['email'] != '' else 'NULL'}, 
                RECPT_NAM = {f"'{recipient['name']}'" if recipient['name'] != '' else 'NULL'}, 
                LST_MAINT_DT = GETDATE()
                WHERE UUID = '{uuid}'
                """,
                commit=True,
            )
            if response['code'] != 200:
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f"""Error updating recipient.\n
                    Query: {query} \n
                    Response: Code: {response['code']}, Message: {response['message']}""",
                    origin='gift_card_recipient',
                )
                return jsonify({'error': 'Error updating recipient'}), 400

        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error updating recipient: {e}', origin='gift_card_recipient'
            )
            return jsonify({'error': 'Error updating recipient'}), 400

        return jsonify({'message': 'Recipient updated.'}), 200


@app.route('/update-uuid-email', methods=['POST'])
@limiter.limit('20 per minute')
def update_uuid_email():
    data = request.json
    email = data['email']

    if not email:
        return jsonify({'error': 'No email provided'}), 400

    if not request.headers.get('Authorization'):
        return jsonify({'error': 'No uuid provided'}), 400

    uuid = request.headers.get('Authorization')

    # Update database entry for uuid -> email, lst_maint_dt
    try:
        response = Database.query(
            f"""
            UPDATE SN_GFC_RECPS
            SET EMAIL = '{email}', LST_MAINT_DT = GETDATE()
            WHERE UUID = '{uuid}'
            """,
            commit=True,
        )
        if response['code'] != 200:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error updating: {response['message']}', origin='update-uuid-email'
            )
            return jsonify({'error': 'Error updating'}), 400

    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(error=f'Error updating: {e}', origin='update-uuid-email')
        return jsonify({'error': 'Error updating'}), 400

    return jsonify({'message': 'Email updated.'}), 200


@app.route('/update-uuid-name', methods=['POST'])
@limiter.limit('20 per minute')
def update_uuid_name():
    data = request.json
    name = data['name']

    if not name:
        return jsonify({'error': 'No name provided'}), 400

    if not request.headers.get('Authorization'):
        return jsonify({'error': 'No uuid provided'}), 400

    uuid = request.headers.get('Authorization')

    # Update database entry for uuid -> name, lst_maint_dt
    try:
        Database.query(
            f"""
            UPDATE SN_GFC_RECPS
            SET NAME = '{name}', LST_MAINT_DT = GETDATE()
            WHERE UUID = '{uuid}'
            """,
            commit=True,
        )
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error updating: {e}', origin='update-uuid-name', traceback=tb()
        )
        return jsonify({'error': 'Error updating'}), 400

    return jsonify({'message': 'Name updated.'}), 200


@app.route(Route.newsletter, methods=['POST'])
@limiter.limit('20 per minute')
def newsletter_signup():
    """Route for website pop-up. Offers user a coupon and adds their information to a csv."""
    token = request.headers.get('Authorization').split(' ')[1]

    url = 'https://www.google.com/recaptcha/api/siteverify'
    payload = {'secret': creds.recaptcha_secret, 'response': token}
    response = requests.post(url, data=payload)
    if not response.json()['success']:
        return 'Could not verify captcha.', 400

    """Route for website pop-up. Offers user a coupon and adds their information to a csv."""
    data = request.json

    # Sanitize the input data
    sanitized_data = {k: bleach.clean(v) for k, v in data.items()}
    # Validate the input data
    try:
        validate(instance=sanitized_data, schema=creds.newsletter_schema)
    except ValidationError as e:
        abort(400, description=e.message)
    else:
        email = data.get('email')
        if Database.Newsletter.is_subscribed(email):
            print(f'{email} is already on file')
            return 'This email address is already on file.', 400

        # Lookup customer by email
        cust_no = Database.Counterpoint.Customer.lookup_customer_by_email(email)
        if cust_no:
            # Subscribe customer to newsletter
            Database.Newsletter.subscribe(email, eh=ProcessInErrorHandler)

        # Send welcome email
        recipient = {'': email}
        with open('./templates/new10.html', 'r') as file:
            template_str = file.read()

        jinja_template = Template(template_str)

        email_data = {
            'title': f'Welcome to {creds.company_name}',
            'greeting': 'Hi!',
            'service': creds.service,
            'coupon': 'NEW10',
            'company': creds.company_name,
            'list_items': creds.list_items,
            'signature_name': creds.signature_name,
            'signature_title': creds.signature_title,
            'company_phone': creds.company_phone,
            'company_url': creds.company_url,
            'company_reviews': creds.company_reviews,
        }

        email_content = jinja_template.render(email_data)

        try:
            Email.send(
                recipients_list=recipient,
                subject='Coupon Code: NEW10',
                content=email_content,
                mode='related',
                logo=True,
            )
        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error sending welcome email: {e}', origin='newsletter'
            )
            return 'Error sending welcome email.', 500
        else:
            res = Database.Newsletter.insert(email)
            if res['code'] != 200:
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f'Error adding {email} to newsletter: {res["message"]}', origin=Route.newsletter
                )
                return 'Error adding email to newsletter.', 500
            return 'OK', 200


@app.route(Route.sms, methods=['POST'])
@limiter.limit('10/second')  # rate limiter
def incoming_sms():
    """Webhook route for incoming SMS/MMS messages to be used with client messenger application.
    Saves all incoming SMS/MMS messages to share drive csv file."""
    raw_data = request.get_data()
    # Decode
    string_code = raw_data.decode('utf-8')
    # Parse to dictionary
    msg = urllib.parse.parse_qs(string_code)

    from_phone = msg['From'][0]
    to_phone = msg['To'][0]
    if 'Body' in msg:
        body = msg['Body'][0]
    else:
        body = ''
    sid = msg['SmsMessageSid'][0]

    # Get MEDIA URL for MMS Messages
    if int(msg['NumMedia'][0]) > 0:
        media_url = ''

        for i in range(int(msg['NumMedia'][0])):
            media_key_index = f'MediaUrl{i}'
            url = msg[media_key_index][0]
            if i < (int(msg['NumMedia'][0]) - 1):
                # Add separator per front-end request.
                media_url += url + ';;;'
            else:
                media_url += url
    else:
        media_url = None

    if body.lower() == creds.sms_sync_keyword:
        # Run sync process
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()

            channel.queue_declare(queue=creds.consumer_sync_on_demand, durable=True)

            channel.basic_publish(
                exchange='',
                routing_key=creds.consumer_sync_on_demand,
                body=from_phone,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
            )
            connection.close()

        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error sending sync request  to RabbitMQ: {e}',
                origin=Route.Shopify.order_create,
                traceback=tb(),
            )

        # Return Response to Twilio
        resp = MessagingResponse()
        return str(resp)

    # Get Customer Name and Category from SQL
    customer_number, full_name, category = Database.Counterpoint.Customer.get_customer_by_phone(from_phone)

    Database.SMS.insert(
        origin='Webhook',
        to_phone=to_phone,
        from_phone=from_phone,
        cust_no=customer_number,
        name=full_name,
        category=category,
        body=body,
        media=media_url,
        sid=sid,
        error_code=None,
        error_message=None,
    )

    # Unsubscribe user from SMS marketing
    if body.lower().strip() in [
        'stop',
        'unsubscribe',
        'stop please',
        'please stop',
        'cancel',
        'opt out',
        'remove me',
    ]:
        Database.SMS.unsubscribe(
            origin='WEBHOOK',
            campaign=Route.sms,
            cust_no=customer_number,
            name=full_name,
            category=category,
            phone=from_phone,
        )

    # Subscribe user to SMS marketing
    elif body.lower().strip() in ['start', 'subscribe', 'start please', 'please start', 'opt in', 'add me']:
        Database.SMS.subscribe(
            origin='WEBHOOK',
            campaign=Route.sms,
            cust_no=customer_number,
            name=full_name,
            category=category,
            phone=from_phone,
        )

    # Return Response to Twilio
    resp = MessagingResponse()
    return str(resp)


@app.route(Route.Shopify.order_create, methods=['POST'])
@limiter.limit('10/second')
def shopify():
    """Webhook route for incoming orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.order_create:
        return jsonify({'success': True}), 200
    EventID.order_create = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    if 'refund_line_items' in webhook_data:
        webhook_data['id'] = webhook_data['order_id']

    with open('order_create.json', 'a') as f:
        json.dump(webhook_data, f)

    order_id = webhook_data['id']

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=creds.consumer_shopify_orders, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.consumer_shopify_orders,
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}',
            origin=Route.Shopify.order_create,
            traceback=tb(),
        )

    return jsonify({'success': True}), 200


@app.route(Route.Shopify.draft_create, methods=['POST'])
@limiter.limit('10/second')
def shopify_draft_create():
    """Webhook route for newly created draft orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.draft_create:
        return jsonify({'success': True}), 200
    EventID.draft_create = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    print('DRAFT ORDER RECEIVED.')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401
    order_id = webhook_data['id']

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=creds.consumer_shopify_draft_create, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.consumer_shopify_draft_create,
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}',
            origin=Route.Shopify.draft_create,
            traceback=tb(),
        )

    return jsonify({'success': True}), 200


@app.route(Route.Shopify.draft_update, methods=['POST'])
@limiter.limit('10/second')
def shopify_draft_update():
    """Webhook route for updated draft orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.draft_update:
        return jsonify({'success': True}), 200
    EventID.draft_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401
    order_id = webhook_data['id']

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=creds.consumer_shopify_draft_update, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.consumer_shopify_draft_update,
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}',
            origin=Route.Shopify.draft_update,
            traceback=tb(),
        )

    return jsonify({'success': True}), 200


@app.route(Route.Shopify.customer_create, methods=['POST'])
@limiter.limit('10/second')
def shopify_customer_create():
    """Webhook route for updated customers. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.customer_create:
        return jsonify({'success': True}), 200
    EventID.customer_create = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')

    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    id = webhook_data['id']

    error_handler = ProcessInErrorHandler.error_handler
    logger = error_handler.logger

    logger.info(f'Processing Customer Create: {id}')

    if Customers.Customer.has_metafield(cust_id=id, key='number'):
        logger.info(f'Customer {id} has customer number metafield. Skipping.')
    else:
        try:
            street = None
            city = None
            state = None
            zip_code = None
            phone = webhook_data['phone']
            email = webhook_data['email']

            addrs = webhook_data['addresses']

            if len(addrs) > 0:
                a = addrs[0]
                street = a['address1']
                city = a['city']
                state = a['province']
                zip_code = a['zip']

                # Merge new customer into existing customer
                # Delete new customer from Shopify

        except Exception as e:
            error_handler.add_error_v(
                error=f'Error adding customer {id}: {e}', origin=Route.Shopify.customer_create, traceback=tb()
            )
            return jsonify({'error': 'Error adding customer'}), 500
        else:
            logger.success(f'Customer {id} added successfully.')
            return jsonify({'success': True}), 200

    logger.success(f'Customer Create Finished: {id}')
    return jsonify({'success': True}), 200


@app.route(Route.Shopify.customer_update, methods=['POST'])
@limiter.limit('10/second')
def shopify_customer_update():
    """Webhook route for updated customers. Sends to RabbitMQ queue for asynchronous processing"""
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.customer_update:
        return jsonify({'success': True}), 200
    EventID.customer_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')

    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    #############################################################################
    #############################################################################
    ########## Im concerned about a potential loop here. If a customer ##########
    ## is updated via integration it triggers the webhook. It will be updated, ##
    ######## changing its LST_MAINT_DT and marking it for the next sync, ########
    ######################## starting the process again. ########################
    #############################################################################
    #############################################################################

    # error_handler = ProcessInErrorHandler.error_handler
    # logger = error_handler.logger

    # logger.info(f'Processing Customer Update: {id}')

    # update_customer()

    # logger.success(f'Customer Update Finished: {id}')
    return jsonify({'success': True}), 200


@app.route(Route.Shopify.product_update, methods=['POST'])
@limiter.limit('10/second')
def shopify_product_update():
    """Webhook route for updated products. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
    logger = Logger(creds.Logs.webhooks_product_update)
    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.product_update:
        return jsonify({'success': True}), 200
    EventID.product_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    # Get product data
    product_id = webhook_data['id']
    title = webhook_data['title']
    description = webhook_data['body_html']
    status = webhook_data['status']
    tags = webhook_data['tags']
    item_no = Database.Shopify.Product.get_parent_item_no(product_id)

    logger.log(f'Webhook: Product Update, SKU:{item_no}, Product ID: {product_id}, Web Title: {title}')

    if item_no and description:
        # Update product description in Counterpoint - Skip timestamp update (avoid loop)
        Database.Counterpoint.Product.HTMLDescription.update(
            item_no=item_no, html_descr=description, update_timestamp=False
        )

    # Get SEO data
    seo_data = Shopify.Product.SEO.get(product_id)
    if seo_data:
        meta_title = seo_data['title']
        meta_description = seo_data['description']
    else:
        meta_title = None
        meta_description = None

    # Get product Metafields
    metafields = Shopify.Product.Metafield.get(product_id)
    features = None
    botanical_name = None
    plant_type = None
    light_requirements = None
    size = None
    features = None
    bloom_season = None
    bloom_color = None
    color = None
    is_featured = None
    in_store_only = None
    is_preorder_item = None
    preorder_message = None
    preorder_release_date = None

    for i in metafields['product_specifications']:
        if i['key'] == 'botanical_name':
            botanical_name = i['value']

        if i['key'] == 'plant_type':
            plant_type = i['value']

        if i['key'] == 'light_requirements':
            light_requirements = i['value']

        if i['key'] == 'size':
            size = i['value']

        if i['key'] == 'features':
            features = i['value']

        if i['key'] == 'bloom_season':
            bloom_season = i['value']

        if i['key'] == 'bloom_color':
            bloom_color = i['value']

        if i['key'] == 'color':
            color = i['value']

    for i in metafields['product_status']:
        if i['key'] == 'featured':
            is_featured = True if i['value'] == 'true' else False

        if i['key'] == 'in_store_only':
            in_store_only = True if i['value'] == 'true' else False

        if i['key'] == 'preorder_item':
            is_preorder_item = True if i['value'] == 'true' else False

        if i['key'] == 'preorder_message':
            preorder_message = i['value']

        if i['key'] == 'preorder_release_date':
            preorder_release_date = convert_utc_to_local(i['value'])

    # Get media data
    media_payload = []
    media = webhook_data['media']

    if media:
        for m in media:
            id = m['id']
            position = m['position']
            alt_text = m['alt']
            if alt_text and position < 4:  # First 4 images only at this time.
                media_payload.append({'position': position, 'id': id, 'alt_text': alt_text})

    if item_no:
        update_payload = {'product_id': product_id, 'item_no': item_no}

        if status:
            update_payload['status'] = status
        if title:
            update_payload['title'] = title
        if meta_title:
            update_payload['meta_title'] = meta_title
        if meta_description:
            update_payload['meta_description'] = meta_description

        if tags:
            update_payload['tags'] = tags

        if botanical_name:
            update_payload['botanical_name'] = botanical_name
        if plant_type:
            update_payload['plant_type'] = plant_type
        if light_requirements:
            update_payload['light_requirements'] = light_requirements
        if size:
            update_payload['size'] = size
        if features:
            update_payload['features'] = features
        if bloom_season:
            update_payload['bloom_season'] = bloom_season
        if bloom_color:
            update_payload['bloom_color'] = bloom_color
        if color:
            update_payload['color'] = color
        if is_featured:
            update_payload['is_featured'] = is_featured
        if in_store_only:
            update_payload['in_store_only'] = in_store_only
        if is_preorder_item:
            update_payload['is_preorder_item'] = is_preorder_item
        if preorder_message:
            update_payload['preorder_message'] = preorder_message
        if preorder_release_date:
            update_payload['preorder_release_date'] = preorder_release_date

        if media_payload:
            for m in media_payload:
                position = m['position']
                update_payload[f'alt_text_{position}'] = m['alt_text']
        try:
            Database.Counterpoint.Product.update(update_payload)
        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error updating product {item_no}: {e}', origin=Route.Shopify.product_update, traceback=tb()
            )

    return jsonify({'success': True}), 200


@app.route(Route.token, methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_token():
    password = request.args.get('password')

    if password.lower() == creds.commercial_availability_pw:
        session = authorization.Session(password)
        authorization.SESSIONS.append(session)
        return jsonify({'token': session.token, 'expires': session.expires}), 200

    ProcessInErrorHandler.error_handler.add_error_v(error=f'Invalid password: {password} ', origin=Route.token)
    return jsonify({'error': 'Invalid username or password'}), 401


@app.route(Route.commercial_availability, methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_commercial_availability():
    token = request.args.get('token')

    session = next((s for s in authorization.SESSIONS if s.token == token), None)

    if not session or session.expires < time.time():
        authorization.SESSIONS = [s for s in authorization.SESSIONS if s.token != token]
        return jsonify({'error': 'Invalid token'}), 401

    response = requests.get(creds.commercial_availability_url)
    if response.status_code == 200:
        return jsonify({'data': response.text}), 200
    else:
        ProcessInErrorHandler.error_handler.add_error_v(
            error='Error fetching data', origin=Route.commercial_availability, traceback=tb()
        )
        return jsonify({'error': 'Error fetching data'}), 500


@app.route(Route.retail_availability, methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_availability():
    response = requests.get(creds.retail_availability_url)
    if response.status_code == 200:
        return jsonify({'data': response.text}), 200
    else:
        ProcessInErrorHandler.error_handler.add_error_v(
            error='Error fetching data', origin=Route.retail_availability
        )
        return jsonify({'error': 'Error fetching data'}), 500


@app.route(f'{Route.file_server}/<path:path>', methods=['GET'])
def serve_file(path):
    try:
        return send_from_directory(creds.public_files, path)
    except NotFound:
        return jsonify({'error': 'File not found'}), 404
    except BadRequest:
        return jsonify({'error': 'Bad request'}), 400
    except Exception as e:
        ProcessOutErrorHandler.error_handler.add_error_v(
            error=f'Error serving file: {e}', origin=Route.file_server, traceback=tb()
        )
        return jsonify({'error': 'Internal server error'}), 500


@app.route(f'{Route.qr}/<qr_id>', methods=['GET'])
def qr_tracker(qr_id):
    QR.visit(qr_id)
    return jsonify({'success': True}), 200


@app.route(Route.unsubscribe, methods=['GET'])
@limiter.limit('20 per minute')
def unsubscribe():
    email = request.args.get('email')
    if not email:
        return jsonify({'error': 'No email provided'}), 400

    response = Database.Newsletter.unsubscribe(email, eh=ProcessInErrorHandler)
    code = response['code']

    if code == 200:
        title = 'Unsubscribed'
        message = f'{email} has been successfully unsubscribed from our newsletter.'
    elif code == 201:
        title = 'Already Unsubscribed'
        message = f'{email} is already unsubscribed.'
    else:
        title = 'Error Unsubscribing'
        message = 'An error occurred while unsubscribing.'

    with open('./templates/email_subscription/unsubscribe.html', 'r') as file:
        template_str = file.read()
        jinja_template = Template(template_str)
        data = {
            'title': title,
            'email': email,
            'endpoint': f'{creds.api_endpoint}/{Route.subscribe}',
            'message': message,
            'code': code,
            'company_url': creds.company_url,
        }
        content = jinja_template.render(data, url_for=url_for)

    return content, 200


@app.route(Route.subscribe, methods=['GET'])
@limiter.limit('20 per minute')
def resubscribe():
    email = request.args.get('email')
    if not email:
        return jsonify({'error': 'No email provided'}), 400

    response = Database.Newsletter.subscribe(email, eh=ProcessInErrorHandler)
    code = response['code']

    if code == 200:
        title = 'Subscribed'
        message = f'{email} has been subscribed to our newsletter.'
    elif code == 201:
        title = 'Already Subscribed'
        message = f'{email} is already subscribed.'

    else:
        title = 'Error Subscribing'
        message = 'An error occurred while resubscribing.'

    with open('./templates/email_subscription/subscribe.html', 'r') as file:
        template_str = file.read()
        jinja_template = Template(template_str)
        data = {
            'title': title,
            'email': email,
            'endpoint': f'{creds.api_endpoint}/{Route.unsubscribe}',
            'message': message,
            'code': code,
            'company_url': creds.company_url,
        }
        content = jinja_template.render(data, url_for=url_for)

    return content, 200


@limiter.limit('10/minute')  # 10 requests per minute
@app.route('/robots.txt', methods=['GET'])
def robots():
    # disallow all robots
    return (
        """
    User-agent: *
    Disallow: /
    """,
        200,
    )


@app.route('/favicon.ico', methods=['GET'])
def favicon():
    return send_from_directory(creds.public_files, 'favicon.ico')


@app.route('/', methods=['GET'])
def index():
    return jsonify({'status': 'Server is running'}), 200


if __name__ == '__main__':
    if dev:
        app.run(debug=True, port=creds.flask_port)
    else:
        running = True
        while running:
            try:
                print('Flask Server Running')
                serve(
                    app,
                    host='localhost',
                    port=creds.flask_port,
                    threads=8,
                    max_request_body_size=1073741824,  # 1 GB
                    max_request_header_size=8192,  # 8 KB
                    connection_limit=1000,
                )
            except Exception as e:
                print('Error serving Flask app: ', e)
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f'Error serving Flask app: {e}', origin='server', traceback=tb()
                )
                time.sleep(5)
            # Stop the server if Keyboard Interrupt
            running = False
            print('Flask Server Stopped')
