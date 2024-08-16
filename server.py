import json
import time
import hmac
import base64
import hashlib
import urllib.parse
from datetime import datetime
import bleach
import flask
import pandas
import pika
import requests
from flask import request, jsonify, abort, send_from_directory
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
from setup.email_engine import Email
from setup.sms_engine import SMSEngine
from setup.error_handler import ProcessInErrorHandler, ProcessOutErrorHandler, LeadFormErrorHandler
from setup import log_engine
from integration.shopify_api import Shopify
from qr.qr_codes import QR

from integration.database import Database
from traceback import format_exc as tb

import uuid_utils as uuidu

app = flask.Flask(__name__)

limiter = Limiter(get_remote_address, app=app)

CORS(app)

# When False, app is served by Waitress
dev = False


# Error handling functions
@app.errorhandler(ValidationError)
def handle_validation_error(e):
    # Return a JSON response with a message indicating that the input data is invalid
    ProcessInErrorHandler.error_handler.add_error_v(error=f'Invalid input data: {e}', origin='validation_error')
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


@app.route('/design', methods=['POST'])
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
            error=f'Error sending design request to RabbitMQ: {e}', origin='design_info'
        )
        return jsonify({'error': 'Internal server error'}), 500
    else:
        return 'Your information has been received. Please check your email for more information from our team.'


@app.route('/stock_notify', methods=['POST'])
@limiter.limit('20/minute')  # 10 requests per minute
def stock_notification():
    """Get contact and product information from user who wants notification of when
    a product comes back into stock."""
    ProcessInErrorHandler.logger.log_file = f'log_{datetime.now():%m_%d_%y}.log'

    data = request.json
    # Sanitize the input data
    sanitized_data = {k: bleach.clean(v) for k, v in data.items()}

    # Validate the input data
    try:
        validate(instance=sanitized_data, schema=creds.stock_notification_schema)
    except ValidationError as e:
        ProcessInErrorHandler.error_handler.add_error_v(error=f'Invalid input data: {e}', origin='stock_notify')
        abort(400, description='Invalid input data')
    else:
        email = sanitized_data.get('email')
        item_no = sanitized_data.get('sku')
        try:
            df = pandas.read_csv(creds.stock_notification_log)
        except FileNotFoundError:
            pass
        else:
            entries = df.to_dict('records')
            for x in entries:
                if x['email'] == email and str(x['item_no']) == item_no:
                    return (
                        'This email address is already on file for this item. We will send you an email '
                        'when it comes back in stock. Please contact our office at '
                        "<a href='tel:8288740679'>(828) 874-0679</a> if you need an alternative "
                        'item. Thank you!'
                    ), 400

        stock_notification_data = [[str(datetime.now())[:-7], email, item_no]]
        df = pandas.DataFrame(stock_notification_data, columns=['date', 'email', str('item_no')])
        log_engine.write_log(df, creds.stock_notification_log)
        return 'Your submission was received.'


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
            response = Database.db.query(query=query)
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
            response = Database.db.query(
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
        response = Database.db.query(
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
        Database.db.query(
            f"""
            UPDATE SN_GFC_RECPS
            SET NAME = '{name}', LST_MAINT_DT = GETDATE()
            WHERE UUID = '{uuid}'
            """,
            commit=True,
        )
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(error=f'Error updating: {e}', origin='update-uuid-name')
        return jsonify({'error': 'Error updating'}), 400

    return jsonify({'message': 'Name updated.'}), 200


@app.route('/newsletter', methods=['POST'])
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
        try:
            df = pandas.read_csv(creds.newsletter_log)
        except FileNotFoundError:
            ProcessInErrorHandler.error_handler.add_error_v(error='File not found', origin='newsletter')
        else:
            entries = df.to_dict('records')
            for x in entries:
                if x['email'] == email:
                    print(f'{email} is already on file')
                    return 'This email address is already on file.', 400

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
            newsletter_data = [[f'{datetime.now():%Y-%m-%d %H:%M:%S}', email]]
            df = pandas.DataFrame(newsletter_data, columns=['date', 'email'])
            log_engine.write_log(df, creds.newsletter_log)
            return 'OK', 200


@app.route('/sms', methods=['POST'])
@limiter.limit('40/minute')  # rate limiter
def incoming_sms():
    """Webhook route for incoming SMS/MMS messages to be used with client messenger application.
    Saves all incoming SMS/MMS messages to share drive csv file."""
    raw_data = request.get_data()
    # Decode
    string_code = raw_data.decode('utf-8')
    # Parse to dictionary
    msg = urllib.parse.parse_qs(string_code)

    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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

    # Get Customer Name and Category from SQL
    customer_number, full_name, category = SMSEngine.lookup_customer_data(from_phone)

    log_data = [[date, to_phone, from_phone, body, full_name, category.title(), media_url]]

    # Write dataframe to CSV file
    df = pandas.DataFrame(log_data, columns=['date', 'to_phone', 'from_phone', 'body', 'name', 'category', 'media'])
    log_engine.write_log(df, creds.incoming_sms_log)

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
    if body.lower() in ['stop', 'unsubscribe', 'stop please', 'please stop', 'cancel', 'opt out', 'remove me']:
        SMSEngine.unsubscribe(
            origin='WEBHOOK',
            campaign='/SMS',
            cust_no=customer_number,
            name=full_name,
            category=category,
            phone=from_phone,
        )
        # Return Response to Twilio
        resp = MessagingResponse()
        return str(resp)

    # Subscribe user to SMS marketing
    elif body.lower() in ['start', 'subscribe', 'start please', 'please start', 'opt in', 'add me']:
        SMSEngine.subscribe(
            origin='WEBHOOK',
            campaign='/SMS',
            cust_no=customer_number,
            name=full_name,
            category=category,
            phone=from_phone,
        )
        # Return Response to Twilio
        resp = MessagingResponse()
        return str(resp)

    # Return Response to Twilio
    resp = MessagingResponse()
    return str(resp)


@app.route('/bc', methods=['POST'])
@limiter.limit('20/minute')
def bc_orders():
    """Webhook route for incoming orders. Sends to RabbitMQ queue for asynchronous processing"""
    response_data = request.get_json()
    order_id = response_data['data']['node']['id']

    ProcessInErrorHandler.logger.info(f'Received order {order_id}')

    # Send order to RabbitMQ for asynchronous processing
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='bc_orders', durable=True)

        channel.basic_publish(
            exchange='',
            routing_key='bc_orders',
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}', origin='bc_orders'
        )

    return jsonify({'success': True}), 200


@app.route(creds.route_shopify_order_create, methods=['POST'])
@limiter.limit('20/minute')
def shopify():
    """Webhook route for incoming orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
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
            error=f'Error sending order {order_id} to RabbitMQ: {e}', origin='shopify_orders'
        )

    return jsonify({'success': True}), 200


@app.route(creds.route_shopify_draft_create, methods=['POST'])
@limiter.limit('20/minute')
def shopify_draft_create():
    """Webhook route for newly created draft orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
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
            error=f'Error sending order {order_id} to RabbitMQ: {e}', origin='shopify_orders'
        )

    return jsonify({'success': True}), 200


@app.route(creds.route_shopify_draft_update, methods=['POST'])
@limiter.limit('20/minute')
def shopify_draft_update():
    """Webhook route for updated draft orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
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
            error=f'Error sending order {order_id} to RabbitMQ: {e}', origin='shopify_orders'
        )

    return jsonify({'success': True}), 200


@app.route(creds.route_shopify_customer_update, methods=['POST'])
@limiter.limit('60/minute')
def shopify_customer_update():
    """Webhook route for updated customers. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')

    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    with open('customer_update.json', 'a') as f:
        json.dump(webhook_data, f)
    # customer_id = webhook_data['id']
    # try:
    #     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    #     channel = connection.channel()

    #     channel.queue_declare(queue='shopify_customer_update', durable=True)

    #     channel.basic_publish(
    #         exchange='',
    #         routing_key='shopify_customer_update',
    #         body=str(customer_id),
    #         properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    #     )
    #     connection.close()
    # except Exception as e:
    #     ProcessInErrorHandler.error_handler.add_error_v(
    #         error=f'Error sending customer {customer_id} to RabbitMQ: {e}', origin='shopify_customer_update'
    #     )

    return jsonify({'success': True}), 200


@app.route(creds.route_shopify_product_update, methods=['POST'])
@limiter.limit('60/minute')
def shopify_product_update():
    """Webhook route for updated products. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
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
    item_no = Database.Shopify.Product.get_parent_item_no(product_id)

    with open('product_update.txt', 'a') as f:
        print(item_no, title, file=f)

    with open('product_update.json', 'a') as f:
        json.dump(webhook_data, f)

    if item_no and description:
        Database.Counterpoint.Product.HTMLDescription.update(item_no=item_no, description=description)

    # Get SEO data
    seo_data = Shopify.Product.SEO.get(product_id)
    meta_title = seo_data['title']
    meta_description = seo_data['description']

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
                error=f'Error updating product {item_no}: {e}', origin='shopify_product_update', traceback=tb()
            )

    return jsonify({'success': True}), 200


@app.route('/token', methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_token():
    password = request.args.get('password')

    if password.lower() == creds.commercial_availability_pw:
        session = authorization.Session(password)
        authorization.SESSIONS.append(session)
        return jsonify({'token': session.token, 'expires': session.expires}), 200

    ProcessInErrorHandler.error_handler.add_error_v(error=f'Invalid password: {password} ', origin='get_token')
    return jsonify({'error': 'Invalid username or password'}), 401


@app.route('/commercialAvailability', methods=['POST'])
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
            error='Error fetching data', origin='commercialAvailability'
        )
        return jsonify({'error': 'Error fetching data'}), 500


@app.route('/availability', methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_availability():
    response = requests.get(creds.retail_availability_url)
    if response.status_code == 200:
        return jsonify({'data': response.text}), 200
    else:
        ProcessInErrorHandler.error_handler.add_error_v(error='Error fetching data', origin='availability')
        return jsonify({'error': 'Error fetching data'}), 500


@app.route('/health', methods=['GET'])
@limiter.limit('10/minute')  # 10 requests per minute
def health_check():
    ProcessInErrorHandler.logger.success('Server is running')
    return jsonify({'status': 'Server is running'}), 200


@app.route('/files/<path:path>', methods=['GET'])
def serve_file(path):
    try:
        return send_from_directory(creds.public_files, path)
    except NotFound:
        return jsonify({'error': 'File not found'}), 404
    except BadRequest:
        return jsonify({'error': 'Bad request'}), 400
    except Exception as e:
        ProcessOutErrorHandler.error_handler.add_error_v(error=f'Error serving file: {e}', origin='serve_file')
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/qr/<qr_id>', methods=['GET'])
def qr_tracker(qr_id):
    QR.visit(qr_id)
    return jsonify({'success': True}), 200


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


if __name__ == '__main__':
    if dev:
        app.run(debug=True, port=creds.flask_port)
    else:
        running = True
        while running:
            try:
                print('Flask Server Running')
                serve(app, host='localhost', port=creds.flask_port)
            except Exception as e:
                print('Error serving Flask app: ', e)
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f'Error serving Flask app: {e}', origin='server'
                )
                time.sleep(5)
            # Stop the server if Keyboard Interrupt
            running = False
            print('Flask Server Stopped')
