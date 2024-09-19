from database import Database
from setup import creds
from setup.creds import Route
from setup.error_handler import ProcessInErrorHandler
from flask import Blueprint, request, jsonify, url_for
from jinja2 import Template
from jsonschema import validate, ValidationError
from flask import abort
import requests
from setup.email_engine import Email
import bleach
import json
import pika
import base64
import hmac
import hashlib
from datetime import datetime
from setup.error_handler import LeadFormErrorHandler
from traceback import format_tb as tb
from setup.utilities import EmailAddress, PhoneNumber
import time
from qr.qr_codes import QR
from routes.limiter import limiter


marketing_routes = Blueprint('marketing_routes', __name__, template_folder='routes')


@marketing_routes.route(Route.design, methods=['POST'])
@limiter.limit('20/minute')  # 10 requests per minute
def marketing_lead_design():
    """Route for information request about company service. Sends JSON to RabbitMQ for asynchronous processing."""
    LeadFormErrorHandler.logger.log_file = f'design_leads_{datetime.now().strftime("%m_%d_%y")}.log'
    token = request.headers.get('Authorization').split(' ')[1]
    url = 'https://www.google.com/recaptcha/api/siteverify'
    payload = {'secret': creds.Company.recaptcha_secret, 'response': token}
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

        channel.queue_declare(queue=creds.Consumer.design_lead_form, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.Consumer.design_lead_form,
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


@marketing_routes.route(Route.design_admin, methods=['POST'])
@limiter.limit('20/minute')  # 10 requests per minute
def marketing_lead_design_admin():
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
    if not verify_hmac(creds.Company.design_admin_key, message, signature):
        return jsonify({'error': 'Invalid token'}), 401
    data = request.json
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=creds.Consumer.design_lead_form, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=creds.Consumer.design_lead_form,
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


@marketing_routes.route(Route.stock_notify, methods=['POST'])
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
    payload = {'secret': creds.Company.recaptcha_secret, 'response': token}
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


@marketing_routes.route(Route.newsletter, methods=['POST'])
@limiter.limit('20 per minute')
def newsletter_signup():
    """Route for website pop-up. Offers user a coupon and adds their information to a csv."""
    token = request.headers.get('Authorization').split(' ')[1]

    url = 'https://www.google.com/recaptcha/api/siteverify'
    payload = {'secret': creds.Company.recaptcha_secret, 'response': token}
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

        # Send welcome email
        recipient = {'': email}
        with open('./templates/new10.html', 'r') as file:
            template_str = file.read()

        jinja_template = Template(template_str)

        email_data = {
            'title': f'Welcome to {creds.Company.name}',
            'greeting': 'Hi!',
            'service': creds.service,
            'coupon': 'NEW10',
            'company': creds.Company.name,
            'list_items': creds.list_items,
            'signature_name': creds.signature_name,
            'signature_title': creds.signature_title,
            'company_phone': creds.Company.phone,
            'company_url': creds.Company.url,
            'company_reviews': creds.Company.reviews,
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
            res = Database.Newsletter.subscribe(email)
            if res['code'] != 200:
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f'Error adding {email} to newsletter: {res["message"]}', origin=Route.newsletter
                )
                return 'Error adding email to newsletter.', 500
            return 'OK', 200


@marketing_routes.route(Route.unsubscribe, methods=['GET'])
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
            'endpoint': f'{creds.Company.API.endpoint}/{Route.subscribe}',
            'message': message,
            'code': code,
            'company_url': creds.Company.url,
        }
        content = jinja_template.render(data, url_for=url_for)

    return content, 200


@marketing_routes.route(Route.subscribe, methods=['GET'])
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
            'endpoint': f'{creds.Company.API.endpoint}/{Route.unsubscribe}',
            'message': message,
            'code': code,
            'company_url': creds.Company.url,
        }
        content = jinja_template.render(data, url_for=url_for)

    return content, 200


@marketing_routes.route(f'{Route.qr}/<qr_id>', methods=['GET'])
def qr_tracker(qr_id):
    QR.visit(qr_id)
    return jsonify({'success': True}), 200
