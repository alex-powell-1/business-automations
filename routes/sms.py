from flask import Blueprint, request
from twilio.twiml.messaging_response import MessagingResponse
from setup import creds
from setup.creds import API
from database import Database
from traceback import format_exc as tb
import urllib.parse
import pika
from routes.limiter import limiter
from setup.error_handler import ProcessInErrorHandler

sms_routes = Blueprint('sms_routes', __name__, template_folder='routes')


@sms_routes.route(API.Route.sms, methods=['POST'])
@limiter.limit('10/second')  # rate limiter
def incoming_sms():
    """Webhook route for incoming SMS/MMS messages to be used with client messenger application.
    Saves all incoming SMS/MMS messages to share drive csv file."""
    eh = ProcessInErrorHandler
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

    if body.lower() == creds.Integrator.sms_sync_keyword:
        # Run sync process
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()

            channel.queue_declare(queue=creds.Consumer.sync_on_demand, durable=True)

            channel.basic_publish(
                exchange='',
                routing_key=creds.Consumer.sync_on_demand,
                body=from_phone,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
            )
            connection.close()

        except Exception as e:
            eh.error_handler.add_error_v(
                error=f'Error sending sync request  to RabbitMQ: {e}', origin=API.Route.sms, traceback=tb()
            )

        # Return Response to Twilio
        resp = MessagingResponse()
        return str(resp)

    elif body.lower().startswith(creds.Integrator.sms_sync_keyword):
        if body.lower().strip().endswith('restart'):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                channel = connection.channel()

                channel.queue_declare(queue=creds.Consumer.sync_on_demand, durable=True)

                channel.basic_publish(
                    exchange='',
                    routing_key=creds.Consumer.restart_services,
                    body=from_phone,
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
                )
                connection.close()

            except Exception as e:
                eh.error_handler.add_error_v(
                    error=f'Error sending sync request  to RabbitMQ: {e}', origin=API.Route.sms, traceback=tb()
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
        eh=eh,
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
            campaign=API.Route.sms,
            cust_no=customer_number,
            name=full_name,
            category=category,
            phone=from_phone,
            eh=eh,
        )

    # Subscribe user to SMS marketing
    elif body.lower().strip() in ['start', 'subscribe', 'start please', 'please start', 'opt in', 'add me']:
        Database.SMS.subscribe(
            origin='WEBHOOK',
            campaign=API.Route.sms,
            cust_no=customer_number,
            name=full_name,
            category=category,
            phone=from_phone,
            eh=eh,
        )

    # Return Response to Twilio
    resp = MessagingResponse()
    return str(resp)
