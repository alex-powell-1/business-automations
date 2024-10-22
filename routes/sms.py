from flask import Blueprint, request
from twilio.twiml.messaging_response import MessagingResponse
from setup.creds import API
from database import Database
import urllib.parse
from routes.limiter import limiter
from setup.error_handler import ProcessInErrorHandler
from setup.sms_engine import SMSEngine
from setup.utilities import is_after_hours, get_hours_message

sms_routes = Blueprint('sms_routes', __name__, template_folder='routes')


def get_media(msg: dict) -> str:
    """Get media URL from MMS messages."""
    media_url = ''
    if int(msg['NumMedia'][0]) > 0:
        for i in range(int(msg['NumMedia'][0])):
            media_key_index = f'MediaUrl{i}'
            url = msg[media_key_index][0]
            if i < (int(msg['NumMedia'][0]) - 1):
                # Add separator per front-end request.
                media_url += url + ';;;'
            else:
                media_url += url

    return media_url


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

    media_url = get_media(msg)

    # Get Customer Name and Category from SQL
    customer_number, full_name, category = Database.CP.Customer.get_customer_by_phone(from_phone)

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
        Database.SMS.unsubscribe(origin='WEBHOOK', campaign=API.Route.sms, phone=from_phone, eh=eh)

    if is_after_hours():
        hours_response = f"""
        Thank you for your message. 
        Our office hours are {get_hours_message()}. 
        We will respond to your message during our next business hours."""

        SMSEngine.send_text(origin='WEBHOOK', to_phone=from_phone, message=hours_response)

    # Subscribe user to SMS marketing
    elif body.lower().strip() in ['start', 'subscribe', 'start please', 'please start', 'opt in', 'add me']:
        Database.SMS.subscribe(origin='WEBHOOK', campaign=API.Route.sms, phone=from_phone, eh=eh)

    # Return Response to Twilio
    resp = MessagingResponse()
    return str(resp)
