import pytz
from dateutil import tz
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from setup import creds
from integration.database import Database
from setup.error_handler import SMSErrorHandler
from setup.utilities import PhoneNumber

est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S %Z%z'
FROM_ZONE = tz.gettz('UTC')
TO_ZONE = tz.gettz('America/New_York')


class SMSEngine:
    logger = SMSErrorHandler.logger
    error_handler = SMSErrorHandler.error_handler
    phone = creds.twilio_phone_number
    sid = creds.twilio_account_sid
    token = creds.twilio_auth_token

    @staticmethod
    def send_text(
        origin,
        campaign,
        to_phone,
        message,
        category=None,
        username=None,
        name=None,
        cust_no=None,
        url=None,
        test_mode=False,
    ):
        # Format phone for Twilio API
        formatted_phone = PhoneNumber(to_phone).to_twilio()

        if category is None:
            if origin != 'SERVER':
                category = (Database.Counterpoint.Customer.get_customer_by_phone(to_phone))[2]

        error_code = None
        error_message = None

        if test_mode:
            SMSEngine.logger.info(f'Sending test sms text to {name}: {message}')
        else:
            # for SMS Messages
            client = Client(SMSEngine.sid, SMSEngine.token)
            try:
                # MMS
                if url:
                    twilio_message = client.messages.create(
                        from_=SMSEngine.phone, media_url=url, to=formatted_phone, body=message
                    )
                else:
                    # SMS
                    twilio_message = client.messages.create(from_=SMSEngine.phone, to=formatted_phone, body=message)

            except TwilioRestException as err:
                if err.code in [21614, 30003, 30005, 30006]:
                    SMSEngine.error_handler.add_error_v(
                        f'Code: {err.code} - Error sending SMS to {name}: {err.msg}'
                    )
                    Database.SMS.move_phone_1_to_landline(
                        origin=origin,
                        campaign=campaign,
                        cust_no=cust_no,
                        name=name,
                        category=category,
                        phone=to_phone,
                    )
                error_code = err.code
                error_message = err.msg
            except Exception as e:
                SMSEngine.error_handler.add_error_v(f'Error sending SMS to {name}: {e}')

            else:
                SMSEngine.logger.success(
                    message=f'{twilio_message.to}, {twilio_message.body}, {twilio_message.sid}'
                )

                Database.SMS.insert(
                    origin=origin,
                    campaign=campaign,
                    to_phone=to_phone,
                    from_phone=creds.twilio_phone_number,
                    cust_no=cust_no,
                    body=message,
                    username=username,
                    name=name,
                    category=category,
                    media=url,
                    sid=twilio_message.sid,
                    error_code=error_code,
                    error_message=error_message,
                )

    @staticmethod
    def design_text(
        first_name, last_name, email, phone, interested_in, timeline, address, comments, test_mode=False
    ):
        """Send text message to sales team mangers for customer followup"""
        name = f'{first_name} {last_name}'.title()
        message = (
            f'{name} just requested a phone follow-up about {creds.service}.\n'
            f'Interested in: {interested_in}\n'
            f'Timeline: {timeline}\n'
            f'Email: {email} \n'
            f'Phone: {PhoneNumber(phone).to_cp()} \n'
            f'Address: {address} \n'
            f'Comments: {comments}'
        )
        if test_mode:
            recipient = creds.test_recipient
        else:
            recipient = creds.lead_recipient

        for k, v in recipient.items():
            SMSEngine.send_text(origin='SERVER', campaign='DESIGN FORM', name=name, to_phone=v, message=message)
