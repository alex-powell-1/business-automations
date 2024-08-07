import pandas
import pytz
from dateutil import tz
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from setup import creds
from integration.database import Database
from setup.query_engine import QueryEngine as db
from setup.utilities import format_phone
from setup.error_handler import SMSErrorHandler, SMSEventHandler
from setup.utilities import convert_timezone

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
        formatted_phone = format_phone(to_phone, mode='twilio')

        if category is None:
            if origin != 'SERVER':
                category = (SMSEngine.lookup_customer_data(to_phone))[2]

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
                    SMSEngine.move_phone_1_to_landline(
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
    def move_phone_1_to_landline(origin, campaign, cust_no, name, category, phone):
        cp_phone = format_phone(phone, mode='counterpoint')
        move_landline_query = f"""
            UPDATE AR_CUST
            SET MBL_PHONE_1 = '{cp_phone}', SET PHONE_1 = NULL
            WHERE PHONE_1 = '{cp_phone}'
        """
        response = db.query(move_landline_query)

        if response['code'] == 200:
            query = f"""
            INSERT INTO {creds.sms_event_log} (ORIGIN, CAMPAIGN, PHONE, CUST_NO, NAME, CATEGORY, EVENT_TYPE, MESSAGE)
            VALUES ('{origin}', '{campaign}', '{phone}', '{cust_no}', '{name}', '{category}', 
            'Landline', 'SET MBL_PHONE_1 = {cp_phone}, SET PHONE_1 = NULL')"""

            response = db.query(query)
            if response['code'] != 200:
                SMSEventHandler.error_handler.add_error_v(f'Error moving {phone} to landline')

        else:
            SMSEventHandler.error_handler.add_error_v(f'Error moving {phone} to landline')

    @staticmethod
    def subscribe(origin, campaign, cust_no, name, category, phone):
        phone = format_phone(phone, mode='counterpoint')
        query = f"""
        UPDATE AR_CUST
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'Y'
        WHERE PHONE_1 = '{phone}' OR PHONE_2 = '{phone}'
        """
        print(query)
        response = db.query(query=query)
        print(response)
        if response['code'] == 200:
            query = f"""
            INSERT INTO {creds.sms_event_log} (ORIGIN, CAMPAIGN, PHONE, CUST_NO, NAME, CATEGORY, EVENT_TYPE, MESSAGE)
            VALUES ('{origin}', '{campaign}', '{phone}', '{cust_no}', '{name}', '{category}',
            'Subscribe', 'SET INCLUDE_IN_MARKETING_MAILOUTS = Y')"""
            print(query)
            response = db.query(query)
            print(response)
            if response['code'] != 200:
                SMSEventHandler.error_handler.add_error_v(f'Error subscribing {phone} to SMS')

        else:
            SMSEventHandler.error_handler.add_error_v(f'Error subscribing {phone} to SMS')

    @staticmethod
    def unsubscribe(origin, campaign, cust_no, name, category, phone):
        phone = format_phone(phone, mode='counterpoint')
        query = f"""
        UPDATE AR_CUST
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'N'
        WHERE PHONE_1 = '{phone}' OR PHONE_2 = '{phone}'
        """
        print(query)
        response = db.query(query=query)
        print(response)
        if response['code'] == 200:
            query = f"""
            INSERT INTO {creds.sms_event_log} (ORIGIN, CAMPAIGN, PHONE, CUST_NO, NAME, CATEGORY, EVENT_TYPE, MESSAGE)
            VALUES ('{origin}', '{campaign}', '{phone}', '{cust_no}', '{name}', '{category}',
            'Unsubscribe', 'SET INCLUDE_IN_MARKETING_MAILOUTS = N')"""
            print(query)
            response = db.query(query)
            print(response)
            if response['code'] != 200:
                SMSEventHandler.error_handler.add_error_v(f'Error unsubscribing {phone} from SMS')

        else:
            SMSEventHandler.error_handler.add_error_v(f'Error unsubscribing {phone} from SMS')

    @staticmethod
    def lookup_customer_data(phone):
        # Format phone for Counterpoint masking ###-###-####
        cp_phone_input = format_phone(phone, mode='counterpoint')
        query = f"""
			SELECT CUST_NO, FST_NAM, LST_NAM, CATEG_COD
			FROM AR_CUST
			WHERE PHONE_1 = '{cp_phone_input}'
			"""
        response = db.query(query)

        if response is not None:
            customer_no = response[0][0]
            first_name = response[0][1]
            last_name = response[0][2]
            full_name = first_name + ' ' + last_name
            category = response[0][3]
        else:
            # For people with no phone in our database
            customer_no = 'Unknown'
            full_name = 'Unknown'
            category = 'Unknown'

        return customer_no, full_name, category

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
            f'Phone: {format_phone(phone)} \n'
            f'Address: {address} \n'
            f'Comments: {comments}'
        )
        if test_mode:
            recipient = creds.test_recipient
        else:
            recipient = creds.lead_recipient

        for k, v in recipient.items():
            SMSEngine.send_text(origin='SERVER', campaign='DESIGN FORM', name=name, to_phone=v, message=message)

    @staticmethod
    def write_all_twilio_messages_to_share():
        """Gets all messages from twilio API and writes to .csv on share drive"""
        client = Client(creds.twilio_account_sid, creds.twilio_auth_token)
        messages = client.messages.list(to=creds.twilio_phone_number)

        # Empty List
        message_list = []

        # Loop through message response in reverse order and format data
        for record in messages[-1::-1]:
            customer_number, customer_name, customer_category = SMSEngine.lookup_customer_data(record.from_)
            # [-1::-1] Twilio supplies data newest to oldest. This reverses that.
            if record.date_sent is not None:
                local_datetime = convert_timezone(timestamp=record.date_sent, from_zone=FROM_ZONE, to_zone=TO_ZONE)
            else:
                continue
            # get rid of extra whitespace
            while '  ' in record.body:
                record.body = record.body.replace('  ', ' ')

            media_url = 'No Media'

            if int(record.num_media) > 0:
                for media in record.media.list():
                    media_url = 'https://api.twilio.com' + media.uri[:-5]  # Strip off the '.json'
                    # Add authorization header
                    media_url = (
                        media_url[0:8]
                        + creds.twilio_account_sid
                        + ':'
                        + creds.twilio_auth_token
                        + '@'
                        + media_url[8:]
                    )

            message_list.append(
                [
                    local_datetime,
                    creds.twilio_phone_number,
                    record.from_,
                    record.body.strip().replace('\n', ' ').replace('\r', ''),
                    customer_name.title(),
                    customer_category.title(),
                    media_url,
                ]
            )

        # Write dataframe to csv
        df = pandas.DataFrame(
            message_list, columns=['date', 'to_phone', 'from_phone', 'body', 'name', 'category', 'media']
        )
        df.to_csv(creds.incoming_sms_log, index=False)
