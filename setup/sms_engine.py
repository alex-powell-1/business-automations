from setup import creds
from setup.create_log import *
from setup.query_engine import QueryEngine
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

db = QueryEngine()


class SMSEngine:
    def __init__(self):
        self.phone = creds.twilio_phone_number
        self.sid = creds.twilio_account_sid
        self.token = creds.twilio_auth_token

    def send_text(self, cust_no, to_phone, message, log_code, url="", create_log=True, test_mode=False):
        twilio_response = ""
        if test_mode:
            print(f"Sending test sms text to {cust_no}: {message}")
            twilio_response = "Test Mode"
        if not test_mode:
            # for SMS Messages
            client = Client(self.sid, self.token)
            try:
                twilio_message = client.messages.create(
                    from_=self.phone,
                    to=to_phone,
                    media_url=url,
                    body=message)
            except TwilioRestException as err:
                if str(err)[-22:] == "is not a mobile number":
                    twilio_response = "landline"
            else:
                twilio_response = twilio_message.sid
                print(twilio_message.to, twilio_message.body)

        if create_log:
            create_sms_log(cust_no, to_phone, message, twilio_response, log_code)

        if twilio_response == "landline":
            move_phone_1_to_mbl_phone_1(cust_no, to_phone)


def move_phone_1_to_mbl_phone_1(cust_no, phone_number):
    cp_phone = format_phone(phone_number, mode="Counterpoint")
    move_landline_query = f"""
        UPDATE AR_CUST
        SET MBL_PHONE_1 = '{cp_phone}'
        WHERE PHONE_1 = '{cp_phone}'

        UPDATE AR_CUST
        SET PHONE_1 = NULL
        WHERE MBL_PHONE_1 = '{cp_phone}'
    """
    db.query_db(move_landline_query, commit=True)
    create_sms_log(cust_no=cust_no, phone=cp_phone, sent_message="Landline", response="Changed in CP", log_code=4)
