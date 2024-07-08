import pandas
import pytz
from dateutil import tz
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from setup import creds
from setup.query_engine import QueryEngine
from setup.error_handler import SMSErrorHandler

est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S %Z%z'
FROM_ZONE = tz.gettz('UTC')
TO_ZONE = tz.gettz('America/New_York')


class SMSEngine:
	def __init__(self):
		self.db = QueryEngine()
		self.phone = creds.twilio_phone_number
		self.sid = creds.twilio_account_sid
		self.token = creds.twilio_auth_token
		self.error_handler = SMSErrorHandler.error_handler
		self.logger = SMSErrorHandler.logger

	def send_text(self, to_phone, message, name=None, cust_no=None, url=None, test_mode=False):
		# Format phone for Twilio API
		formatted_phone = format_phone(to_phone, mode='twilio')

		if test_mode:
			self.logger.info(f'Sending test sms text to {name}: {message}')
		else:
			# for SMS Messages
			client = Client(self.sid, self.token)
			try:
				# MMS
				if url:
					twilio_message = client.messages.create(
						from_=self.phone, media_url=url, to=formatted_phone, body=message
					)
				else:
					# SMS
					twilio_message = client.messages.create(from_=self.phone, to=formatted_phone, body=message)

			except TwilioRestException as err:
				if err.code in [21614, 30003, 30005, 30006]:
					self.error_handler.add_error_v(f'Code: {err.code} - Error sending SMS to {name}: {err.msg}')
					self.move_phone_1_to_landline(cust_no, to_phone)
			except Exception as e:
				self.error_handler.add_error_v(f'Error sending SMS to {name}: {e}')

			else:
				self.logger.success(message=f'{twilio_message.to}, {twilio_message.body}, {twilio_message.sid}')

	def move_phone_1_to_landline(self, cust_no, phone_number):
		cp_phone = self.format_phone(phone_number, mode='counterpoint')
		move_landline_query = f"""
            UPDATE AR_CUST
            SET MBL_PHONE_1 = '{cp_phone}', SET PHONE_1 = NULL
            WHERE PHONE_1 = '{cp_phone}'
        """
		response = self.db.query_db(move_landline_query, commit=True)
		if response['code'] == 200:
			self.logger.success(f'Moved {phone_number} to landline for customer {cust_no}')
		else:
			self.error_handler.add_error_v(f'Error moving {phone_number} to landline')

	def unsubscribe_from_sms(self, phone_number):
		query = f"""
        UPDATE AR_CUST
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'N'
        WHERE PHONE_1 = '{phone_number}' OR PHONE_2 = '{phone_number}'
        """
		response = self.db.query_db(query=query, commit=True)
		if response['code'] == 200:
			self.logger.success(f'Unsubscribed {phone_number} from SMS')
		else:
			self.error_handler.add_error_v(f'Error unsubscribing {phone_number} from SMS')


def lookup_customer_data(phone):
	# Format phone for Counterpoint masking ###-###-####
	cp_phone_input = format_phone(phone, mode='counterpoint')
	db = QueryEngine()
	# Create customer variables from tuple return of query_db
	query = f"""
        SELECT FST_NAM, LST_NAM, CATEG_COD
        FROM AR_CUST
        WHERE PHONE_1 = '{cp_phone_input}'
        """
	response = db.query_db(query)

	if response is not None:
		first_name = response[0][0]
		last_name = response[0][1]
		full_name = first_name + ' ' + last_name
		category = response[0][2]
		# For people with no phone in our database
	else:
		full_name = 'Unknown'
		category = 'Unknown'

	return full_name, category


def format_phone(phone_number, mode='clickable'):
	"""Cleanses input data and returns masked phone for either Twilio or Counterpoint configuration"""
	phone_number_as_string = str(phone_number)
	# Strip away extra symbols
	formatted_phone = phone_number_as_string.replace(' ', '')  # Remove Spaces
	formatted_phone = formatted_phone.replace('-', '')  # Remove Hyphens
	formatted_phone = formatted_phone.replace('(', '')  # Remove Open Parenthesis
	formatted_phone = formatted_phone.replace(')', '')  # Remove Close Parenthesis
	formatted_phone = formatted_phone.replace('+1', '')  # Remove +1
	formatted_phone = formatted_phone[-10:]  # Get last 10 characters
	if mode == 'counterpoint':
		# Masking ###-###-####
		cp_phone = formatted_phone[0:3] + '-' + formatted_phone[3:6] + '-' + formatted_phone[6:10]
		return cp_phone

	elif mode == 'clickable':
		# Masking (###) ###-####
		clickable_phone = '(' + formatted_phone[0:3] + ') ' + formatted_phone[3:6] + '-' + formatted_phone[6:10]
		return clickable_phone

	elif mode == 'twilio':
		formatted_phone = '+1' + formatted_phone

	return formatted_phone


def write_all_twilio_messages_to_share():
	"""Gets all messages from twilio API and writes to .csv on share drive"""
	client = Client(creds.twilio_account_sid, creds.twilio_auth_token)
	messages = client.messages.list(to=creds.twilio_phone_number)

	# Empty List
	message_list = []

	# Loop through message response in reverse order and format data
	for record in messages[-1::-1]:
		customer_name, customer_category = lookup_customer_data(record.from_)
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
					media_url[0:8] + creds.twilio_account_sid + ':' + creds.twilio_auth_token + '@' + media_url[8:]
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
	df = pandas.DataFrame(message_list, columns=['date', 'to_phone', 'from_phone', 'body', 'name', 'category', 'media'])
	df.to_csv(creds.incoming_sms_log, index=False)


def convert_timezone(timestamp, from_zone, to_zone):
	"""Convert from UTC to Local Time"""
	start_time = timestamp.replace(tzinfo=from_zone)
	result_time = start_time.astimezone(to_zone).strftime('%Y-%m-%d %H:%M:%S')
	return result_time


def design_text(first_name, last_name, email, phone, interested_in, timeline, address, comments, test_mode=False):
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
	sms = SMSEngine()
	if test_mode:
		for k, v in creds.test_recipient.items():
			sms.send_text(name=name, to_phone=v, message=message)
	else:
		for k, v in creds.lead_recipient.items():
			sms.send_text(name=name, to_phone=v, message=message)
