from datetime import datetime

import pandas
import pytz
from dateutil import tz
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client

from setup import creds
from setup.query_engine import QueryEngine
from setup import log_engine

est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S %Z%z'
FROM_ZONE = tz.gettz('UTC')
TO_ZONE = tz.gettz('America/New_York')


class SMSEngine:
	def __init__(self):
		self.phone = creds.twilio_phone_number
		self.sid = creds.twilio_account_sid
		self.token = creds.twilio_auth_token

	def send_text(self, name, to_phone, message, log_location, create_log=True, test_mode=False):
		twilio_response = ''
		if test_mode:
			print(f'Sending test sms text to {name}: {message}')
			twilio_response = 'Test Mode'
		if not test_mode:
			# for SMS Messages
			client = Client(self.sid, self.token)
			try:
				twilio_message = client.messages.create(from_=self.phone, to=to_phone, body=message)

			except TwilioRestException as err:
				if str(err)[-22:] == 'is not a mobile number':
					twilio_response = 'landline'
			else:
				twilio_response = twilio_message.sid
				print(twilio_message.to, twilio_message.body)

		if create_log:
			create_sms_log(name, to_phone, message, twilio_response, log_location=log_location)


def create_sms_log(name, phone, sent_message, response, log_location):
	"""Creates a log file on share server. Logs date, phone, message, and twilio response"""
	log_message = sent_message
	log_data = [
		[
			str(datetime.now())[:-7],
			name,
			format_phone(phone, mode='Counterpoint'),
			log_message.strip().replace('\n', ''),
			response,
		]
	]
	df = pandas.DataFrame(log_data, columns=['date', 'name', 'to_phone', 'body', 'response'])
	# Looks for file. If it has been deleted, it will recreate.

	log_engine.write_log(df, log_location)


def format_phone(phone_number, mode='clickable', prefix=False):
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
		# Masking ###-###-####
		clickable_phone = (
			'(' + formatted_phone[0:3] + ') ' + formatted_phone[3:6] + '-' + formatted_phone[6:10]
		)
		return clickable_phone

	else:
		if prefix:
			formatted_phone = '+1' + formatted_phone
		return formatted_phone


def lookup_customer_data(phone):
	# Format phone for Counterpoint masking ###-###-####
	cp_phone_input = format_phone(phone, mode='counterpoint')
	# Create customer variables from tuple return of query_db
	db = QueryEngine()
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
			local_datetime = convert_timezone(
				timestamp=record.date_sent, from_zone=FROM_ZONE, to_zone=TO_ZONE
			)
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
		message_list,
		columns=['date', 'to_phone', 'from_phone', 'body', 'name', 'category', 'media'],
	)
	df.to_csv(creds.incoming_sms_log, index=False)


def convert_timezone(timestamp, from_zone, to_zone):
	"""Convert from UTC to Local Time"""
	start_time = timestamp.replace(tzinfo=from_zone)
	result_time = start_time.astimezone(to_zone).strftime('%Y-%m-%d %H:%M:%S')
	return result_time


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
	sms = SMSEngine()
	if test_mode:
		for k, v in creds.test_recipient.items():
			sms.send_text(
				name=name,
				to_phone=format_phone(v, prefix=True),
				message=message,
				log_location=creds.sms_log,
				create_log=True,
			)
	else:
		for k, v in creds.lead_recipient.items():
			sms.send_text(
				name=name,
				to_phone=format_phone(v, prefix=True),
				message=message,
				log_location=creds.sms_log,
				create_log=True,
			)


def unsubscribe_from_sms(phone_number):
	query = f"""
    UPDATE AR_CUST
    SET INCLUDE_IN_MARKETING_MAILOUTS = 'N'
    WHERE PHONE_1 = '{phone_number}' OR PHONE_2 = '{phone_number}'
    """
	db = QueryEngine()
	db.query_db(query=query, commit=True)

	date = f'{datetime.now():%m-%d-%Y %H:%M:%S}'

	log_data = [[date, phone_number]]
	df = pandas.DataFrame(log_data, columns=['date', 'phone'])
	log_engine.write_log(df, creds.sms_unsubscribe)
