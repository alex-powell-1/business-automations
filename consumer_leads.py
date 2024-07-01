import json
import os
import sys
import time
from datetime import datetime

import pandas
import pika
import requests
from docxtpl import DocxTemplate

from setup import creds, email_engine, sms_engine_flask as sms_engine
from setup import log_engine

from setup.error_handler import LeadFormErrorHandler

test_mode = False


class RabbitMQConsumer:
	def __init__(self, queue_name, host='localhost'):
		self.queue_name = queue_name
		self.host = host
		self.connection = None
		self.channel = None
		self.logger = LeadFormErrorHandler.logger
		self.error_handler = LeadFormErrorHandler.error_handler

	def connect(self):
		parameters = pika.ConnectionParameters(self.host)
		self.connection = pika.BlockingConnection(parameters)
		self.channel = self.connection.channel()
		self.channel.queue_declare(queue=self.queue_name, durable=True)

	def callback(self, ch, method, properties, body):
		json_body = json.loads(body.decode())
		first_name = json_body['first_name']
		last_name = json_body['last_name']
		email = json_body['email']
		phone = sms_engine.format_phone(json_body['phone'], mode='counterpoint')
		timeline = json_body['timeline']
		interested_in = json_body['interested_in']
		street = str(json_body['street']).replace(',', '')
		city = str(json_body['city']).replace(',', '')
		state = json_body['state'] if json_body['state'] != 'State' else ''
		zip_code = str(json_body['zip_code']).replace(',', '')
		# comments = str(json_body['comments']).replace(",", "")
		comments = str(json_body['comments']).replace('"', '""')
		# Concat the address
		address = f'{street}, {city}, {state}, {zip_code}'

		# Concatenate user interests (for text and spreadsheet use)
		interests = ''
		if interested_in is not None:
			for x in interested_in:
				interests += x
				if len(interested_in) > 1:
					interests += ', '
			if len(interested_in) > 1:
				# remove last trailing characters (", ")
				interests = interests[:-2]

		# Log Details
		# establish time for consistent logging
		now = datetime.now()
		now_log_format = f'{now:%Y-%m-%d %H:%M:%S}'
		self.logger.info(now_log_format)
		self.logger.info(f'Received message from {first_name} {last_name}. Beginning Processing...')

		design_lead_data = [
			[
				now_log_format,
				first_name,
				last_name,
				email,
				phone,
				interested_in,
				timeline,
				street,
				city,
				state,
				zip_code,
				comments,
			]
		]
		df = pandas.DataFrame(
			design_lead_data,
			columns=[
				'date',
				'first_name',
				'last_name',
				'email',
				'phone',
				'interested_in',
				'timeline',
				'street',
				'city',
				'state',
				'zip_code',
				'comments',
			],
		)
		log_engine.write_log(df, creds.lead_log)

		# Send text notification To sales team manager
		self.logger.info(f'Sending SMS Message to Sales Team')
		try:
			sms_engine.design_text(
				first_name,
				last_name,
				email,
				phone,
				interests,
				timeline,
				address,
				comments,
				test_mode=test_mode,
			)
		except Exception as err:
			error_type = 'sms'
			error_data = [[now_log_format, error_type, err]]
			df = pandas.DataFrame(error_data, columns=['date', 'error_type', 'message'])
			log_engine.write_log(df, f'{creds.lead_error_log}/error_{now:%m_%d_%y_%H_%M_%S}.csv')
			self.error_handler.add_error_v(error=f'Error ({error_type}): {err}')
		else:
			self.logger.success(f'SMS Sent at {datetime.now():%H:%M:%S}')

		# Send email to client
		self.logger.info('Sending Email to Lead')
		try:
			email_engine.design_email(first_name, email)
		except Exception as err:
			error_type = 'email'
			error_data = [[now_log_format, error_type, err]]
			df = pandas.DataFrame(error_data, columns=['date', 'error_type', 'message'])
			log_engine.write_log(df, f'{creds.lead_error_log}/error_{now:%m_%d_%y_%H_%M_%S}.csv')
			self.error_handler.add_error_v(error=f'Error ({error_type}): {err}')
		else:
			self.logger.success(f'Email Sent at {datetime.now():%H:%M:%S}')
		# Print lead details for in-store use

		# Create the Word document
		self.logger.info('Rendering Word Document')
		try:
			doc = DocxTemplate('./templates/design_lead/lead_print_template.docx')

			context = {
				# Product Details
				'date': now_log_format,
				'name': first_name + ' ' + last_name,
				'email': email,
				'phone': phone,
				'interested_in': interested_in,
				'timeline': timeline,
				'address': address,
				'comments': comments.replace('""', '"'),
			}

			doc.render(context)
			ticket_name = f"lead_{now.strftime("%m_%d_%y_%H_%M_%S")}.docx"
			# Save the rendered file for printing
			doc.save(f'./{ticket_name}')
			# Print the file to default printer
			self.logger.info('Printing Word Document')
			if not test_mode:
				os.startfile(ticket_name, 'print')
			# Delay while print job executes
			time.sleep(4)
			# Delete the unneeded Word document
			# os.close causing crash of printing
			# os.close(1)
			self.logger.info('Deleting Word Document')
			os.remove(ticket_name)
		except Exception as err:
			error_type = 'lead_ticket'
			error_data = [[now_log_format, error_type, err]]
			df = pandas.DataFrame(error_data, columns=['date', 'error_type', 'message'])
			log_engine.write_log(df, f'{creds.lead_error_log}/error_{now:%m_%d_%y_%H_%M_%S}.csv')
			print(f'Error ({error_type}): {err}')
		else:
			print(f'Word Document created, printed, and deleted at {datetime.now():%H:%M:%S}')

		# Upload to sheety API for spreadsheet use
		self.logger.info('Sending Details to Google Sheets')
		sheety_post_body = {
			'sheet1': {
				'date': f'{now:%Y-%m-%d %H:%M:%S}',
				'first': first_name,
				'last': last_name,
				'phone': phone,
				'email': email,
				'interested': interests,
				'timeline': timeline,
				'street': street,
				'city': city,
				'state': state,
				'zip': zip_code,
				'comments': comments,
			}
		}
		try:
			# Try block stands to decouple our implementation from API changes that might impact app.
			requests.post(
				url=creds.sheety_design_url, headers=creds.sheety_header, json=sheety_post_body
			)
		except Exception as err:
			error_type = 'spreadsheet'
			error_data = [[now_log_format, error_type, err]]
			df = pandas.DataFrame(error_data, columns=['date', 'error_type', 'message'])
			log_engine.write_log(df, f'{creds.lead_error_log}/error_{now:%m_%d_%y_%H_%M_%S}.csv')
			self.error_handler.add_error_v(f'Error ({error_type}): {err}')
		else:
			self.logger.success(f'Sent to Google Sheets at {datetime.now():%H:%M:%S}')
		# Done
		self.logger.success(f'Processing Completed at {datetime.now():%H:%M:%S}\n')
		# Send acknowledgement for RabbitMQ to delete from Queue
		ch.basic_ack(delivery_tag=method.delivery_tag)

	def start_consuming(self):
		while True:
			try:
				self.connect()
				self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
				print('Waiting for messages. To exit press CTRL+C')
				self.channel.start_consuming()
			except KeyboardInterrupt:
				sys.exit(0)
			except pika.exceptions.AMQPConnectionError:
				print('Connection lost. Reconnecting...', file=creds.lead_error_log)
				time.sleep(5)  # Wait before attempting reconnection
			except Exception as err:
				print(err, file=creds.lead_error_log)
				time.sleep(5)  # Wait before attempting reconnection


if __name__ == '__main__':
	consumer = RabbitMQConsumer(queue_name='design_info')
	consumer.start_consuming()
