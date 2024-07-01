import numpy as np
import pandas
from jinja2 import Template

import customer_tools.customers
from setup import create_log
from setup import creds
from setup.date_presets import *
from setup.email_engine import send_html_email


def lead_notification_email(recipients, log_file):
	"""Renders Jinja2 template and sends HTML email to sales team with leads from yesterday
	for follow-up"""
	print(f'Lead Notification Email: Starting at {datetime.now():%H:%M:%S}', file=log_file)
	with open(creds.design_lead_log, encoding='utf-8') as lead_file:
		# Dataframe for Log
		df = pandas.read_csv(lead_file)
		df = df.replace({np.nan: None})
		entries = df.to_dict('records')
		# Get yesterday submissions
		yesterday_entries = []
		for x in entries:
			# 4/25/24 note: yesterday is a dt object whereas today is a string
			if x['date'][:10] == f'{yesterday:%Y-%m-%d}' or x['date'][:10] == today:
				yesterday_entries.append(x)

		if len(yesterday_entries) < 1:
			print('No entries to send.', file=log_file)
		else:
			print(
				f'{len(yesterday_entries)} leads from yesterday. Constructing Email', file=log_file
			)
			with open('./reporting/templates/follow_up.html', 'r') as template_file:
				template_str = template_file.read()

			jinja_template = Template(template_str)

			email_data = {
				'title': 'Design Lead Followup Email',
				'company': creds.company_name,
				'leads': yesterday_entries,
				'format': create_log,
				'date_format': datetime,
			}

			email_content = jinja_template.render(email_data)

			try:
				send_html_email(
					from_name=creds.company_name,
					from_address=creds.sales_email,
					from_pw=creds.sales_password,
					recipients_list=recipients,
					subject='Landscape Design Leads',
					content=email_content,
					mode='related',
					logo=True,
					staff=True,
				)

			except Exception as err:
				print('Error: Sending Email', file=log_file)
				print(err, file=log_file)

			else:
				print('Email Sent.', file=log_file)

	print(f'Lead Notification Email: Finished at {datetime.now():%H:%M:%S}', file=log_file)
	print('-----------------------', file=log_file)


def create_new_customers(log_file):
	"""Send yesterday's entry's to Counterpoint as new customer_tools for further marketing.
	Will skip customer if email or phone is already in our system."""
	print('Create New Customers: Starting', file=log_file)
	with open(creds.design_lead_log, encoding='utf-8') as lead_file:
		# Dataframe for Log
		df = pandas.read_csv(lead_file)
		df = df.replace({np.nan: None})
		entries = df.to_dict('records')

		# Get yesterday submissions
		today_entries = []

		for x in entries:
			if x['date'][:10] == str(today):
				today_entries.append(x)

		if len(today_entries) > 0:
			print('HERE')
			for x in today_entries:
				first_name = x['first_name']
				last_name = x['last_name']
				phone_number = x['phone']
				email = x['email']
				street_address = x['street']
				city = x['city']
				state = x['state']
				zip_code = x['zip_code']
				if not customer_tools.customers.is_customer(
					email_address=x['email'], phone_number=x['phone']
				):
					# Add new customer via NCR Counterpoint API
					customer_number = customer_tools.customers.add_new_customer(
						first_name=first_name,
						last_name=last_name,
						phone_number=phone_number,
						email_address=email,
						street_address=street_address,
						city=city,
						state=state,
						zip_code=zip_code,
					)
					# Log on share
					log_data = [
						[
							str(datetime.now())[:-7],
							customer_number,
							first_name,
							last_name,
							phone_number,
							email,
							street_address,
							city,
							state,
							zip_code,
						]
					]
					df = pandas.DataFrame(
						log_data,
						columns=[
							'date',
							'customer_number',
							'first_name',
							'last_name',
							'phone_number',
							'email',
							'street',
							'city',
							'state',
							'zip_code',
						],
					)
					create_log.write_log(df, creds.new_customer_log)
					print(
						f'Created customer: {customer_number}: {first_name} {last_name}',
						file=log_file,
					)
				else:
					print(
						f'{first_name} {last_name} is already a customer. Skipping customer creation.',
						file=log_file,
					)
		else:
			print('No new customer_tools to add', file=log_file)

	print(f'Create New Customers: Finished at {datetime.now():%H:%M:%S}', file=log_file)
	print('-----------------------', file=log_file)
