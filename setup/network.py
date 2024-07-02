import os
from icmplib import ping
import requests
from setup import sms_engine
from setup import creds
from datetime import datetime
from setup.error_handler import ScheduledTasksErrorHandler as error_handler

hosts = ['https://www.google.com/', '1.1.1.1', '8.8.8.8']


def check_for_connection(hostname: str):
	host = ping(hostname, count=5, interval=0.2)
	if host.packets_sent == host.packets_received:
		error_handler.logger.info(f'{hostname} is connected.')
	else:
		error_handler.logger.warn(f'{hostname} is not connected.')
	return host.packets_sent == host.packets_received


def restart_server_if_disconnected():
	error_handler.logger.info(f'Business Automation Health Check: Starting at {datetime.now():%H:%M:%S}')
	if not check_for_connection(hosts[0]) and check_for_connection(hosts[1]) and check_for_connection(hosts[2]):
		error_handler.logger.warn('No Internet Connection. Rebooting.')
		os.system('shutdown -t 2 -r -f')
	else:
		error_handler.logger.info('Server is connected to internet. Will continue.')
		error_handler.logger.info(f'Business Automation Health Check: Completed at {datetime.now():%H:%M:%S}')


def health_check():
	error_handler.logger.info(f'Flask Server Health Check: Starting at {datetime.now():%H:%M:%S}')
	url = f'{creds.ngrok_domain}/health'
	response = requests.get(url=url)
	if response.status_code != 200:
		error_handler.logger.warn(f'Flask server is not running. Restart the server: {creds.flask_server_name}')
		sms = sms_engine.SMSEngine()
		sms.send_text(
			'none',
			to_phone=creds.network_notification_phone,
			message=f'Flask server is not running. Restart the server: {creds.flask_server_name}',
			log_location=creds.sms_utility_log,
			create_log=False,
			test_mode=False,
		)
	else:
		error_handler.logger.info('Flask server is running.')

	error_handler.logger.success(f'Flask Server Health Check: Completed at {datetime.now():%H:%M:%S}')
