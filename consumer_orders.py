import os
import sys
from time import sleep
from datetime import datetime
from email import utils

import pika
from docx.shared import Mm
from docxtpl import DocxTemplate, InlineImage

from setup import barcode_engine
from setup import creds, product_engine
from setup.order_engine import Order, utc_to_local
from integration.error_handler import Logger, ErrorHandler, ProcessInErrorHandler

from integration.orders import Order as BCOrder


class RabbitMQConsumer:
	def __init__(self, queue_name, host='localhost'):
		self.logger = ProcessInErrorHandler.logger
		self.error_handler = ProcessInErrorHandler.error_handler
		self.queue_name = queue_name
		self.host = host
		self.connection = None
		self.channel = None

	def connect(self):
		parameters = pika.ConnectionParameters(self.host)
		self.connection = pika.BlockingConnection(parameters)
		self.channel = self.connection.channel()
		self.channel.queue_declare(queue=self.queue_name, durable=True)

	def callback(self, ch, method, properties, body):
		order_id = body.decode()
		# Create order object
		self.logger.info(f'Beginning processing for Order #{order_id}')

		# Give CC Processor time to complete capture of CC info and process
		sleep(5)  # <---- This is to give payment processor time to complete

		self.logger.info('Getting Order Details for Order #{}'.format(order_id))

		try:
			order = Order(order_id)

			# Filter out DECLINED payments
			if order.status_id == 11:
				# Add order to SQL Database. Datestamp and status are added by default.
				bc_date = order.date_created
				# Format Date and Time
				dt_date = utils.parsedate_to_datetime(bc_date)
				date = utc_to_local(dt_date).strftime('%m/%d/%Y')  # ex. 04/24/2024
				time = utc_to_local(dt_date).strftime('%I:%M:%S %p')  # ex. 02:34:24 PM
				products = order.order_products
				product_list = []
				gift_card_only = True
				for x in products:
					if x['type'] == 'physical':
						gift_card_only = False
					item = product_engine.Product(x['sku'])
					product_details = {
						'sku': item.item_no,
						'name': item.descr,
						'qty': x['quantity'],
						'base_price': x['base_price'],
						'base_total': x['base_total'],
					}
					product_list.append(product_details)

				# Currently, the printing does not work correctly with bigcommerce line discounts. We will need
				# to address this issue as well.

				# FILTER OUT GIFT CARDS (NO PHYSICAL PRODUCTS)
				if not gift_card_only:
					# Create Barcode
					barcode_filename = 'barcode'
					self.logger.info('Creating barcode')
					try:
						barcode_engine.generate_barcode(data=order_id, filename=barcode_filename)
					except Exception as err:
						error_type = 'barcode'
						self.error_handler.add_error_v(
							error=f'Error ({error_type}): {err}', origin='Design - Barcode'
						)
					else:
						self.logger.success(
							f'Creating barcode - Success at {datetime.now():%H:%M:%S}'
						)

					self.logger.info('Creating Word Document')
					# Create the Word document
					try:
						doc = DocxTemplate('./templates/ticket_template.docx')
						barcode = InlineImage(
							doc, f'./{barcode_filename}.png', height=Mm(15)
						)  # width in mm
						context = {
							# Company Details
							'company_name': creds.company_name,
							'co_address': creds.company_address,
							'co_phone': creds.company_phone,
							# Order Details
							'order_number': order_id,
							'order_date': date,
							'order_time': time,
							'order_subtotal': float(order.subtotal_inc_tax),
							'order_shipping': float(order.shipping_cost_inc_tax),
							'order_total': float(order.total_inc_tax),
							# Customer Billing
							'cb_name': order.billing_first_name + ' ' + order.billing_last_name,
							'cb_phone': order.billing_phone,
							'cb_email': order.billing_email,
							'cb_street': order.billing_street_address,
							'cb_city': order.billing_city,
							'cb_state': order.billing_state,
							'cb_zip': order.billing_zip,
							# Customer Shipping
							'shipping_method': order.shipping_method,
							'cs_name': order.shipping_first_name + ' ' + order.shipping_last_name,
							'cs_phone': order.shipping_phone,
							'cs_email': order.shipping_email,
							'cs_street': order.shipping_street_address,
							'cs_city': order.shipping_city,
							'cs_state': order.shipping_state,
							'cs_zip': order.shipping_zip,
							# Product Details
							'number_of_items': order.items_total,
							'ticket_notes': order.customer_message,
							'products': product_list,
							'coupon_code': order.order_coupons['code'],
							'coupon_discount': float(order.coupon_discount),
							'loyalty': float(order.store_credit_amount),
							'gc_amount': float(order.gift_certificate_amount),
							'barcode': barcode,
						}

						doc.render(context)
						ticket_name = (
							f"ticket_{order_id}_{datetime.now().strftime("%m_%d_%y_%H_%M_%S")}.docx"
						)
						file_path = creds.ticket_location + ticket_name
						doc.save(file_path)
					except Exception as err:
						error_type = 'Word Document'
						self.error_handler.add_error_v(
							error=f'Error ({error_type}): {err}', origin='Design - Word Document'
						)
					else:
						self.logger.success(
							f'Creating Word Document - Success at {datetime.now():%H:%M:%S}'
						)
						try:
							# Print the file to default printer
							os.startfile(file_path, 'print')
						except Exception as err:
							error_type = 'Printing'
							self.error_handler.add_error_v(
								f'Error ({error_type}): {err}', origin='Design - Printing'
							)
						else:
							self.logger.success(f'Printing - Success at {datetime.now():%H:%M:%S}')

						self.logger.info('Deleting barcode files')

						# Delete barcode files
						try:
							os.remove(f'./{barcode_filename}.png')
							# os.remove(f"./{order_id}.svg")
						except Exception as err:
							error_type = 'Deleting Barcode'
							self.error_handler.add_error_v(
								error=f'Error ({error_type}): {err}',
								origin='Design - Deleting Barcode',
							)
						else:
							self.logger.success(
								f'Deleting Barcode - Success at {datetime.now():%H:%M:%S}'
							)
				# Gift Card Only
				else:
					self.logger.info(f'Skipping Order #{order_id}: Gift Card Only')
			# Declined Payments
			elif order.status_id == 4:
				self.logger.info(
					f'Skipping Order #{order_id}: Order Refunded. Status: {order.payment_status}'
				)
			elif order.status_id == 6:
				self.logger.info(
					f'Skipping Order #{order_id}: Payment Declined. Status: {order.payment_status}'
				)
			else:
				self.logger.info(
					f'Skipping Order #{order_id}: Payment Status: {order.payment_status}'
				)

			BCOrder(order_id).process()

		except Exception as err:
			error_type = 'General Catch'
			self.error_handler.add_error_v(
				error=f'Error ({error_type}): {err}', origin='General Catch'
			)
		else:
			ch.basic_ack(delivery_tag=method.delivery_tag)
		finally:
			self.logger.success(f'Processing Finished at {datetime.now():%H:%M:%S}\n')
			self.error_handler.print_errors()

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
				self.error_handler.add_error_v(
					error='Connection lost. Reconnecting...', origin='Design Consumer'
				)
				sleep(5)  # Wait before attempting reconnection
			except Exception as err:
				self.error_handler.add_error_v(error=err, origin='Design Consumer')
				sleep(5)  # Wait before attempting reconnection


if __name__ == '__main__':  #
	consumer = RabbitMQConsumer(queue_name='bc_orders')
	consumer.start_consuming()
