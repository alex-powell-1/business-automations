import sys
from time import sleep
import pika

from setup.error_handler import ProcessInErrorHandler
from integration.shopify_api import Shopify
from integration.orders import Order as ShopifyOrder
from setup.print_engine import Printer
from traceback import format_exc as tb
from datetime import datetime


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
        try:
            # Convert order to BC Order dictionary
            order = Shopify.Order.as_bc_order(order_id=order_id)
            shopify_order = ShopifyOrder(order_id)
            shopify_order.post_shopify_order()

            ProcessInErrorHandler.logger.info(f'Order {order_id} processed successfully')

            # Filter out DECLINED payments
            if order['status'] == 'UNFULFILLED' or order['status'] == 'FULFILLED':
                Printer.Order.print(order_id)

            elif order['status'] == 'Partially Refunded':
                ProcessInErrorHandler.error_handler.add_error_v(
                    error=f'Order {order_id} was partially refunded. Skipping...', origin='Design Consumer'
                )
            elif order['status'] == 'ON_HOLD':
                ProcessInErrorHandler.logger.info(message=f'Order {order_id} is on hold. Skipping...for now...')
            else:
                ProcessInErrorHandler.logger.info(
                    message=f'Order {order_id} status is {order['status']}. Skipping...'
                )

        except Exception as err:
            error_type = 'General Catch'
            self.error_handler.add_error_v(
                error=f'Error ({error_type}): {err}', origin='General Catch', traceback=tb()
            )
        else:
            self.logger.success(f'Processing Finished at {datetime.now():%H:%M:%S}\n')
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
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
                    error='Connection lost. Reconnecting...', origin='Design Consumer', traceback=tb()
                )
                sleep(5)  # Wait before attempting reconnection
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Design Consumer', traceback=tb())
                sleep(5)  # Wait before attempting reconnection


if __name__ == '__main__':  #
    consumer = RabbitMQConsumer(queue_name='shopify_orders')
    consumer.start_consuming()
