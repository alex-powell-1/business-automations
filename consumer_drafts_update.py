import sys
from time import sleep
import pika
from setup import creds

from setup.error_handler import ProcessInErrorHandler
from traceback import format_exc as tb
from datetime import datetime

from integration.draft_orders import on_draft_updated

origin = 'Consumer-Draft Update'


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
        self.logger.info(f'Beginning processing for Order #{order_id}')

        try:
            on_draft_updated(order_id)
        except Exception as err:
            error_type = 'General Catch'
            self.error_handler.add_error_v(
                error=f'Error ({error_type}): {err}', origin=f'{origin}-General Catch', traceback=tb()
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
                    error='Connection lost. Reconnecting...', origin=origin, traceback=tb()
                )
                sleep(5)  # Wait before attempting reconnection
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin=origin, traceback=tb())
                sleep(5)  # Wait before attempting reconnection


if __name__ == '__main__':  #
    consumer = RabbitMQConsumer(queue_name=creds.consumer_shopify_draft_update)
    consumer.start_consuming()
