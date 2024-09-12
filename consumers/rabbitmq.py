from datetime import datetime
from traceback import format_exc as tb

import pika.exceptions
from setup.error_handler import ProcessInErrorHandler
import pika
import sys
import time
import threading


class RabbitMQConsumer:
    def __init__(self, queue_name, callback_func, host='localhost', eh=ProcessInErrorHandler):
        self.logger = eh.logger
        self.error_handler = eh.error_handler
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None
        self._stop_event = threading.Event()  # Add stop event
        self.callback_func = callback_func

    def connect(self):
        parameters = pika.ConnectionParameters(self.host)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def callback(self, ch, method, properties, body):
        body = body.decode()
        self.logger.info(f'{self.queue_name}: Received: {body}')
        try:
            self.callback_func(body)
        except Exception as err:
            error_type = 'Exception:'
            self.error_handler.add_error_v(
                error=f'Error ({error_type}): {err}', origin=self.queue_name, traceback=tb()
            )
        else:
            self.logger.success(f'Processing Finished at {datetime.now():%H:%M:%S}\n')
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.error_handler.print_errors()

    def start_consuming(self):
        while not self._stop_event.is_set():  # Check stop event
            try:
                self.connect()
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
                print(f'Consumer {self.queue_name}: Waiting for messages. To exit press CTRL+C')
                self.channel.start_consuming()
            except KeyboardInterrupt:
                sys.exit(0)
            # Don't recover if connection was closed by broker
            except pika.exceptions.ConnectionClosedByBroker:
                break
            # Don't recover on channel errors
            except pika.exceptions.AMQPChannelError:
                break
            # Don't recover on stream errors
            except pika.exceptions.StreamLostError:
                break
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                continue  # Connection error, retry connection
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin=self.queue_name, traceback=tb())
                time.sleep(5)  # Wait before attempting reconnection

    def stop_consuming(self):
        self._stop_event.set()
        if self.channel:
            self.channel.stop_consuming()
        if self.connection:
            self.connection.close()


if __name__ == '__main__':
    pass
