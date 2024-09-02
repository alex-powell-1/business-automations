import sys
import time
from subprocess import Popen

import pika

from setup import creds
from setup.error_handler import ProcessOutErrorHandler
from datetime import datetime
from setup.sms_engine import SMSEngine
from setup.utilities import PhoneNumber

test_mode = False


class RabbitMQConsumer:
    def __init__(self, queue_name, host='localhost'):
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
        phone_number = PhoneNumber(body.decode()).to_twilio()
        SMSEngine.send_text(
            origin='SERVER',
            campaign='SYNC_ON_DEMAND',
            to_phone=phone_number,
            message='Syncing data. Please wait...',
        )
        phone_response = None

        try:
            file = creds.sync_batch_file
            path = creds.batch_file_path
            p = Popen(args=file, cwd=path, shell=True)
            stdout, stderr = p.communicate()

        except Exception as e:
            ProcessOutErrorHandler.error_handler.add_error_v(error=f'Error: {e}', origin='sync_on_demand')
            phone_response = 'Sync failed. Please check logs.'

        else:
            phone_response = f'Sync completed successfully at {datetime.now():%m/%d/%Y %H:%M:%S}'
        finally:
            SMSEngine.send_text(origin='sync_on_demand', to_phone=phone_number, message=phone_response)
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
                ProcessOutErrorHandler.error_handler.add_error_v(
                    error='Connection lost. Reconnecting...', origin='sync_on_demand'
                )
                time.sleep(5)  # Wait before attempting reconnection
            except Exception as err:
                ProcessOutErrorHandler.error_handler.add_error_v(
                    error=f'Error (General Catch): {err}', origin='sync_on_demand'
                )
                time.sleep(5)  # Wait before attempting reconnection


if __name__ == '__main__':
    consumer = RabbitMQConsumer(queue_name=creds.consumer_sync_on_demand)
    consumer.start_consuming()
