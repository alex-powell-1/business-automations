import rabbitmq
from setup import creds
from setup import error_handler
from integration.draft_orders import on_draft_updated


consumer = rabbitmq.RabbitMQConsumer(
    queue_name=creds.Consumer.draft_update, callback_func=on_draft_updated, eh=error_handler.ProcessInErrorHandler
)
consumer.start_consuming()
