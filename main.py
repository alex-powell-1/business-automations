import subprocess
import threading
import signal
import sys
from consumers.rabbitmq import RabbitMQConsumer
from setup import creds, error_handler
from integration.draft_orders import on_draft_created, on_draft_updated
from consumers.sync_on_demand import sync_on_demand
from consumers.marketing_leads import process_design_lead
from consumers.orders import process_shopify_order


def run_server():
    """HTTP server to handle incoming requests, webhooks, and other API calls."""
    subprocess.run('py server.py')


def run_integration():
    """Run integration to keep E-Commerce platform in sync with physical store.
    Includes syncing inventory, collections, customers, promotions, and other data."""
    subprocess.run('py integrator.py')


def run_inventory_sync():
    """Keep E-Commerce platform in sync with physical store's inventory quantities throughout day."""
    subprocess.run('py inventory_sync.py')


def run_scheduled_tasks():
    """Run scheduled tasks."""
    subprocess.run('py scheduled_tasks.py')


def consumer_draft_created():
    """Process a new draft order created in Shopify."""
    return RabbitMQConsumer(
        queue_name=creds.consumer_shopify_draft_create,
        callback_func=on_draft_created,
        eh=error_handler.ProcessInErrorHandler,
    )


def consumer_draft_updated():
    """Process an updated draft order in Shopify."""
    return RabbitMQConsumer(
        queue_name=creds.consumer_shopify_draft_update,
        callback_func=on_draft_updated,
        eh=error_handler.ProcessInErrorHandler,
    )


def consumer_sync_on_demand():
    """Consumer for SMS Based Trigger for Catalog Sync."""
    return RabbitMQConsumer(
        queue_name=creds.consumer_sync_on_demand,
        callback_func=sync_on_demand,
        eh=error_handler.ProcessInErrorHandler,
    )


def consumer_marketing_lead():
    """Consumer for processing Marketing Leads."""
    return RabbitMQConsumer(
        queue_name=creds.consumer_design_lead_form,
        callback_func=process_design_lead,
        eh=error_handler.ProcessInErrorHandler,
    )


def consumer_shopify_orders():
    """Consumer for Shopify Orders."""
    return RabbitMQConsumer(
        queue_name=creds.consumer_shopify_orders,
        callback_func=process_shopify_order,
        eh=error_handler.ProcessInErrorHandler,
    )


def shutdown_handler(signum, frame):
    print('Received shutdown signal, stopping consumers...')
    for consumer in consumers:
        consumer.connection.close()
    for thread in threads:
        thread.join()
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    threads = [
        threading.Thread(target=run_server),
        threading.Thread(target=run_integration),
        threading.Thread(target=run_inventory_sync),
        threading.Thread(target=run_scheduled_tasks),
    ]

    consumers = [
        consumer_draft_created(),
        consumer_draft_updated(),
        consumer_sync_on_demand(),
        consumer_marketing_lead(),
        consumer_shopify_orders(),
    ]

    for consumer in consumers:
        threads.append(threading.Thread(target=consumer.start_consuming))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
