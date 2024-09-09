import time
from setup.error_handler import ProcessInErrorHandler
from integration.orders import Order as ShopifyOrder
from integration.shopify_api import Shopify
from setup.print_engine import Printer


def process_shopify_order(order_id, eh=ProcessInErrorHandler):
    eh.logger.info(f'Beginning processing for Order #{order_id}')
    time.sleep(5)  # <-- This is to give payment processor time to complete
    order = Shopify.Order.as_bc_order(order_id=order_id)  # Convert order to BC Order dictionary
    shopify_order = ShopifyOrder(order_id)
    shopify_order.post_shopify_order()
    eh.logger.info(f'Order {order_id} processed successfully')

    # PRINTING - Filter out DECLINED payments
    if order['status'] == 'UNFULFILLED' or order['status'] == 'FULFILLED':
        Printer.Order.print(order_id)

    elif order['status'] == 'Partially Refunded':
        eh.error_handler.add_error_v(
            error=f'Order {order_id} was partially refunded. Skipping...', origin='Design Consumer'
        )
    elif order['status'] == 'ON_HOLD':
        eh.logger.info(message=f'Order {order_id} is on hold. Skipping...for now...')
    else:
        eh.logger.info(message=f'Order {order_id} status is {order['status']}. Skipping...')
