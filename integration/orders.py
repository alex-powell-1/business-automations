from integration.cp_api import OrderAPI

from setup.error_handler import ProcessInErrorHandler
from integration.shopify_api import Shopify
import traceback


class Order:
    def __init__(self, order_id: int, send_gft=False, verbose: bool = False):
        self.order_id = order_id
        self.oapi: OrderAPI = OrderAPI(self.order_id, verbose=verbose)
        self.order = self.oapi.order
        self.send_gft = send_gft
        self.cust_no = self.oapi.order.customer.cp_id
        self.payload = self.order.payload

    def __str__(self) -> str:
        return self.order

    def print_payload(self):
        print(self.payload)

    def process(self):
        try:
            OrderAPI.process_order(self.order_id)
            if self.send_gft:
                self.order.send_gift_cards()
        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error processing order {self.order_id}',
                origin='integration.orders',
                traceback=traceback.format_exc(),
            )

            ProcessInErrorHandler.error_handler.add_error_v(error=str(e), origin='integration.orders')


class OrderProcessor:
    def __init__(self, order_ids: list[str | int] = []):
        self.order_ids = order_ids

    def process(self):
        orders = [Order(order_id) for order_id in self.order_ids]
        for order in orders:
            order.process()


if __name__ == '__main__':
    print(Shopify.Order().as_bc_order(5671246889127))
