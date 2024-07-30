from integration.cp_api import OrderAPI

from setup.error_handler import ProcessInErrorHandler
from integration.shopify_api import Shopify


class Order:
    def __init__(self, order_id: str | int):
        self.order_id = order_id
        self.oapi = OrderAPI()
        self.bc_order = None
        self.cust_no = None
        self.payload = None
        self.shopify_order = None

    def print_order(self):
        print(self.get_bc_order())

    def print_payload(self):
        print(self.get_payload())

    def get_bc_order(self):
        if self.bc_order is None:
            self.bc_order = OrderAPI.get_order(self.order_id)
        return self.bc_order

    def get_shopify_order(self):
        if self.shopify_order is None:
            self.shopify_order = Shopify.Order.as_bc_order(self.order_id)
        return self.shopify_order

    def get_cust_no(self):
        if self.cust_no is None:
            self.cust_no = OrderAPI.get_cust_no(self.get_bc_order())
        return self.cust_no

    def get_shopify_cust_no(self):
        if self.cust_no is None:
            self.cust_no = OrderAPI.get_cust_no(self.get_shopify_order())

    def get_payload(self):
        if self.payload is None:
            self.payload = self.oapi.get_post_order_payload(
                bc_order=self.get_bc_order(), cust_no=self.get_cust_no()
            )
        return self.payload

    def post_order(self, cust_no_override: str = None):
        try:
            OrderAPI.post_order(self.order_id, cust_no_override=cust_no_override)
        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error processing order {self.order_id}', origin='integration.orders'
            )

            ProcessInErrorHandler.error_handler.add_error_v(error=str(e), origin='integration.orders')

    def process(self, session=None):
        self.post_order()


class OrderProcessor:
    def __init__(self, order_ids: list[str | int] = []):
        self.order_ids = order_ids

    def process(self):
        orders = [Order(order_id) for order_id in self.order_ids]
        for order in orders:
            order.process()


if __name__ == '__main__':
    print(Order(138).get_bc_order())
