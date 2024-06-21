from integration.cp_api import OrderAPI
from integration.object_processor import ObjectProcessor

from integration.error_handler import GlobalErrorHandler


class Order:
    def __init__(self, order_id: str | int):
        self.order_id = order_id
        self.oapi = OrderAPI()
        self.bc_order = None
        self.cust_no = None
        self.payload = None

    def print_order(self):
        print(self.get_bc_order())

    def print_payload(self):
        print(self.get_payload())

    def get_bc_order(self):
        if self.bc_order is None:
            self.bc_order = OrderAPI.get_order(self.order_id)
        return self.bc_order

    def get_cust_no(self):
        if self.cust_no is None:
            self.cust_no = OrderAPI.get_cust_no(self.get_bc_order())
        return self.cust_no

    def get_payload(self):
        if self.payload is None:
            self.payload = self.oapi.get_post_order_payload(
                self.get_bc_order(), self.get_cust_no()
            )
        return self.payload

    def post_order(self, cust_no_override: str = None):
        try:
            OrderAPI.post_order(self.order_id, cust_no_override=cust_no_override)
        except:
            GlobalErrorHandler.error_handler.add_error_v(
                error=f"Error processing order {self.order_id}",
                origin="integration.orders",
            )

    def process(self, session=None):
        self.post_order()


class OrderProcessor:
    def __init__(self, order_ids: list[str | int] = []):
        self.order_ids = order_ids

    def process(self):
        orders = [Order(order_id) for order_id in self.order_ids]
        ObjectProcessor(orders).process()


if __name__ == "__main__":
    orders = [1150, 1151, 1152]

    for order in orders:
        Order(order).process()
