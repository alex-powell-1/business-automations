from integration.cp_api import OrderAPI


class Order:
    def __init__(self, order_id: str | int):
        self.order_id = order_id
        self.oapi = OrderAPI()

    def print_order(self):
        print(self.get_bc_order())

    def print_payload(self):
        print(self.get_payload())

    def get_bc_order(self):
        return OrderAPI.get_order(self.order_id)

    def get_cust_no(self):
        return OrderAPI.get_cust_no(self.bc_order)

    def get_payload(self):
        return self.oapi.get_post_order_payload(self.bc_order, self.cust_no)

    def post_order(self, cust_no_override: str = None):
        OrderAPI.post_order(self.order_id, cust_no_override=cust_no_override)
