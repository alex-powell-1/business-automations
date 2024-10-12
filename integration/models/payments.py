from integration.models.shopify_orders import ShopifyOrder


class GCPayment:
    """Gift Card Payment"""

    def __init__(self, order: ShopifyOrder):
        self.order: ShopifyOrder = order
        self.AMT: float = self.get_amount(order)
        self.PAY_COD: str = 'GC'
        self.FINAL_PMT: str = 'N'
        self.CARD_NO: str = self.get_card_no(order)
        self.PMT_LIN_TYP: str = 'C' if order.is_refund else 'T'
        self.REMAINING_BAL: float = self.get_remaining_balance(order)

    def get_amount(self) -> float:
        for pay_method in self.order.transactions['data']:
            if pay_method['method'] == 'gift_certificate':
                return pay_method['amount']

    def get_card_no(self) -> str:
        for pay_method in self.order.transactions['data']:
            if pay_method['method'] == 'gift_certificate':
                return pay_method['card_no']

    def get_remaining_balance(self) -> float:
        for pay_method in self.order.transactions['data']:
            if pay_method['method'] == 'gift_certificate':
                return float(pay_method['gift_certificate']['remaining_balance'])
