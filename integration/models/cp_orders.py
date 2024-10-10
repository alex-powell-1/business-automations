from integration.models import shopify_orders


class CPLineItem:
    """Counterpoint Line Item"""

    def __init__(self, item: shopify_orders.LineItem):
        self.is_refund: bool = item.is_refunded
        self.multiplier = -1 if item.is_refunded else 1
        self.type: str = 'O'
        self.sku: str = item.sku
        self.user_entered_price: bool = False
        self.quantity: float = float(item.quantity_refunded) if item.is_refunded else float(item.quantity)
        self.extended_price: float = (item.base_price * self.quantity - item.total_discount) * self.multiplier
        self.price: float = self.extended_price / self.quantity
        self.extended_cost: float = item.ext_cost * self.quantity * self.multiplier
        self.discount_amount: float = item.total_discount
        self.payload = self.get_payload()

    def get_payload(self):
        """Return the payload for the line item to be used in the CP API"""

        return {
            'LIN_TYP': self.type,
            'ITEM_NO': self.sku,
            'USR_ENTD_PRC': self.user_entered_price,
            'QTY_SOLD': self.quantity,
            'PRC': self.price,
            'EXT_PRC': self.extended_price,
            'EXT_COST': self.extended_cost,
            'DSC_AMT': self.discount_amount,
            'sku': self.sku,
        }


class CPGiftCard:
    def __init__(self, product: shopify_orders.LineItem, line_item_length: int, sequence: int):
        self.pay_code: str = 'GC'
        self.number: str = product.gift_certificate_id
        self.amount: float = float(product.base_price)
        self.line_seq_no: int = line_item_length
        self.description: str = 'Gift Certificate'
        self.create_as_store_credit: str = 'N'
        self.gfc_seq_no: int = sequence
        self.payload = self.get_payload()

    def get_payload(self):
        return {
            'GFC_COD': self.pay_code,
            'GFC_NO': self.number,
            'AMT': self.amount,
            'LIN_SEQ_NO': self.line_seq_no,
            'DESCR': self.description,
            'CREATE_AS_STC': self.create_as_store_credit,
            'GFC_SEQ_NO': self.gfc_seq_no,
        }


class CPNote:
    """Counterpoint Note"""

    def __init__(self, order: shopify_orders.ShopifyOrder):
        self.note_id: str = 'Customer Message'
        self.text: str = order.customer_message
        self.payload = self.get_payload()

    def get_payload(self):
        """Return the payload for the note to be used in the CP API"""
        return {'NOTE_ID': self.note_id, 'NOTE': self.text}
