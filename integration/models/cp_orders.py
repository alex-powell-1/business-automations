from integration.models import shopify_orders


class CPNote:
    """Counterpoint Note"""

    def __init__(self, order: shopify_orders.ShopifyOrder):
        self.note_id: str = 'Customer Message'
        self.text: str = order.customer_message
        self.payload = self.get_payload()

    def get_payload(self):
        """Return the payload for the note to be used in the CP API"""
        return {'NOTE_ID': self.note_id, 'NOTE': self.text}
