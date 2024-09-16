class CustomerWebhook:
    def __init__(self, data):
        self.email = data['email']
        self.id = data['id']
        self.first_name = data['first_name']
        self.last_name = data['last_name']
        self.phone = data['phone']
        self.addresses = data['addresses']
        if 'state' in data['email_marketing_consent']:
            self.email_consent = True if data['email_marketing_consent']['state'] == 'subscribed' else False
            self.email_consent_updated_at = data['email_marketing_consent']['consent_updated_at']
        else:
            self.email_consent = False
            self.email_consent_updated_at = None

        if self.phone and 'state' in data['sms_marketing_consent']:
            self.sms_consent = True if data['sms_marketing_consent']['state'] == 'subscribed' else False
            self.sms_consent_updated_at = data['sms_marketing_consent']['consent_updated_at']
        else:
            self.sms_consent = False
            self.sms_consent_updated_at = None
        self.created_at = data['created_at']
        self.updated_at = data['updated_at']
        self.last_order_id = data['last_order_id']
        self.note = data['note']

    def __str__(self) -> str:
        result = 'Customer Webhook\n'
        result += '----------------\n'
        result += f'Customer ID: {self.id}\n'
        result += f'Email: {self.email}\n'
        result += f'First Name: {self.first_name}\n'
        result += f'Last Name: {self.last_name}\n'
        result += f'Phone: {self.phone}\n'
        result += f'Email Consent: {self.email_consent}\n'
        result += f'Email Consent Updated At: {self.email_consent_updated_at}\n'
        result += f'SMS Consent: {self.sms_consent}\n'
        result += f'SMS Consent Updated At: {self.sms_consent_updated_at}\n'
        result += f'Created At: {self.created_at}\n'
        result += f'Updated At: {self.updated_at}\n'
        result += f'Last Order ID: {self.last_order_id}\n'
        result += f'Note: {self.note}\n'
        return result


class OrderWebhook:
    def __init__(self, data):
        self.id = data['id']
        self.email = data['email']
        self.created_at = data['created_at']
        self.updated_at = data['updated_at']
        self.customer_id = data['customer_id']
        self.line_items = data['line_items']
        self.total_price = data['total_price']
        self.total_discounts = data['total_discounts']
        self.total_tax = data['total_tax']
        self.total_shipping = data['total_shipping']
        self.total_price_usd = data['total_price_usd']
        self.total_discounts_usd = data['total_discounts_usd']
        self.total_tax_usd = data['total_tax_usd']
        self.total_shipping_usd = data['total_shipping_usd']
        self.subtotal_price = data['subtotal_price']
        self.subtotal_price_usd = data['subtotal_price_usd']
        self.total_tip_received = data['total_tip_received']
        self.total_tip_received_usd = data['total_tip_received_usd']
        self.currency = data['currency']
        self.financial_status = data['financial_status']
        self.fulfillment_status = data['fulfillment_status']
        self.order_number = data['order_number']

    def __str__(self) -> str:
        result = 'Order Webhook\n'
        result += '----------------\n'
        result += f'Order ID: {self.id}\n'
        result += f'Email: {self.email}\n'
        result += f'Created At: {self.created_at}\n'
        result += f'Updated At: {self.updated_at}\n'
        result += f'Customer ID: {self.customer_id}\n'
        result += f'Line Items: {self.line_items}\n'
        result += f'Total Price: {self.total_price}\n'
        result += f'Total Discounts: {self.total_discounts}\n'
        result += f'Total Tax: {self.total_tax}\n'
        result += f'Total Shipping: {self.total_shipping}\n'
        result += f'Total Price USD: {self.total_price_usd}\n'
        result += f'Total Discounts USD: {self.total_discounts_usd}\n'
        result += f'Total Tax USD: {self.total_tax_usd}\n'
        result += f'Total Shipping USD: {self.total_shipping_usd}\n'
        result += f'Subtotal Price: {self.subtotal_price}\n'
        result += f'Subtotal Price USD: {self.subtotal_price_usd}\n'
        result += f'Total Tip Received: {self.total_tip_received}\n'
        result += f'Total Tip Received USD: {self.total_tip_received_usd}\n'
        result += f'Currency: {self.currency}\n'
        result += f'Financial Status: {self.financial_status}\n'
        result += f'Fulfillment Status: {self.fulfillment_status}\n'
        result += f'Order Number: {self.order_number}\n'
        return result


class ProductWebhook:
    def __init__(self, data):
        self.id = data['id']
        self.title = data['title']
        self.vendor = data['vendor']
        self.product_type = data['product_type']
        self.created_at = data['created_at']
        self.updated_at = data['updated_at']
        self.tags = data['tags']
        self.variants = data['variants']
        self.options = data['options']
        self.images = data['images']

    def __str__(self) -> str:
        result = 'Product Webhook\n'
        result += '----------------\n'
        result += f'Product ID: {self.id}\n'
        result += f'Title: {self.title}\n'
        result += f'Vendor: {self.vendor}\n'
        result += f'Product Type: {self.product_type}\n'
        result += f'Created At: {self.created_at}\n'
        result += f'Updated At: {self.updated_at}\n'
        result += f'Tags: {self.tags}\n'
        result += f'Veariants: {self.variants}\n'
        result += f'Options: {self.options}\n'
        result += f'Images: {self.images}\n'
        return result
