import json
from datetime import timezone

import requests


class Order:
    def __init__(self, order_id):
        self.order_id = order_id
        self.customer_id = ""
        self.date_created = ""
        self.date_modified = ""
        self.date_shipped = ""
        self.status_id = ""
        self.status = ""
        self.subtotal_ex_tax = ""
        self.subtotal_inc_tax = ""
        self.subtotal_tax = ""
        self.base_shipping_cost = ""
        self.shipping_cost_ex_tax = ""
        self.shipping_cost_inc_tax = "0.0000"
        self.shipping_cost_tax = ""
        self.shipping_cost_tax_class_id = 2
        self.base_handling_cost = ""
        self.handling_cost_ex_tax = ""
        self.handling_cost_inc_tax = ""
        self.handling_cost_tax = ""
        self.handling_cost_tax_class_id = 2
        self.base_wrapping_cost = ""
        self.wrapping_cost_ex_tax = ""
        self.wrapping_cost_inc_tax = ""
        self.wrapping_cost_tax = ""
        self.wrapping_cost_tax_class_i = 3
        self.total_ex_tax = ""
        self.total_inc_tax = ""
        self.total_tax = ""
        self.items_total = 1
        self.items_shipped = 0
        self.payment_method = ""
        self.payment_provider_id = ""
        self.payment_status = ""
        self.refunded_amount = ""
        self.order_is_digital = ""
        self.store_credit_amount = ""
        self.gift_certificate_amount = ""
        self.ip_address = ""
        self.ip_address_v6 = ""
        self.geoip_country = ""
        self.geoip_country_iso2 = ""
        self.currency_id = 1
        self.currency_code = ""
        self.currency_exchange_rate = ""
        self.default_currency_id = 1
        self.default_currency_code = ""
        self.staff_notes = ""
        self.customer_message = ""
        self.discount_amount = ""
        self.coupon_discount = ""
        self.shipping_address_count = 1
        self.is_deleted = ""
        self.ebay_order_id = "0"
        self.cart_id = ""
        self.billing_first_name = ""
        self.billing_last_name = ""
        self.billing_company = ""
        self.billing_street_address = ""
        # self.billing_street_1 = ""
        # self.billing_street_2 = ""
        self.billing_city = ""
        self.billing_state = ""
        self.billing_zip = ""
        self.billing_country = ""
        self.billing_country_iso2 = ""
        self.billing_phone = ""
        self.billing_email = ""
        self.form_fields = []
        self.is_email_opt_in = ""
        self.credit_card_type = ""
        self.order_source = ""
        self.channel_id = 1
        self.external_source = ""
        self.order_products = []
        self.order_coupons = {}
        self.shipping_address = {}
        self.shipping_first_name = ""
        self.shipping_last_name = ""
        self.shipping_street_address = ""
        # self.shipping_street_1 = ""
        # self.shipping_street_2 = ""
        self.shipping_city = ""
        self.shipping_state = ""
        self.shipping_zip = ""
        self.shipping_email = ""
        self.shipping_phone = ""
        self.shipping_method = ""
        self.get_order_details()

    def get_order_details(self):
        from setup import creds
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders/{self.order_id}"
        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = (requests.get(url, headers=headers))
        if response.status_code == 200:
            data = response.json()
            pretty = response.content
            pretty = json.loads(pretty)
            pretty = json.dumps(pretty, indent=4)
            print("----")
            print("Order")
            print(pretty)
            print("----")
            self.customer_id = data['customer_id']
            self.date_created = data['date_created']
            self.date_modified = data['date_modified']
            self.date_shipped = data['date_shipped']
            self.status_id = data['status_id']
            self.status = data['status']
            self.subtotal_ex_tax = data['subtotal_ex_tax']
            self.subtotal_inc_tax = data['subtotal_inc_tax']
            self.subtotal_tax = data['subtotal_tax']
            self.base_shipping_cost = data['base_shipping_cost']
            self.shipping_cost_ex_tax = data['shipping_cost_ex_tax']
            self.shipping_cost_inc_tax = data['shipping_cost_inc_tax']
            self.shipping_cost_tax = data['shipping_cost_tax']
            self.shipping_cost_tax_class_id = data['shipping_cost_tax_class_id']
            self.base_handling_cost = data['base_handling_cost']
            self.handling_cost_ex_tax = data['handling_cost_ex_tax']
            self.handling_cost_inc_tax = data['handling_cost_inc_tax']
            self.handling_cost_tax = data['handling_cost_tax']
            self.handling_cost_tax_class_id = data['handling_cost_tax_class_id']
            self.base_wrapping_cost = data['base_wrapping_cost']
            self.wrapping_cost_ex_tax = data['wrapping_cost_ex_tax']
            self.wrapping_cost_inc_tax = data['wrapping_cost_inc_tax']
            self.wrapping_cost_tax = data['wrapping_cost_tax']
            self.wrapping_cost_tax_class_i = data['wrapping_cost_tax_class_id']
            self.total_ex_tax = data['total_ex_tax']
            self.total_inc_tax = data['total_inc_tax']
            self.total_tax = data['total_tax']
            self.items_total = data['items_total']
            self.items_shipped = data['items_shipped']
            self.payment_method = data['customer_id']
            self.payment_provider_id = data['payment_provider_id']
            self.payment_status = data['payment_status']
            self.refunded_amount = data['refunded_amount']
            self.order_is_digital = data['order_is_digital']
            self.store_credit_amount = data['store_credit_amount']
            self.gift_certificate_amount = data['gift_certificate_amount']
            self.ip_address = data['ip_address']
            self.ip_address_v6 = data['ip_address_v6']
            self.geoip_country = data['geoip_country']
            self.geoip_country_iso2 = data['geoip_country_iso2']
            self.currency_id = data['currency_id']
            self.currency_code = data['currency_code']
            self.currency_exchange_rate = data['currency_exchange_rate']
            self.default_currency_id = data['default_currency_id']
            self.default_currency_code = data['default_currency_code']
            self.staff_notes = data['staff_notes']
            self.customer_message = data['customer_message']
            self.discount_amount = data['discount_amount']
            self.coupon_discount = data['coupon_discount']
            self.shipping_address_count = data['shipping_address_count']
            self.is_deleted = data['is_deleted']
            self.ebay_order_id = data['ebay_order_id']
            self.cart_id = data['cart_id']
            self.billing_first_name = data['billing_address']['first_name']
            self.billing_last_name = data['billing_address']['last_name']
            self.billing_company = data['billing_address']['company']
            if data['billing_address']['street_2'] == '':
                self.billing_street_address = data['billing_address']['street_1']
            else:
                self.billing_street_address = (data['billing_address']['street_1'] + "\n" +
                                               data['billing_address']['street_2'])
            # self.billing_street_1 = data['billing_address']['street_1']
            # self.billing_street_2 = data['billing_address']['street_2']
            self.billing_city = data['billing_address']['city']
            self.billing_state = data['billing_address']['state']
            self.billing_zip = data['billing_address']['zip']
            self.billing_country = data['billing_address']['country']
            self.billing_country_iso2 = data['billing_address']['country_iso2']
            self.billing_phone = format_phone(data['billing_address']['phone'], mode='clickable')
            self.billing_email = data['billing_address']['email']
            self.form_fields = data['billing_address']['form_fields']
            self.is_email_opt_in = data['is_email_opt_in']
            self.credit_card_type = data['credit_card_type']
            self.order_source = data['order_source']
            self.channel_id = data['channel_id']
            self.external_source = data['external_source']

            # Get Products
            url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders/{self.order_id}/products"
            headers = {
                'X-Auth-Token': creds.big_access_token,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            response = (requests.get(url, headers=headers))
            if response.status_code == 200:
                data = response.json()
                pretty = response.content
                pretty = json.loads(pretty)
                pretty = json.dumps(pretty, indent=4)
                print("----")
                print("Products")
                print(pretty)
                print("----")
                for x in data:
                    self.order_products.append(x)

            # Get Coupons
            url = f"https://api.bigcommerce.com/stores/wmonsw2bbs/v2/orders/{self.order_id}/coupons"
            response = (requests.get(url, headers=headers))
            if response.status_code == 200:
                data = response.json()
                self.order_coupons = data[0]
            elif response.status_code == 204:
                self.order_coupons = {
                    "code": None
                }

            # Get Shipping Addresses
            url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders/{self.order_id}/shipping_addresses"
            response = (requests.get(url, headers=headers))
            if response.status_code == 200:
                data = response.json()
                self.shipping_first_name = data[0]['first_name']
                self.shipping_last_name = data[0]['last_name']
                if data[0]['street_2'] == '':
                    self.shipping_street_address = data[0]['street_1']
                else:
                    self.shipping_street_address = (data[0]['street_1'] + "\n" +
                                                    data[0]['street_2'])
                # self.shipping_street_1 = data[0]['street_1']
                # self.shipping_street_2 = data[0]['street_2']
                self.shipping_city = data[0]['city']
                self.shipping_state = data[0]['state']
                self.shipping_zip = data[0]['zip']
                self.shipping_email = data[0]['email']
                self.shipping_phone = format_phone(data[0]['phone'], mode='clickable')
                self.shipping_method = data[0]['shipping_method']

    def refund_order(self):
        from setup import creds
        url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/'
               f'v3/orders/{self.order_id}/payment_actions/refund_quotes')
        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        refund_products = []
        shipping_id = ""
        for x in self.order_products:
            refund_products.append({
                'item_type': 'PRODUCT',
                'item_id': x['id'],
                'amount': x['price_inc_tax'],
                'quantity': x['quantity'],
                'reason': "APP REFUND"
            })
            shipping_id = x['order_address_id']
        refund_products.append({
            'item_type': 'SHIPPING',
            'item_id': shipping_id,
            'amount': self.shipping_cost_inc_tax
        })
        payload = {
            'items': refund_products
        }
        response = requests.post(url, headers=headers, json=payload)
        # data = response.json()
        data = response.json()
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        print(pretty)
        # To be continued


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def format_phone(phone_number, mode="Twilio", prefix=False):
    """Cleanses input data and returns masked phone for either Twilio or Counterpoint configuration"""
    phone_number_as_string = str(phone_number)
    # Strip away extra symbols
    formatted_phone = phone_number_as_string.replace(" ", "")  # Remove Spaces
    formatted_phone = formatted_phone.replace("-", "")  # Remove Hyphens
    formatted_phone = formatted_phone.replace("(", "")  # Remove Open Parenthesis
    formatted_phone = formatted_phone.replace(")", "")  # Remove Close Parenthesis
    formatted_phone = formatted_phone.replace("+1", "")  # Remove +1
    formatted_phone = formatted_phone[-10:]  # Get last 10 characters
    if mode == "counterpoint":
        # Masking ###-###-####
        cp_phone = formatted_phone[0:3] + "-" + formatted_phone[3:6] + "-" + formatted_phone[6:10]
        return cp_phone

    elif mode == "clickable":
        # Masking ###-###-####
        clickable_phone = "(" + formatted_phone[0:3] + ") " + formatted_phone[3:6] + "-" + formatted_phone[6:10]
        return clickable_phone

    else:
        if prefix:
            formatted_phone = "+1" + formatted_phone
        return formatted_phone
