from database import Database
from setup.error_handler import ProcessInErrorHandler
from setup.utilities import PhoneNumber
from datetime import datetime, timezone
from setup.email_engine import Email


class ShopifyOrder:
    def __init__(self, node: dict, send_gfc=False):
        self.node = node
        self.logger = ProcessInErrorHandler.logger
        self.error_handler = ProcessInErrorHandler.error_handler
        self.id: str = self.node['name']
        self.channel: str = self.get_channel()
        self.email: str = self.node['email'] or ''
        self.date_created: str = ShopifyOrder.convert_date(self.node['createdAt'])
        self.date_modified: str = ShopifyOrder.convert_date(self.node['updatedAt'])
        self.billing_address: BillingAddress = BillingAddress(self.node)
        self.has_shipping_address: bool = self.node['shippingAddress'] is not None
        if self.has_shipping_address:
            self.shipping_address: ShippingAddress = ShippingAddress(self.node)
        else:
            self.shipping_address = None
        self.customer = Customer(self.node)
        self.payment_status: str = self.node['displayFinancialStatus']
        self.is_declined: bool = self.payment_status.lower() in ['declined', '']
        self.refunds: list[dict] = self.node['refunds']
        self.status: str = self.get_status()
        self.is_refund: bool = self.status in ['Refunded', 'Partially Refunded']
        self.delivery_from_lines: float = 0
        self.refunded_subtotal: float = 0
        self.items: list[LineItem] = self.get_items()
        self.shipping_cost: float = self.get_shipping_cost()
        self.is_shipping: bool = self.shipping_cost > 0
        self.base_shipping_cost: float = self.shipping_cost
        self.get_shipping_item()
        self.header_discount: float = ShopifyOrder.get_money(node['totalDiscountsSet'])
        self.coupon_codes: list[str] = self.node['discountCodes']
        self.coupon_amount: float = self.header_discount
        self.subtotal: float = self.get_subtotal()
        self.subtotal_ex_tax: float = self.subtotal
        self.subtotal_inc_tax: float = self.subtotal
        self.total: float = self.get_total()
        self.total_ex_tax: float = self.total
        self.total_inc_tax: float = self.total
        self.refund_total: float = ShopifyOrder.get_money(node['totalRefundedSet'])
        self.store_credit_amount: float = self.get_store_credit_amount()
        self.customer_message: str = self.node['note']
        self.transactions: dict = {'data': []}  # Not implemented
        self.send_gift_cards()

    def __str__(self) -> str:
        result = f'\nOrder ID: {self.id}\n'
        result += '----------------\n'
        result += f'Channel: {self.channel}\n'
        result += f'Email: {self.email}\n'
        result += f'Date Created: {self.date_created}\n'
        result += f'Date Modified: {self.date_modified}\n'
        result += f'Is Refund: {self.is_refund}\n'
        result += f'Is Declined: {self.is_declined}\n'
        result += f'Payment Status: {self.payment_status}\n'
        result += f'Status: {self.status}\n'
        result += str(self.customer)
        result += str(self.billing_address)
        if self.has_shipping_address:
            result += str(self.shipping_address)
        result += f'Transactions: {self.transactions}\n'
        result += '\nItems\n'
        result += '-----\n'

        for i, item in enumerate(self.items):
            result += f'\nItem {i + 1}\n'
            result += '---------'
            result += str(item)

        result += '\nDiscounts\n'
        result += '---------\n'
        result += f'Coupon Codes: {self.coupon_codes}\n'
        result += f'Coupon Amount: ${self.coupon_amount}\n'
        result += f'Header Discount: ${self.header_discount}\n'
        result += '\nTotals\n'
        result += '------\n'
        result += f'Subtotal: ${self.subtotal}\n'
        result += f'Subtotal Ex Tax: ${self.subtotal_ex_tax}\n'
        result += f'Subtotal Inc Tax: ${self.subtotal_inc_tax}\n'
        result += f'Total: ${self.total}\n'
        result += f'Total Ex Tax: ${self.total_ex_tax}\n'
        result += f'Total Inc Tax: ${self.total_inc_tax}\n'
        result += f'Refund Total: ${self.refund_total}\n'
        result += f'Store Credit Amount: ${self.store_credit_amount}\n'

        return result

    def get_status(self) -> str:
        if len(self.refunds) > 0:
            return 'Partially Refunded'
        else:
            return self.node['displayFulfillmentStatus']

    def get_items(self) -> list['LineItem']:
        node_items = []
        for i in self.node['lineItems']['edges']:
            item = i['node']
            if item['sku'] is not None and 'GFC' in item['sku'] and item['quantity'] > 1:
                # Convert gift card line to multiple lines
                for _ in range(item['quantity']):
                    node_items.append(item)
            else:
                node_items.append(item)

        if not node_items:
            return []

        results: list['LineItem'] = []

        for item in node_items:
            price = ShopifyOrder.get_money(item['originalUnitPriceSet'])
            is_refunded = False
            quantity_refunded = 0

            if len(self.refunds) > 0:
                for refunds in self.refunds:
                    for refund in refunds['refundLineItems']['edges']:
                        if refund['node']['lineItem']['id'] == item['id']:
                            is_refunded = True
                            quantity_refunded = int(refund['node']['quantity'])
                            self.refunded_subtotal += price * float(quantity_refunded)

            if item['name'] is None:
                item['name'] = ''

            if item['name'].split('-')[0].strip().lower() == 'delivery':
                if is_refunded:
                    item['quantity'] = quantity_refunded
                self.delivery_from_lines += price * float(item['quantity'])
                continue

            if item['name'].split('-')[0].strip().lower() == 'service':
                item['sku'] = 'SERVICE'

            if item['sku'] is None:
                continue

            item['isGiftCard'] = 'GFC' in item['sku']

            results.append(
                LineItem(
                    id=item['id'],
                    sku=item['sku'],
                    type='giftcertificate' if item['isGiftCard'] else 'physical',
                    base_price=price,
                    price_ex_tax=price,
                    price_inc_tax=price,
                    price_tax=0,
                    base_total=price,
                    total_ex_tax=price,
                    total_inc_tax=price,
                    total_tax=0,
                    quantity=item['quantity'],
                    is_refunded=is_refunded,
                    quantity_refunded=quantity_refunded,
                    refund_amount=0,
                    return_id=0,
                    fixed_shipping_cost=0,
                    gift_certificate_id=None,
                    discounted_total_inc_tax=ShopifyOrder.get_money(item['discountedTotalSet']),
                    applied_discounts=[],
                )
            )
        return results

    def get_subtotal(self) -> float:
        if len(self.refunds) > 0:
            return self.refunded_subtotal

        return (
            ShopifyOrder.get_money(self.node['currentSubtotalPriceSet']) + self.header_discount + self.shipping_cost
        )

    def get_total(self) -> float:
        target = self.node['totalRefundedSet'] if len(self.refunds) > 0 else self.node['currentTotalPriceSet']
        return ShopifyOrder.get_money(target)

    def send_gift_cards(self):
        for item in self.items:
            if item.type == 'giftcertificate' and not item.is_refunded:
                code = Database.CP.GiftCard.create_code()
                item.gift_certificate_id = code
                if self.email:
                    Email.Customer.GiftCard.send(
                        name=f'{self.billing_address.first_name.title()} {self.billing_address.last_name.title()}',
                        email=self.email,
                        gc_code=item.gift_certificate_id,
                        amount=item.base_price,
                    )
                else:
                    self.error_handler.add_error_v('Cannot Send Gift Card - No Email Provided')

    def get_shipping_cost(self) -> float:
        shipping_cost = 0

        if len(self.refunds) > 0:
            shipping_cost = ShopifyOrder.get_money(self.node['totalRefundedShippingSet'])
        else:
            try:
                shipping_cost = ShopifyOrder.get_money(self.node['shippingLine']['discountedPriceSet'])
            except:
                pass

        shipping_cost += self.delivery_from_lines
        return float(shipping_cost)

    def get_shipping_item(self):
        """Create Dummy Shipping Item"""
        if self.shipping_cost > 0:
            self.items.append(
                ShopifyOrder.LineItem(
                    id='',
                    sku='DELIVERY',
                    type='physical',
                    base_price=self.shipping_cost,
                    price_ex_tax=self.shipping_cost,
                    price_inc_tax=self.shipping_cost,
                    price_tax=0,
                    base_total=self.shipping_cost,
                    total_ex_tax=self.shipping_cost,
                    total_inc_tax=self.shipping_cost,
                    total_tax=0,
                    quantity=1,
                    is_refunded=True if self.status == 'Partially Refunded' else False,
                    quantity_refunded=1 if self.status == 'Partially Refunded' else 0,
                    refund_amount=0,
                    return_id=0,
                    fixed_shipping_cost=0,
                    gift_certificate_id=None,
                    discounted_total_inc_tax=self.shipping_cost,
                    applied_discounts=[],
                )
            )

    def get_store_credit_amount(self) -> float:
        store_credit_amount: float = 0
        for transaction in self.node['transactions']:
            if transaction['gateway'] == 'shopify_store_credit':
                store_credit_amount = float(transaction['amountSet']['shopMoney']['amount'])
                break

        return store_credit_amount

    def get_channel(self) -> str:
        if self.node['channelInformation'] is not None:
            return self.node['channelInformation']['channelDefinition']['handle']
        else:
            return 'EMPTY'

    @staticmethod
    def get_phone(node):
        try:
            return PhoneNumber(
                node['billingAddress']['phone']
                or node['customer']['phone']
                or ((node['shippingAddress'] or {'phone': None})['phone'])
            ).to_cp()
        except:
            return None

    @staticmethod
    def convert_date(date_string: str) -> str:
        date = None
        try:
            date = datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')
        except:
            try:
                # 2024-07-27T17:20:30Z
                date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
            except:
                pass

        date = date.replace(tzinfo=timezone.utc).astimezone(tz=None)
        date_string = date.strftime('%Y-%m-%d %H:%M:%S.%f')
        return date_string[:-3]

    @staticmethod
    def get_money(money: dict) -> float:
        """Returns the money or 0 from a shopify money object"""
        result = 0
        try:
            result = float(money['shopMoney']['amount'])
        except:
            pass

        return float(result)


class Customer:
    def __init__(self, node: dict):
        self.id: int = int(node['customer']['id'].split('/')[-1])
        """Shopify Customer ID (last part of the URL)"""
        self.first_name: str = node['customer']['firstName']
        self.last_name: str = node['customer']['lastName']
        self.email: str = node['customer']['email']
        self.phone: str = node['customer']['phone']
        self.customer_message: str = node['note']

    def __str__(self) -> str:
        result = '\nCustomer\n'
        result += '--------\n'
        result += f'Shopify ID: {self.id}\n'
        result += f'First Name: {self.first_name}\n'
        result += f'Last Name: {self.last_name}\n'
        result += f'Email: {self.email}\n'
        result += f'Phone: {self.phone}\n'
        result += f'Customer Message: {self.customer_message}\n'
        return result


class BillingAddress:
    def __init__(self, node: dict):
        self.first_name: str = None
        self.last_name: str = None
        self.company: str = None
        self.address_1: str = None
        self.address_2: str = None
        self.city: str = None
        self.province: str = None
        self.zip: str = None
        self.country: str = None
        self.phone: str = None
        self.email: str = None
        self.get_billing_address(node)

    def __str__(self) -> str:
        result = '\nBilling Address\n'
        result += '----------------\n'
        result += f'First Name: {self.first_name}\n'
        result += f'Last Name: {self.last_name}\n'
        result += f'Company: {self.company}\n'
        result += f'Address 1: {self.address_1}\n'
        result += f'Address 2: {self.address_2}\n'
        result += f'City: {self.city}\n'
        result += f'Province: {self.province}\n'
        result += f'Zip: {self.zip}\n'
        result += f'Country: {self.country}\n'
        result += f'Phone: {self.phone}\n'
        result += f'Email: {self.email}\n'
        return result

    def get_billing_address(self, node):
        if node['billingAddress']:
            if 'firstName' in node['billingAddress']:
                self.first_name: str = node['billingAddress']['firstName']
            if 'lastName' in node['billingAddress']:
                self.last_name: str = node['billingAddress']['lastName']
            if 'company' in node['billingAddress']:
                self.company: str = node['billingAddress']['company']
            if 'address1' in node['billingAddress']:
                self.address_1: str = node['billingAddress']['address1']
            if 'address2' in node['billingAddress']:
                self.address_2: str = node['billingAddress']['address2']
            if 'city' in node['billingAddress']:
                self.city: str = node['billingAddress']['city']
            if 'province' in node['billingAddress']:
                self.province: str = node['billingAddress']['province']
            if 'zip' in node['billingAddress']:
                self.zip: str = node['billingAddress']['zip']
            if 'country' in node['billingAddress']:
                self.country: str = node['billingAddress']['country']
            if 'phone' in node['billingAddress']:
                self.phone: str = node['billingAddress']['phone']
            if 'email' in node['billingAddress']:
                self.email: str = node['billingAddress']['email']
            else:
                self.email = node['email']
        else:
            self.first_name = node['customer']['firstName']
            self.last_name = node['customer']['lastName']
            self.phone = node['customer']['phone']
            self.email = node['customer']['email'] or node['email']


class ShippingAddress:
    def __init__(self, node: dict):
        self.first_name: str = None
        self.last_name: str = None
        self.company: str = None
        self.address_1: str = None
        self.address_2: str = None
        self.city: str = None
        self.province: str = None
        self.zip: str = None
        self.country: str = None
        self.phone: str = None
        self.email: str = node['email']
        self.get_shipping_address(node)

    def __str__(self) -> str:
        result = '\nShipping Address\n'
        result += '----------------\n'
        result += f'First Name: {self.first_name}\n'
        result += f'Last Name: {self.last_name}\n'
        result += f'Company: {self.company}\n'
        result += f'Address 1: {self.address_1}\n'
        result += f'Address 2: {self.address_2}\n'
        result += f'City: {self.city}\n'
        result += f'Province: {self.province}\n'
        result += f'Zip: {self.zip}\n'
        result += f'Country: {self.country}\n'
        result += f'Phone: {self.phone}\n'
        result += f'Email: {self.email}\n\n'
        return result

    def get_shipping_address(self, node):
        if not node['shippingAddress']:
            self.phone = ShopifyOrder.get_phone(node)
            return

        if 'firstName' in node['shippingAddress']:
            self.first_name = node['shippingAddress']['firstName']
        if 'lastName' in node['shippingAddress']:
            self.last_name = node['shippingAddress']['lastName']
        if 'company' in node['shippingAddress']:
            self.company = node['shippingAddress']['company']
        if 'address1' in node['shippingAddress']:
            self.address_1 = node['shippingAddress']['address1']
        if 'address2' in node['shippingAddress']:
            self.address_2 = node['shippingAddress']['address2']
        if 'city' in node['shippingAddress']:
            self.city = node['shippingAddress']['city']
        if 'state' in node['shippingAddress']:
            self.state = node['shippingAddress']['state']
        if 'zip' in node['shippingAddress']:
            self.zip = node['shippingAddress']['zip']
        if 'country' in node['shippingAddress']:
            self.country = node['shippingAddress']['country']
        if 'phone' in node['shippingAddress']:
            self.phone = node['shippingAddress']['phone']
        else:
            self.phone = ShopifyOrder.get_phone(node)


class LineItem:
    def __init__(
        self,
        id: str,
        sku: str,
        type: str,
        base_price: float,
        price_ex_tax: float,
        price_inc_tax: float,
        price_tax: float,
        base_total: float,
        total_ex_tax: float,
        total_inc_tax: float,
        total_tax: float,
        quantity: float,
        is_refunded: bool,
        quantity_refunded: float,
        refund_amount: float,
        return_id: float,
        fixed_shipping_cost: float,
        gift_certificate_id: str,
        discounted_total_inc_tax: float,
        applied_discounts: list,
    ):
        self.id: str = id
        self.sku: str = sku
        self.type: str = type
        self.base_price: float = base_price
        self.price_ex_tax: float = price_ex_tax
        self.price_inc_tax: float = price_inc_tax
        self.price_tax: float = price_tax
        self.base_total: float = base_total
        self.total_ex_tax: float = total_ex_tax
        self.total_inc_tax: float = total_inc_tax
        self.total_tax: float = total_tax
        self.quantity: float = quantity
        self.cost: float = Database.CP.Product.get_cost(self.sku)
        self.ext_cost: float = self.cost * self.quantity
        self.is_refunded: bool = is_refunded
        self.quantity_refunded: int = quantity_refunded
        self.refund_amount: float = refund_amount
        self.return_id: int = return_id
        self.fixed_shipping_cost: float = fixed_shipping_cost
        self.gift_certificate_id: str = gift_certificate_id
        self.discounted_total_inc_tax: float = discounted_total_inc_tax
        self.applied_discounts: list = applied_discounts
        self.total_discount: float = self.get_total_discount()

    def __str__(self) -> str:
        result = f'\nLine Item ID: {self.id}\n'
        result += f'SKU: {self.sku}\n'
        result += f'Type: {self.type}\n'
        result += f'Base Price: {self.base_price}\n'
        result += f'Price Ex Tax: {self.price_ex_tax}\n'
        result += f'Price Inc Tax: {self.price_inc_tax}\n'
        result += f'Price Tax: {self.price_tax}\n'
        result += f'Base Total: {self.base_total}\n'
        result += f'Total Ex Tax: {self.total_ex_tax}\n'
        result += f'Total Inc Tax: {self.total_inc_tax}\n'
        result += f'Total Tax: {self.total_tax}\n'
        result += f'Quantity: {self.quantity}\n'
        result += f'Cost: {self.cost}\n'
        result += f'Ext Cost: {self.ext_cost}\n'
        result += f'Is Refunded: {self.is_refunded}\n'
        result += f'Quantity Refunded: {self.quantity_refunded}\n'
        result += f'Refund Amount: {self.refund_amount}\n'
        result += f'Return ID: {self.return_id}\n'
        result += f'Fixed Shipping Cost: {self.fixed_shipping_cost}\n'
        result += f'Gift Certificate ID: {self.gift_certificate_id}\n'
        result += f'Discounted Total Inc Tax: {self.discounted_total_inc_tax}\n'
        result += f'Applied Discounts: {self.applied_discounts}\n'
        result += f'Total Discount: {self.total_discount}\n'
        return result

    def get_total_discount(self) -> float:
        """Returns the total discount amount associated with a line item"""
        total_discount = 0
        if self.type == 'physical':
            if len(self.applied_discounts) > 0:
                for discount in self.applied_discounts:
                    if discount['target'] == 'product':
                        total_discount += abs(float(discount['amount']))
        return total_discount
