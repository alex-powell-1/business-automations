from setup.error_handler import ProcessInErrorHandler
from setup.utilities import PhoneNumber
from datetime import datetime, timezone
from integration.shopify_api import Shopify
import json
from database import Database


class Item:
    line_type = 'O'

    def __init__(
        self,
        lin_seq_no: int,
        sku: str,
        quantity: int,
        unit_retail_value: float,
        extended_unit_price: float,
        is_refunded: bool,
        quantity_refunded: float,
    ):
        self.lin_seq_no: int = lin_seq_no

        self.sku: str = sku
        """SKU of the item"""

        self.lin_seq_no: int = lin_seq_no
        """The line sequence number of the item in the order"""

        self.unit_retail_value: float = unit_retail_value
        """Retail Price of the item as defined by PRC_1 in CP"""

        self.extended_unit_price: float = extended_unit_price
        """Retail price minus the discount amount"""

        self.quantity: float = quantity

        self.extended_price: float = self.extended_unit_price * self.quantity
        """Final amount of line item after discount and quantity are applied"""

        # Cost
        self.cost = Database.CP.Product.get_cost(self.sku)
        """Cost of the item as defined by the cost in CP"""
        self.extended_cost: float = self.cost * self.quantity
        """Cost of the item multiplied by the quantity"""

        self.is_refunded: bool = is_refunded
        """Is the line item refunded"""
        self.quantity_refunded: int = quantity_refunded
        """Quantity refunded for the line"""
        self.refund_amount: float = self.extended_unit_price * quantity_refunded
        """Amount refunded for the line - Positive Number"""

    def get_payload(self):
        """Return the payload for the line item to be used in the CP API"""

        return {
            'LIN_TYP': Item.line_type,
            'ITEM_NO': self.sku,
            'USR_ENTD_PRC': False,
            'QTY_SOLD': self.quantity,
            'PRC': self.extended_unit_price,
            'EXT_PRC': self.extended_price,
            'EXT_COST': self.extended_cost,
            'sku': self.sku,
        }


class InventoryItem(Item):
    def __init__(
        self,
        id: int,
        lin_seq_no: int,
        sku: str,
        quantity: int,
        unit_retail_value: float,
        extended_unit_price: float,
        is_refunded: bool,
        quantity_refunded: float,
    ):
        super().__init__(
            lin_seq_no, sku, quantity, unit_retail_value, extended_unit_price, is_refunded, quantity_refunded
        )
        self.id: int = id
        """Shopify ID of the item in the order"""

        self.payload: dict = self.get_payload()

    def __str__(self) -> str:
        result = f'Shopify Item ID: {self.id}\n'
        result += f'SKU: {self.sku}\n'
        result += f'Line Seq No: {self.lin_seq_no}\n'
        result += f'Is Refunded: {self.is_refunded}\n'
        result += f'Quantity Refunded: {self.quantity_refunded}\n'
        result += f'Amount Refunded: ${self.refund_amount:.2f}\n'
        result += f'Unit Retail Value: ${self.unit_retail_value:.2f}\n'
        result += f'Extended Unit Price: ${self.extended_unit_price:.2f}\n'
        result += f'Quantity: {self.quantity}\n'
        result += f'Extended Price: ${self.extended_price:.2f}\n'
        result += '\nCost\n'
        result += f'Unit Cost: ${self.cost:.2f}\n'
        result += f'Extended Cost: ${self.extended_cost:.2f}\n'
        return result


class GiftCard:
    """Gift Card Purchase"""

    def __init__(self, amount: float, lin_seq_no: int, code_no: str = None):
        self.amount: float = amount
        self.number: str = code_no
        self.quantity: float = 1
        self.pay_code: str = 'GC'
        self.description: str = 'Gift Certificate'
        self.lin_seq_no: int = lin_seq_no
        self.gfc_seq_no: int = 0
        self.create_as_store_credit: str = 'N'
        if not self.number:
            self.number = Database.CP.GiftCard.create_code()

    def __str__(self) -> str:
        result = '\nGift Card\n'
        result += '---------\n'
        result += f'Amount: ${self.amount:.2f}\n'
        result += f'Number: {self.number}\n'
        result += f'Pay Code: {self.pay_code}\n'
        result += f'Description: {self.description}\n'
        result += f'Line Seq No: {self.lin_seq_no}\n'
        result += f'GFC Seq No: {self.gfc_seq_no}\n'
        result += f'Create As Store Credit: {self.create_as_store_credit}\n'

        return result

    def get_payload(self):
        return {
            'GFC_COD': self.pay_code,
            'GFC_NO': self.number,
            'AMT': self.amount,
            'LIN_SEQ_NO': self.lin_seq_no,
            'DESCR': self.description,
            'CREATE_AS_STC': self.create_as_store_credit,
            'GFC_SEQ_NO': self.gfc_seq_no,
        }


class Delivery(Item):
    sku = 'DELIVERY'
    quantity = 1

    def __init__(self, unit_retail_value: float, is_refunded: bool, lin_seq_no: int):
        super().__init__(
            lin_seq_no=lin_seq_no,
            sku=Delivery.sku,
            quantity=Delivery.quantity,
            unit_retail_value=unit_retail_value,
            extended_unit_price=unit_retail_value,
            is_refunded=is_refunded,
            quantity_refunded=1 if is_refunded else 0,
        )
        self.payload: dict = self.get_payload()

    def __str__(self) -> str:
        result = '\nDelivery\n'
        result += '--------\n'
        result += f'Amount: ${self.unit_retail_value:.2f}\n'
        return result


class ShopifyOrder:
    def __init__(self, order_id: int, gc_code_override: str = None, verbose: bool = False):
        self.order_id = order_id
        self.gc_code_override = gc_code_override
        self.verbose = verbose
        self.node = Shopify.Order.get(self.order_id)['node']
        if self.verbose:
            print(json.dumps(self.node, indent=4))
        if not self.node:
            raise Exception(f'Order {order_id} not found in Shopify')

        self.logger = ProcessInErrorHandler.logger
        self.error_handler = ProcessInErrorHandler.error_handler
        self.id: str = self.node['name']
        self.channel: str = self.get_channel()
        self.email: str = self.node['email'] or ''
        self.date_created: str = ShopifyOrder.convert_date(self.node['createdAt'])
        self.billing_address: BillingAddress = BillingAddress(self.node)
        self.shipping_address: ShippingAddress = ShippingAddress(self.node)
        self.customer = Customer(self.node) if self.node['customer'] is not None else None
        self.refunds: list[dict] = self.node['refunds']
        self.is_refund: bool = len(self.refunds) > 0
        self.financial_status: str = self.node['displayFinancialStatus']
        self.fulfillment_status: str = self.node['displayFulfillmentStatus']
        self.is_declined: bool = self.financial_status.lower() in ['declined', '']
        # Refund Information
        self.refunded_subtotal: float = ShopifyOrder.get_money(self.node['totalRefundedSet'])
        self.total_refunded_shipping = ShopifyOrder.get_money(self.node['totalRefundedShippingSet'])
        self.refund_total: float = 0
        # Shipping Information
        self.shipping_cost: float = self.get_shipping_cost()
        self.total_discount: float = ShopifyOrder.get_money(self.node['totalDiscountsSet'])
        self.line_items: list[InventoryItem | GiftCard | Delivery] = []
        self.total_extended_cost = self.get_total_extended_cost(self.line_items)
        self.gift_card_purchases: list[GiftCard] = []
        self.is_gift_card_only: bool = len(self.line_items) == 1 and isinstance(self.line_items[0], GiftCard)
        self.is_shipping: bool = self.shipping_cost > 0
        self.base_shipping_cost: float = self.shipping_cost
        self.coupon_codes: list[str] = self.node['discountCodes']
        self.subtotal: float = self.get_subtotal()
        self.total: float = self.get_total()
        self.store_credit_amount: float = self.get_store_credit_amount()
        self.gift_certificate_payment: float = 0  # Not implemented
        self.customer_message: str = self.node['note']
        self.payments: Payments = Payments(self)
        self.get_items()
        self.get_total_refund_amount()

    def __str__(self) -> str:
        result = f'\nOrder ID: {self.id}\n'
        result += '----------------\n'
        result += f'Channel: {self.channel}\n'
        result += f'Email: {self.email}\n'
        result += f'Date Created: {self.date_created}\n'
        result += f'Is Refund: {self.is_refund}\n'
        result += f'Is Declined: {self.is_declined}\n'
        result += f'Payment Status: {self.financial_status}\n'
        result += f'Status: {self.fulfillment_status}\n'
        result += str(self.customer)
        result += str(self.billing_address)
        result += str(self.shipping_address)
        result += '\nInventory Items\n'
        result += '-----\n'
        for i, item in enumerate(self.line_items):
            result += f'\nItem {i + 1}\n'
            result += '---------\n'
            result += str(item)

        result += '\nGift Card Purchases\n'
        result += '-----\n'
        for i, item in enumerate(self.gift_card_purchases):
            result += f'\nGift Card {i + 1}\n'
            result += '---------'
            result += str(item)

        result += '\nDiscounts\n'
        result += '---------\n'
        result += f'Coupon Codes: {self.coupon_codes}\n'
        result += f'Total Discount: ${self.total_discount:.2f}\n'
        result += '\nTotals\n'
        result += '------\n'
        result += f'Subtotal: ${self.subtotal:.2f}\n'
        result += f'Total: ${self.total:.2f}\n'
        result += f'Total Refunded Shipping: ${self.total_refunded_shipping:.2f}\n'
        result += f'Refund Total: ${self.refund_total:.2f}\n'
        result += f'Store Credit Amount: ${self.store_credit_amount:.2f}\n'

        result += str(self.payments)

        return result

    def get_total_extended_cost(self, line_items: list[InventoryItem | GiftCard | Delivery]) -> float:
        total = 0
        for item in line_items:
            if isinstance(item, InventoryItem):
                total += item.extended_cost
        return total

    def get_items(self) -> list[InventoryItem | GiftCard | Delivery]:
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

        result = []

        for i, item in enumerate(node_items, start=1):
            price = float(item['variant']['price'] or 0) if item['variant'] else 0
            compare_at_price = float(item['variant']['compareAtPrice'] or 0) if item['variant'] else 0
            quantity = int(item['quantity'])

            unit_retail_value = max(price, compare_at_price)
            extended_unit_price = ShopifyOrder.get_money(item['discountedUnitPriceAfterAllDiscountsSet'])

            is_refunded = False
            quantity_refunded = 0
            if len(self.refunds) > 0:
                for refunds in self.refunds:
                    for refund in refunds['refundLineItems']['edges']:
                        if refund['node']['lineItem']['id'] == item['id']:
                            is_refunded = True
                            quantity_refunded = int(refund['node']['quantity'])
                            self.refunded_subtotal += extended_unit_price * float(quantity_refunded)

            if item['name'] is None:
                item['name'] = ''

            if item['name'].split('-')[0].strip().lower() == 'service':
                item['sku'] = 'SERVICE'
            
            elif item['name'].split('-')[0].strip().lower() == 'custom':
                item['sku'] = 'CUSTOM'
            
            elif item['name'].replace('-', '').lower() in ['onsite', 'on-site consultation']:
                item['sku'] = 'ONSITE'
            
            elif item['sku'] is None:
                continue

            if 'GFC' in item['sku']:
                result.append(GiftCard(unit_retail_value, lin_seq_no=i, code_no=self.gc_code_override))
            else:
                result.append(
                    InventoryItem(
                        id=item['id'].split('/')[-1],
                        lin_seq_no=i,
                        sku=item['sku'],
                        quantity=quantity,
                        unit_retail_value=unit_retail_value,
                        extended_unit_price=extended_unit_price,
                        is_refunded=is_refunded,
                        quantity_refunded=quantity_refunded,
                    )
                )

        # Get the gift card purchase line sequence numbers
        gc_count = 1
        for item in result:
            if isinstance(item, GiftCard):
                item.gfc_seq_no = gc_count
                gc_count += 1

        self.line_items = result

        # Add delivery line item
        if self.shipping_cost > 0:
            self.line_items.append(
                Delivery(
                    unit_retail_value=self.shipping_cost,
                    is_refunded=self.is_refund,
                    lin_seq_no=len(self.line_items) + 1,
                )
            )

        return result

    def get_subtotal(self) -> float:
        if len(self.refunds) > 0:
            return self.refunded_subtotal

        return (
            ShopifyOrder.get_money(self.node['currentSubtotalPriceSet']) + self.total_discount + self.shipping_cost
        )

    def get_total_refund_amount(self) -> float:
        total: float = 0
        if not self.refunds:
            return total

        for refund in self.refunds:
            for refund_line in refund['refundLineItems']['edges']:
                self.refund_total += ShopifyOrder.get_money(refund_line['node']['subtotalSet'])

        self.refund_total += self.total_refunded_shipping

    def get_total(self) -> float:
        target = self.node['totalRefundedSet'] if len(self.refunds) > 0 else self.node['currentTotalPriceSet']
        return ShopifyOrder.get_money(target)

    def get_shipping_cost(self) -> float:
        shipping_cost = 0

        if len(self.refunds) > 0:
            shipping_cost = ShopifyOrder.get_money(self.node['totalRefundedShippingSet'])
        else:
            try:
                shipping_cost = ShopifyOrder.get_money(self.node['shippingLine']['discountedPriceSet'])
            except:
                pass

        return float(shipping_cost)

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
    def get_phone(node=None, order: 'ShopifyOrder' = None) -> str:
        if not node and not order:
            return None

        if node:
            try:
                return PhoneNumber(
                    node['billingAddress']['phone']
                    or node['customer']['phone']
                    or ((node['shippingAddress'] or {'phone': None})['phone'])
                ).to_cp()
            except:
                return None

        elif order:
            try:
                return PhoneNumber(
                    order.billing_address.phone
                    or order.customer.phone
                    or (order.shipping_address or {'phone': None}).phone
                ).to_cp()
            except:
                return None

    @staticmethod
    def convert_date(date_string: str, as_dt: bool = False) -> datetime:
        date = None
        try:
            date = datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')
        except:
            try:
                # 2024-07-27T17:20:30Z
                date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
            except:
                pass

        return date.replace(tzinfo=timezone.utc).astimezone(tz=None)

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
        self.state: str = None
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
        result += f'State: {self.state}\n'
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
                self.state: str = node['billingAddress']['province']
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
            if node['customer']:
                self.first_name = node['customer']['firstName']
                self.last_name = node['customer']['lastName']
                self.phone = node['customer']['phone']
                self.email = node['customer']['email'] or node['email']
            else:
                self.first_name = None
                self.last_name = None
                self.phone = None
                self.email = None


class ShippingAddress:
    def __init__(self, node: dict = None):
        self.first_name: str = None
        self.last_name: str = None
        self.company: str = None
        self.address_1: str = None
        self.address_2: str = None
        self.city: str = None
        self.state: str = None
        self.zip: str = None
        self.country: str = None
        self.phone: str = None
        self.email: str = None
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
        result += f'State: {self.state}\n'
        result += f'Zip: {self.zip}\n'
        result += f'Country: {self.country}\n'
        result += f'Phone: {self.phone}\n'
        result += f'Email: {self.email}\n\n'
        return result

    def get_shipping_address(self, node):
        if not node['shippingAddress']:
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
        if 'province' in node['shippingAddress']:
            self.state = node['shippingAddress']['province']
        if 'zip' in node['shippingAddress']:
            self.zip = node['shippingAddress']['zip']
        if 'country' in node['shippingAddress']:
            self.country = node['shippingAddress']['country']
        if 'phone' in node['shippingAddress']:
            self.phone = node['shippingAddress']['phone']
        if 'email' in node['shippingAddress']:
            self.email = node['shippingAddress']['email']


class Payments:
    def __init__(self, order: ShopifyOrder):
        self.order: ShopifyOrder = order
        self.shopify_payment: ShopifyPayment = ShopifyPayment(order)
        self.loyalty_payment: LoyaltyPayment = LoyaltyPayment(order)
        self.gc_payment: GCPayment = None  # GCPayment(order) - Not implemented
        self.payload: dict = self.get_payload()

    def __str__(self) -> str:
        result = '\nPayments\n'
        result += '--------\n'
        result += str(self.shopify_payment)
        result += str(self.loyalty_payment)
        return result

    def get_payload(self) -> dict:
        """Returns the payment payload to be used in NCR Counterpoint API"""
        payload = []
        if self.shopify_payment.payload:
            payload.append(self.shopify_payment.payload)
        if self.loyalty_payment.get_payload():
            payload.append(self.loyalty_payment.get_payload())
        return payload


class Payment:
    def __init__(self, order: ShopifyOrder):
        self.order: ShopifyOrder = order
        self.FINAL_PMT: str = 'N'
        self.PMT_LIN_TYP: str = 'C' if order.is_refund else 'T'


class LoyaltyPayment(Payment):
    """Loyalty Payment"""

    def __init__(self, order: ShopifyOrder):
        super().__init__(order)
        self.PAY_COD: str = 'LOYALTY'
        self.AMT: float = order.store_credit_amount
        self.payload = self.get_payload

    def __str__(self) -> str:
        result = '\nLoyalty Payment\n'
        result += '---------------\n'
        result += f'AMT: {self.AMT}\n'
        result += f'PAY_COD: {self.PAY_COD}\n'
        result += f'FINAL_PMT: {self.FINAL_PMT}\n'
        result += f'PMT_LIN_TYP: {self.PMT_LIN_TYP}\n'
        return result

    def get_payload(self) -> dict:
        if self.order.store_credit_amount > 0:
            return {
                'AMT': self.AMT,
                'PAY_COD': self.PAY_COD,
                'FINAL_PMT': self.FINAL_PMT,
                'PMT_LIN_TYP': self.PMT_LIN_TYP,
            }


class ShopifyPayment(Payment):
    """Shopify Payment"""

    def __init__(self, order: ShopifyOrder):
        super().__init__(order)
        self.PAY_COD: str = 'SHOP'
        self.AMT: float = order.total - order.store_credit_amount
        self.payload = self.get_payload()

    def __str__(self) -> str:
        result = '\nShopify Payment\n'
        result += '---------------\n'
        result += f'AMT: {self.AMT}\n'
        result += f'PAY_COD: {self.PAY_COD}\n'
        result += f'FINAL_PMT: {self.FINAL_PMT}\n'
        result += f'PMT_LIN_TYP: {self.PMT_LIN_TYP}\n'
        return result

    def get_payload(self) -> dict:
        return {
            'AMT': self.AMT,
            'PAY_COD': self.PAY_COD,
            'FINAL_PMT': self.FINAL_PMT,
            'PMT_LIN_TYP': self.PMT_LIN_TYP,
        }


class GCPayment(Payment):
    """Gift Card Payment - Not implemented"""

    def __init__(self, order: ShopifyOrder):
        super().__init__(order)
        self.PAY_COD: str = 'GC'


if __name__ == '__main__':
    print(ShopifyOrder(5717619933351))

    """Things to work on: Now that the appropriate discount amount is associated with each line item,
    How will this affect the total discount amount?"""
