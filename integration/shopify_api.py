from setup import creds
from setup.creds import Metafield, Route
import requests
import json
from time import sleep
from setup.error_handler import ProcessOutErrorHandler
from pathlib import Path
from database import Database
from shortuuid import ShortUUID
from setup.email_engine import Email
from customer_tools.customers import lookup_customer
from traceback import print_exc as tb
from setup.utilities import PhoneNumber, local_to_utc
from datetime import datetime


verbose_print = False


class MoveInput:
    def __init__(self, item_id: int, position: int):
        self.item_id = f'gid://shopify/Product/{item_id}'
        self.position = position

    def get(self):
        return {'id': self.item_id, 'newPosition': f'{self.position}'}


class Moves:
    def __init__(self):
        self.moves: list[MoveInput] = []

    def can_add(self):
        return len(self.moves) < 250

    def add(self, move: MoveInput):
        self.moves.append(move)

    def get(self):
        return self.moves


class MovesCollection:
    def __init__(self):
        self.moves: list[Moves] = [Moves()]

    def add(self, move: MoveInput):
        if not self.moves[-1].can_add():
            self.moves.append(Moves())
        self.moves[-1].add(move)

    def get(self):
        return [move.get() for move in self.moves]


class Shopify:
    eh = ProcessOutErrorHandler
    logger = eh.logger
    error_handler = eh.error_handler
    token = creds.shopify_admin_token
    shop_url = creds.shopify_shop_url
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}

    class Query:
        def __init__(self, document, variables=None, operation_name=None, verbose=True):
            self.response = self.execute_query(document, variables, operation_name)
            self.data = self.response['data'] if 'data' in self.response else None
            self.errors = self.response['errors'] if 'errors' in self.response else None
            self.user_errors = []
            if self.data:
                for i in self.data:
                    try:
                        for j in self.data[i]:
                            if j == 'userErrors':
                                for k in self.data[i][j]:
                                    self.user_errors.append(k['message'])
                    except:
                        print(i)

            if self.errors:
                Shopify.error_handler.add_error_v(f'Error: {self.errors}')

            if self.user_errors:
                Shopify.error_handler.add_error_v(
                    f'User Error: {self.user_errors}\nResponse: {json.dumps(self.response, indent=4)}'
                )
            if self.errors or self.user_errors:
                if self.user_errors:
                    for i in self.user_errors:
                        if operation_name.startswith('customer'):
                            if i == 'Email has already been taken':
                                Shopify.logger.warn(
                                    f'Operation: {operation_name} - Duplicate Email: {variables["input"]["email"]}'
                                )
                                # Remove email from variables
                                if 'email' in variables['input']:
                                    del variables['input']['email']
                                if 'emailMarketingConsent' in variables['input']:
                                    del variables['input']['emailMarketingConsent']
                                for error in self.user_errors:
                                    # This will remove any instances of this error from the user_errors list
                                    if error == 'Email has already been taken':
                                        self.user_errors.remove(error)
                                return self.__init__(document, variables, operation_name)

                            elif i == 'Phone has already been taken':
                                Shopify.logger.warn(
                                    f'Operation: {operation_name} - Duplicate Phone: {variables["input"]["phone"]}'
                                )
                                # Remove phone from variables
                                if 'phone' in variables['input']:
                                    del variables['input']['phone']
                                if 'smsMarketingConsent' in variables['input']:
                                    del variables['input']['smsMarketingConsent']
                                for error in self.user_errors:
                                    # This will remove any instances of this error from the user_errors list
                                    if error == 'Phone has already been taken':
                                        self.user_errors.remove(error)
                                return self.__init__(document, variables, operation_name)

                            elif i == 'Province is not valid':
                                province_code_list = [x['provinceCode'] for x in variables['input']['addresses']]
                                if province_code_list:
                                    counter = 0
                                    for code in province_code_list:
                                        # Remove province from variables
                                        if 'provinceCode' in variables['input']['addresses'][counter]:
                                            del variables['input']['addresses'][counter]['provinceCode']
                                        counter += 1
                                for error in self.user_errors:
                                    # This will remove any instances of this error from the user_errors list
                                    if error == 'Province is not valid':
                                        self.user_errors.remove(error)

                                return self.__init__(document, variables, operation_name)

                            elif i == 'Customer does not exist':
                                Database.Shopify.Customer.delete(
                                    shopify_cust_no=variables['input']['id'].split('/')[-1]
                                )
                                # remove id from variables
                                variables['id'] = None
                                self.user_errors.remove(i)
                                # Re-run query
                                return self.__init__(document, variables, operation_name)

                            elif i == 'Key must be unique within this namespace on this resource':
                                # Delete all customer metafields for this customer and resend the payload
                                customer_id = variables['input']['id'].split('/')[-1]
                                Shopify.Metafield.delete(customer_id=customer_id)
                                Database.Shopify.Customer.Metafield.delete(shopify_cust_no=customer_id)
                                for error in self.user_errors:
                                    # This will remove any instances of this error from the user_errors list
                                    if error == 'Key must be unique within this namespace on this resource':
                                        self.user_errors.remove(error)
                                # Re-run query
                                Shopify.logger.info(f'Re-running query for customer {customer_id}')
                                return self.__init__(document, variables, operation_name)

                            elif operation_name == 'customerDelete':
                                if i == "Customer can't be found":
                                    # If a customer cannot be found in shopify, simply remove this error and move on.
                                    self.user_errors.remove(i)
                                    continue
                                elif i == 'Customer can’t be deleted because they have associated orders':
                                    # If a customer cannot be deleted because they have associated orders, simply remove this error and move on.
                                    self.user_errors.remove(i)
                                    continue

                        elif operation_name == 'customerUpdate':
                            if i == 'Key must be unique within this namespace on this resource':
                                # Delete all customer metafields for this customer and resend the payload
                                customer_id = variables['input']['id'].split('/')[-1]
                                Shopify.Metafield.delete(customer_id=customer_id)
                                Database.Shopify.Customer.Metafield.delete(shopify_cust_no=customer_id)
                                for error in self.user_errors:
                                    # This will remove any instances of this error from the user_errors list
                                    if error == 'Key must be unique within this namespace on this resource':
                                        self.user_errors.remove(error)
                                # Re-run query
                                Shopify.logger.info(f'Re-running query for customer {customer_id}')
                                return self.__init__(document, variables, operation_name)

                        elif operation_name.startswith('product'):
                            if operation_name == 'productUpdate':
                                if i == "Namespace can't be blank":
                                    break
                                elif i == "Type can't be blank":
                                    break

                                elif i == 'Key must be unique within this namespace on this resource':
                                    product_id = variables['input']['id'].split('/')[-1]
                                    Shopify.Metafield.delete(product_id=product_id)
                                    Database.Shopify.Product.Metafield.delete(product_id=product_id)
                                    for error in self.user_errors:
                                        # This will remove any instances of this error from the user_errors list
                                        if error == 'Key must be unique within this namespace on this resource':
                                            self.user_errors.remove(error)
                                    # Re-run query
                                    return self.__init__(document, variables, operation_name)

                            elif operation_name == 'productDelete':
                                if i == 'Product does not exist':
                                    # If a product cannot be found in shopify, simply remove this error and move on.
                                    self.user_errors.remove(i)
                                    continue

                            elif i == 'Metafield does not exist':
                                self.user_errors.remove(i)
                                continue

                        else:
                            # Uncaught user error
                            raise Exception(
                                f'Operation Name: {operation_name}\n\nUser Error: {self.user_errors}\n\nVariables: {variables}'
                            )
                else:
                    raise Exception(
                        f'Operation Name: {operation_name}\n\nError: {self.errors}\n\nUser Error: {self.user_errors}\n\nVariables: {variables}'
                    )

            if verbose_print:
                print(operation_name, self)

        def __str__(self):
            return json.dumps(self.response, indent=4)

        def execute_query(self, document, variables=None, operation_name=None):
            query_doc = Path(document).read_text()
            endpoint = f'https://{Shopify.shop_url}/admin/api/2024-07/graphql.json'
            payload = {'query': query_doc, 'variables': variables, 'operationName': operation_name}
            response = requests.post(endpoint, headers=Shopify.headers, json=payload)
            try:
                return response.json()
            except:
                if response.text.startswith('upstream connect error or disconnect/reset before headers.'):
                    sleep(5)
                    return self.execute_query(document, variables, operation_name)

                raise Exception(f'Error: {response.text}')

    class Order:
        queries = './integration/queries/orders.graphql'
        prefix = 'gid://shopify/Order/'

        @staticmethod
        def get(order_id: int):
            response = Shopify.Query(
                document=Shopify.Order.queries,
                operation_name='order',
                variables={'id': f'{Shopify.Order.prefix}{order_id}'},
            )
            return response.data

        @staticmethod
        def get_all():
            response = Shopify.Query(document=Shopify.Order.queries, operation_name='orders')
            return response.data

        @staticmethod
        def as_bc_order(order_id: int, send=False):
            """Convert Shopify order to BigCommerce order format"""
            shopify_order = Shopify.Order.get(order_id)
            snode = shopify_order['node']
            billing = snode['billingAddress'] or {
                'firstName': snode['customer']['firstName'],
                'lastName': snode['customer']['lastName'],
                'company': None,
                'address1': None,
                'address2': None,
                'city': None,
                'province': None,
                'zip': None,
                'country': None,
                'phone': snode['customer']['phone'],
                'email': snode['customer']['email'],
            }
            status = snode['displayFulfillmentStatus']

            shopify_products = []

            delivery_from_lines = 0

            refunded_subtotal = 0

            items = []

            # Convert gift card line into multiple lines
            for i, _item in enumerate(snode['lineItems']['edges']):
                item = _item['node']

                if item['sku'] is not None and 'GFC' in item['sku'] and item['quantity'] > 1:
                    for _ in range(item['quantity']):
                        items.append(item)
                else:
                    items.append(item)

            for item in items:

                def get_money(money: dict):
                    return money['shopMoney']['amount']

                price = float(get_money(item['originalUnitPriceSet']))  # Fixed

                is_refunded = False
                quantity_refunded = 0

                if len(snode['refunds']) > 0:
                    for refunds in snode['refunds']:
                        for refund in refunds['refundLineItems']['edges']:
                            if refund['node']['lineItem']['id'] == item['id']:
                                is_refunded = True
                                quantity_refunded = int(refund['node']['quantity'])

                                refunded_subtotal += price * float(quantity_refunded)

                if item['name'] is None:
                    item['name'] = ''

                if item['name'].split('-')[0].strip().lower() == 'delivery':
                    if is_refunded:
                        item['quantity'] = quantity_refunded

                    delivery_from_lines += price * float(item['quantity'])
                    continue

                if item['name'].split('-')[0].strip().lower() == 'service':
                    item['sku'] = 'SERVICE'

                if item['sku'] is None:
                    continue

                item['isGiftCard'] = 'GFC' in item['sku']

                pl = {
                    'id': item['id'],
                    'sku': item['sku'],
                    'type': 'giftcertificate' if item['isGiftCard'] else 'physical',
                    'base_price': price,
                    'price_ex_tax': price,
                    'price_inc_tax': price,
                    'price_tax': 0,
                    'base_total': price,
                    'total_ex_tax': price,
                    'total_inc_tax': price,
                    'total_tax': 0,
                    'quantity': item['quantity'],
                    'is_refunded': is_refunded,
                    'quantity_refunded': quantity_refunded,
                    'refund_amount': 0,
                    'return_id': 0,
                    'fixed_shipping_cost': 0,
                    'gift_certificate_id': None,
                    'discounted_total_inc_tax': get_money(item['discountedTotalSet']),
                    'applied_discounts': [],
                }

                if item['isGiftCard'] and status == 'UNFULFILLED' and send and not is_refunded:

                    def has_code(code):
                        query = f"""
                        SELECT GFC_NO FROM SY_GFC
                        WHERE GFC_NO = '{code}'
                        """

                        response = Database.query(query)
                        try:
                            return response[0][0] is not None
                        except:
                            return False

                    # Make sure code is unique
                    def gen_code():
                        code_gen = ShortUUID()
                        code_gen.set_alphabet('ABCDEFG123456789')  # 16
                        code = code_gen.random(12)
                        code = f'{code[0:4]}-{code[4:8]}-{code[8:12]}'

                        if has_code(code):
                            return gen_code()
                        else:
                            return code

                    code = gen_code()

                    pl['gift_certificate_id'] = {'code': code}

                    Email.Customer.GiftCard.send(
                        name=f'{billing['firstName']} {billing["lastName"]}',
                        email=snode['email'],
                        gc_code=code,
                        amount=price,
                    )

                shopify_products.append(pl)

            def get_money(money: dict):
                return money['shopMoney']['amount']

            try:
                shippingCost = float(get_money(snode['shippingLine']['discountedPriceSet']))
            except:
                shippingCost = 0

            header_discount = float(get_money(snode['totalDiscountsSet']))

            subtotal = float(get_money(snode['currentSubtotalPriceSet'])) + header_discount + shippingCost
            total = float(get_money(snode['currentTotalPriceSet']))
            refund_total = float(get_money(snode['totalRefundedSet']))

            if len(snode['refunds']) > 0:
                status = 'Partially Refunded'
                subtotal = refunded_subtotal
                total = float(get_money(snode['totalRefundedSet']))
                shippingCost = float(get_money(snode['totalRefundedShippingSet']))

            # Delivery from lines is total from Dummy Delivery Item
            shippingCost += delivery_from_lines

            def create_shipping_item():
                return {
                    'id': '',
                    'sku': 'DELIVERY',
                    'type': 'physical',
                    'base_price': shippingCost,
                    'price_ex_tax': shippingCost,
                    'price_inc_tax': shippingCost,
                    'price_tax': 0,
                    'base_total': shippingCost,
                    'total_ex_tax': shippingCost,
                    'total_inc_tax': shippingCost,
                    'total_tax': 0,
                    'quantity': 1,
                    'is_refunded': True if status == 'Partially Refunded' else False,
                    'quantity_refunded': 1 if status == 'Partially Refunded' else 0,
                    'refund_amount': 0,
                    'return_id': 0,
                    'fixed_shipping_cost': 0,
                    'gift_certificate_id': None,
                    'discounted_total_inc_tax': shippingCost,
                    'applied_discounts': [],
                }

            def get_store_credit_amount():
                store_credit_amount = 0
                for transaction in snode['transactions']:
                    if transaction['gateway'] == 'shopify_store_credit':
                        store_credit_amount = float(transaction['amountSet']['shopMoney']['amount'])
                        break
                return store_credit_amount

            if shippingCost > 0:
                shopify_products.append(create_shipping_item())

            def get_phone():
                try:
                    return PhoneNumber(
                        billing['phone']
                        or snode['customer']['phone']
                        or ((snode['shippingAddress'] or {'phone': None})['phone'])
                    ).to_cp()
                except:
                    return None

            bc_order = {
                'id': snode['name'],
                'customer_id': snode['customer']['id'],
                'date_created': snode['createdAt'],
                'date_modified': snode['updatedAt'],
                'status': status,
                'subtotal_ex_tax': subtotal,
                'subtotal_inc_tax': subtotal,
                'base_shipping_cost': shippingCost,
                'total_ex_tax': total,
                'total_inc_tax': total,
                'refund_total': refund_total,
                'items_total': snode['subtotalLineItemsQuantity'],
                'payment_status': snode['displayFinancialStatus'],
                'store_credit_amount': get_store_credit_amount(),
                'customer_message': snode['note'],
                'billing_address': {
                    'first_name': billing['firstName'],
                    'last_name': billing['lastName'],
                    'company': billing['company'],
                    'street_1': billing['address1'],
                    'street_2': billing['address2'],
                    'city': billing['city'],
                    'state': billing['province'],
                    'zip': billing['zip'],
                    'country': billing['country'],
                    'phone': get_phone(),
                    'email': snode['email'] or snode['customer']['email'],
                },
                'products': {'url': shopify_products},
                'shipping_addresses': {
                    'url': [
                        {
                            'first_name': (snode['shippingAddress'] or {'firstName': None})['firstName'],
                            'last_name': (snode['shippingAddress'] or {'lastName': None})['lastName'],
                            'company': (snode['shippingAddress'] or {'company': None})['company'],
                            'street_1': (snode['shippingAddress'] or {'address1': None})['address1'],
                            'street_2': (snode['shippingAddress'] or {'address2': None})['address2'],
                            'city': (snode['shippingAddress'] or {'city': None})['city'],
                            'state': (snode['shippingAddress'] or {'province': None})['province'],
                            'zip': (snode['shippingAddress'] or {'zip': None})['zip'],
                            'country': (snode['shippingAddress'] or {'country': None})['country'],
                            'phone': (snode['shippingAddress'] or {'phone': None})['phone'] or get_phone(),
                            'email': snode['email'],
                        }
                    ]
                },
                'coupons': {'url': []},
                'transactions': {'data': []},
                'order_coupons': snode['discountCodes'],
                'channel': snode['channelInformation']['channelDefinition']['handle']
                if snode['channelInformation'] is not None
                else 'EMPTY',
            }

            if header_discount > 0:
                bc_order['coupons']['url'] = [{'amount': header_discount}]

            return bc_order

        def get_orders_not_in_cp():
            query = """
            SELECT TKT_NO, TKT_DT FROM PS_DOC_HDR
            WHERE STR_ID = 'WEB' OR STA_ID = 'POS'
            UNION
            SELECT TKT_NO, TKT_DT FROM PS_TKT_HIST
            WHERE STR_ID = 'WEB' OR STA_ID = 'POS'
            ORDER BY TKT_DT DESC
            OFFSET 0 ROWS
            FETCH NEXT 300 ROWS ONLY
            """

            response = Database.query(query)

            try:
                tkt_nos = [x[0].replace('S', '') for x in response]

                # print(tkt_nos)

                orders = []

                for _order in Shopify.Order.get_all()['orders']['edges']:
                    order = _order['node']

                    if order['name'] not in tkt_nos:
                        orders.append(order)

                return orders

            except:
                return []

        def get_id_from_tkt_no(tkt_no):
            orders = Shopify.Order.get_all()
            for _order in orders['orders']['edges']:
                order = _order['node']
                if order['name'] == tkt_no:
                    return order['id'].split('/')[-1]

        def update_order(order_id, payload):
            url = f'https://{Shopify.shop_url}/admin/api/2024-07/orders/{order_id}.json'
            payload = {'order': {'id': order_id, **payload}}
            response = requests.put(url=url, headers=Shopify.headers, json=payload)
            return response.json()

        def update_customer(order_id, customer_id):
            customer = Shopify.Customer.get(customer_id, rest=True)
            return Shopify.Order.update_order(order_id, {'customer': customer})

        def remove_customer(order_id):
            return Shopify.Order.update_order(order_id, {'customer': None})

        class Draft:
            queries = './integration/queries/draft_orders.graphql'
            prev_order_id = -1
            prev_order = None

            @staticmethod
            def get(order_id: int):
                if Shopify.Order.Draft.prev_order_id == order_id:
                    return Shopify.Order.Draft.prev_order

                response = Shopify.Query(
                    document=Shopify.Order.Draft.queries,
                    variables={'id': f'gid://shopify/DraftOrder/{order_id}'},
                    operation_name='draftOrder',
                )

                Shopify.Order.Draft.prev_order = response.data
                Shopify.Order.Draft.prev_order_id = order_id

                return response.data

            @staticmethod
            def get_cust_no(order_id: int):
                try:
                    shopify_order = Shopify.Order.Draft.get(order_id)
                    snode = shopify_order['node']
                    customer = snode['customer']
                    billing = snode['billingAddress']
                    email = snode['email'] or None

                    if customer is not None and email is None:
                        email = customer['email']

                    phone = billing['phone'] if billing is not None else None

                    if customer is not None and phone is None:
                        phone = customer['phone']

                    return lookup_customer(email_address=email, phone_number=phone)
                except:
                    return None

            @staticmethod
            def delete(order_id: int):
                response = Shopify.Query(
                    document=Shopify.Order.Draft.queries,
                    variables={'input': {'id': f'gid://shopify/DraftOrder/{order_id}'}},
                    operation_name='draftOrderDelete',
                )
                return response.data

            @staticmethod
            def get_note(order_id: int):
                shopify_order = Shopify.Order.Draft.get(order_id)
                snode = shopify_order['node']
                note = snode['note2'] or ''
                return note

            @staticmethod
            def get_events(order_id: int):
                shopify_order = Shopify.Order.Draft.get(order_id)
                snode = shopify_order['node']

                if snode['events'] is None or len(snode['events']['edges']) == 0:
                    return []

                events = snode['events']['edges']
                events = [x['node'] for x in events]

                return events

            @staticmethod
            def get_discount(order_id: int):
                try:
                    shopify_order = Shopify.Order.Draft.get(order_id)
                    snode = shopify_order['node']
                    hdsc = float(snode['totalDiscountsSet']['shopMoney']['amount'])
                    return hdsc
                except:
                    return 0

            @staticmethod
            def get_shipping(order_id: int):
                try:
                    shopify_order = Shopify.Order.Draft.get(order_id)
                    snode = shopify_order['node']
                    shipping = float(snode['shippingLine']['discountedPriceSet']['shopMoney']['amount'])

                    for _item in snode['lineItems']['edges']:
                        item = _item['node']

                        price = float(item['originalUnitPriceSet']['shopMoney']['amount'])
                        qty = float(item['quantity'])

                        if item['name'] is not None and item['name'].lower() == 'delivery':
                            shipping += price * qty

                    return float(shipping)
                except:
                    return 0

            @staticmethod
            def get_subtotal(order_id: int):
                try:
                    shopify_order = Shopify.Order.Draft.get(order_id)
                    snode = shopify_order['node']
                    sub_tot = float(snode['subtotalPriceSet']['shopMoney']['amount'])
                    return sub_tot
                except:
                    return 0

    class Customer:
        queries = './integration/queries/customers.graphql'
        prefix = 'gid://shopify/Customer/'

        def get(customer_id: int = None, rest=False):
            if rest:
                if customer_id:
                    response = requests.get(
                        f'https://{Shopify.shop_url}/admin/api/2024-07/customers/{customer_id}.json',
                        headers=Shopify.headers,
                    )
                    if response.status_code == 200:
                        return response.json()['customer']
            if customer_id:
                variables = {'id': f'gid://shopify/Customer/{customer_id}'}
                response = Shopify.Query(
                    document=Shopify.Customer.queries, variables=variables, operation_name='customer'
                )
                return response.data

            id_list = []
            variables = {'first': 250}
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=variables, operation_name='customers'
            )
            id_list += [int(x['node']['id'].split('/')[-1]) for x in response.data['customers']['edges']]

            while response.data['customers']['pageInfo']['hasNextPage']:
                variables['after'] = response.data['customers']['pageInfo']['endCursor']
                response = Shopify.Query(
                    document=Shopify.Customer.queries, variables=variables, operation_name='customers'
                )
                id_list += [int(x['node']['id'].split('/')[-1]) for x in response.data['customers']['edges']]

            return id_list

        def get_customer_ids_not_in_mw():
            all_shopify_cust_ids = Shopify.Customer.get()
            all_mw_cust_data = Database.Shopify.Customer.get()
            all_mw_cust_ids = []
            for x in all_mw_cust_data:
                all_mw_cust_ids.append(x[2])
            return [x for x in all_shopify_cust_ids if x not in all_mw_cust_ids]

        def get_customer_metafields(metafields: list):
            result = {}
            for x in metafields:
                if x['node']['namespace'] == Metafield.Namespace.Customer.customer:
                    if x['node']['key'] == 'number':
                        result['cust_no_id'] = x['node']['id'].split('/')[-1]
                    elif x['node']['key'] == 'category':
                        result['category_id'] = x['node']['id'].split('/')[-1]
                    elif x['node']['key'] == 'wholesale_price_tier':
                        result['wholesale_price_tier_id'] = x['node']['id'].split('/')[-1]
                    elif x['node']['key'] == 'birth_month':
                        result['birth_month_id'] = x['node']['id'].split('/')[-1]
                    elif x['node']['key'] == 'birth_month_spouse':
                        result['birth_month_spouse_id'] = x['node']['id'].split('/')[-1]

            return result

        def get_by_email(email: str):
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables={'email': email}, operation_name='customerByEmail'
            )
            if response.data['customers']['edges']:
                return response.data['customers']['edges'][0]['node']['id'].split('/')[-1]
            return None

        def get_by_phone(phone: str):
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables={'phone': phone}, operation_name='customerByPhone'
            )
            if response.data['customers']['edges']:
                return response.data['customers']['edges'][0]['node']['id'].split('/')[-1]
            return None

        def create(payload):
            operation_name = 'customerCreate'
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=payload, operation_name=operation_name
            )
            customer_id = response.data[operation_name]['customer']['id'].split('/')[-1]
            metafields = response.data[operation_name]['customer']['metafields']['edges']
            return {'id': customer_id, 'metafields': Shopify.Customer.get_customer_metafields(metafields)}

        def update(payload):
            operation_name = 'customerUpdate'
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=payload, operation_name=operation_name
            )
            customer_id = response.data[operation_name]['customer']['id'].split('/')[-1]
            metafields = response.data[operation_name]['customer']['metafields']['edges']
            return {'id': customer_id, 'metafields': Shopify.Customer.get_customer_metafields(metafields)}

        def delete(customer_id: int = None, all=False):
            if customer_id:
                response = Shopify.Query(
                    document=Shopify.Customer.queries,
                    variables={'id': f'gid://shopify/Customer/{customer_id}'},
                    operation_name='customerDelete',
                )
                try:
                    deleted_id = int(response.data['customerDelete']['deletedCustomerId'].split('/')[-1])
                except:
                    deleted_id = None

                if deleted_id:
                    if deleted_id == customer_id:
                        Shopify.logger.success(f'Successfully deleted customer {customer_id}')
                    else:
                        Shopify.error_handler.add_error_v(
                            f'Error deleting customer. Deleted ID: {deleted_id}, Customer ID: {customer_id}'
                        )
                return response.data
            elif all:
                id_list = Shopify.Customer.get(all=True)
                for i in id_list:
                    response = Shopify.Query(
                        document=Shopify.Customer.queries,
                        variables={'id': f'gid://shopify/Customer/{i}'},
                        operation_name='customerDelete',
                    )

        def backfill(all=False):
            if all:
                cust_ids = Shopify.Customer.get()

            else:
                cust_ids = Shopify.Customer.get_customer_ids_not_in_mw()

            if cust_ids:
                for shop_cust_id in cust_ids:
                    response = Shopify.Customer.get(shop_cust_id)
                    email = response['customer']['email']
                    phone = response['customer']['phone']
                    metafields = response['customer']['metafields']['edges']
                    meta_cust_id = None
                    meta_category = None
                    meta_birth_month = None
                    meta_birth_month_spouse = None
                    meta_wholesale_tier = None
                    for meta in metafields:
                        if meta['node']['namespace'] == 'customer' and meta['node']['key'] == 'number':
                            meta_cust_id = meta['node']['id'].split('/')[-1]
                            print(f'Meta Cust ID: {meta_cust_id}')
                        elif meta['node']['namespace'] == 'customer' and meta['node']['key'] == 'category':
                            meta_category = meta['node']['id'].split('/')[-1]
                            print(f'Meta Category: {meta_category}')
                        elif meta['node']['namespace'] == 'customer' and meta['node']['key'] == 'birth_month':
                            meta_birth_month = meta['node']['id'].split('/')[-1]
                            print(f'Meta Birth Month: {meta_birth_month}')
                        elif (
                            meta['node']['namespace'] == 'customer' and meta['node']['key'] == 'birth_month_spouse'
                        ):
                            meta_birth_month_spouse = meta['node']['id'].split('/')[-1]
                            print(f'Meta Birth Month Spouse: {meta_birth_month_spouse}')
                        elif (
                            meta['node']['namespace'] == 'customer'
                            and meta['node']['key'] == 'wholesale_price_tier'
                        ):
                            meta_wholesale_tier = meta['node']['id'].split('/')[-1]
                            print(f'Meta Wholesale Tier: {meta_wholesale_tier}')

                    loyalty_id = Shopify.Customer.StoreCredit.add_store_credit(shop_cust_id, 1)
                    Shopify.Customer.StoreCredit.remove_store_credit(shop_cust_id, 1)
                    print(f'Loyalty ID: {loyalty_id}')

                    cust_number = lookup_customer(email, phone)
                    if cust_number:
                        if not Database.Shopify.Customer.exists(shopify_cust_no=shop_cust_id):
                            Database.Shopify.Customer.insert(
                                cp_cust_no=cust_number,
                                shopify_cust_no=shop_cust_id,
                                loyalty_point_id=loyalty_id,
                                meta_cust_no_id=meta_cust_id,
                                meta_category_id=meta_category,
                                meta_birth_month_id=meta_birth_month,
                                meta_spouse_birth_month_id=meta_birth_month_spouse,
                                meta_wholesale_price_tier_id=meta_wholesale_tier,
                            )
                        else:
                            Database.Shopify.Customer.update(
                                cp_cust_no=cust_number,
                                shopify_cust_no=shop_cust_id,
                                loyalty_point_id=loyalty_id,
                                meta_cust_no_id=meta_cust_id,
                                meta_category_id=meta_category,
                                meta_birth_month_id=meta_birth_month,
                                meta_spouse_birth_month_id=meta_birth_month_spouse,
                                meta_wholesale_price_tier_id=meta_wholesale_tier,
                            )

        def update_marketing_consent(customer: object):
            if customer.email:
                email_variables = {
                    'input': {
                        'customerId': f'gid://shopify/Customer/{customer.shopify_cust_no}',
                        'emailMarketingConsent': {},
                    }
                }
                if customer.email_subscribe:
                    email_variables['input']['emailMarketingConsent'] = {
                        'marketingOptInLevel': 'SINGLE_OPT_IN',
                        'marketingState': 'SUBSCRIBED',
                    }
                else:
                    email_variables['input']['emailMarketingConsent'] = {'marketingState': 'NOT_SUBSCRIBED'}

                response = Shopify.Query(
                    document=Shopify.Customer.queries,
                    variables=email_variables,
                    operation_name='customerEmailMarketingConsentUpdate',
                )
                return response.data

            if customer.phone:
                phone_variables = {
                    'input': {
                        'customerId': f'gid://shopify/Customer/{customer.shopify_cust_no}',
                        'smsMarketingConsent': {},
                    }
                }
                if customer.sms_subscribe:
                    phone_variables['input']['smsMarketingConsent'] = {
                        'marketingOptInLevel': 'SINGLE_OPT_IN',
                        'marketingState': 'SUBSCRIBED',
                    }
                else:
                    phone_variables['input']['smsMarketingConsent'] = {'marketingState': 'NOT_SUBSCRIBED'}

                response = Shopify.Query(
                    document=Shopify.Customer.queries,
                    variables=phone_variables,
                    operation_name='customerSmsMarketingConsentUpdate',
                )
                return response.data

        class Metafield:
            def get(shopify_cust_no: int):
                response = Shopify.Query(
                    document=Shopify.Customer.queries,
                    variables={'id': f'gid://shopify/Customer/{shopify_cust_no}'},
                    operation_name='customerMetafields',
                )
                return response.data

        class StoreCredit:
            queries = './integration/queries/storeCredit.graphql'

            def get(store_credit_account_id: int):
                variables = {'accountId': f'gid://shopify/StoreCreditAccount/{store_credit_account_id}'}
                response = Shopify.Query(
                    document=Shopify.Customer.StoreCredit.queries,
                    variables=variables,
                    operation_name='storeCreditAccount',
                )
                amount = float(response.data['storeCreditAccount']['balance']['amount'])
                return amount

            def add_store_credit(customer_id: int, amount: float):
                variables = {
                    'id': f'gid://shopify/Customer/{customer_id}',
                    'creditInput': {'creditAmount': {'amount': amount, 'currencyCode': 'USD'}},
                }

                response = Shopify.Query(
                    document=Shopify.Customer.StoreCredit.queries,
                    variables=variables,
                    operation_name='storeCreditAccountCredit',
                )

                account_id = response.data['storeCreditAccountCredit']['storeCreditAccountTransaction']['account'][
                    'id'
                ].split('/')[-1]

                return account_id

            def remove_store_credit(customer_id: int, amount: float):
                variables = {
                    'id': f'gid://shopify/Customer/{customer_id}',
                    'debitInput': {'debitAmount': {'amount': amount, 'currencyCode': 'USD'}},
                }

                response = Shopify.Query(
                    document=Shopify.Customer.StoreCredit.queries,
                    variables=variables,
                    operation_name='storeCreditAccountDebit',
                )
                account_id = response.data['storeCreditAccountDebit']['storeCreditAccountTransaction']['account'][
                    'id'
                ].split('/')[-1]

                return account_id

    class Product:
        queries = './integration/queries/products.graphql'
        prefix = 'gid://shopify/Product/'

        def get(product_id: int = None, collection_title: str = None):
            if product_id:
                variables = {'id': f'{Shopify.Product.prefix}{product_id}'}
                response = Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='product'
                )
                return response.data
            collection_id = None
            if collection_title:
                collections = Shopify.Product.Collection.get()
                for collection in collections:
                    if collection['title'] == collection_title:
                        collection_id = collection['id']
                        break
            if collection_id:
                Database.Shopify.Product.get(collection_id=collection_id)

            id_list = []
            variables = {'first': 250, 'after': None}
            response = Shopify.Query(
                document=Shopify.Product.queries, variables=variables, operation_name='products'
            )
            id_list += [x['node']['id'].split('/')[-1] for x in response.data['products']['edges']]
            while response.data['products']['pageInfo']['hasNextPage']:
                variables['after'] = response.data['products']['pageInfo']['endCursor']
                response = Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='products'
                )
                id_list += [x['node']['id'].split('/')[-1] for x in response.data['products']['edges']]
            return id_list

        def create(product_payload) -> tuple:
            """Create product on shopify and return tuple of product ID, media IDs, and variant IDs"""
            # Step 1: Create base product and associated media. Default Variant is created.

            response = Shopify.Query(
                document=Shopify.Product.queries,
                operation_name='CreateProductWithNewMedia',
                variables=product_payload,
            )

            prod_id = response.data['productCreate']['product']['id'].split('/')[-1]

            media_ids = [
                x['id'].split('/')[-1] for x in response.data['productCreate']['product']['media']['nodes']
            ]

            option_ids = [x['id'].split('/')[-1] for x in response.data['productCreate']['product']['options']]
            option_value_ids = [
                x['id'].split('/')[-1]
                for x in response.data['productCreate']['product']['options'][0]['optionValues']
            ]
            variant_ids = [
                x['id'].split('/')[-1] for x in response.data['productCreate']['product']['variants']['nodes']
            ]
            variant_meta_ids = [
                {
                    'id': x['metafield']['id'].split('/')[-1],
                    'namespace': x['metafield']['namespace'],
                    'key': x['metafield']['key'],
                    'value': x['metafield']['value'],
                }
                for x in response.data['productCreate']['product']['variants']['nodes']
                if x['metafield']
            ]

            inventory_ids = [
                x['inventoryItem']['id'].split('/')[-1]
                for x in response.data['productCreate']['product']['variants']['nodes']
            ]

            meta_ids = [
                {'id': x['node']['id'].split('/')[-1], 'namespace': x['node']['namespace'], 'key': x['node']['key']}
                for x in response.data['productCreate']['product']['metafields']['edges']
            ]

            result = {
                'product_id': prod_id,
                'media_ids': media_ids,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'variant_ids': variant_ids,
                'variant_meta_ids': variant_meta_ids,
                'inventory_ids': inventory_ids,
                'meta_ids': meta_ids,
            }
            return result

        def update(product_payload):
            if 'media' in product_payload:
                operation_name = 'UpdateProductWithNewMedia'
            else:
                operation_name = 'updateProduct'

            response = Shopify.Query(
                document=Shopify.Product.queries, operation_name=operation_name, variables=product_payload
            )

            if response.errors or response.user_errors:
                raise Exception(
                    f'Error: {response.errors}\nUser Error: {response.user_errors}\nPayload: {product_payload}'
                )

            media_ids = [
                x['id'].split('/')[-1] for x in response.data['productUpdate']['product']['media']['nodes']
            ]
            option_ids = [x['id'].split('/')[-1] for x in response.data['productUpdate']['product']['options']]
            meta_ids = [
                {'id': x['node']['id'].split('/')[-1], 'namespace': x['node']['namespace'], 'key': x['node']['key']}
                for x in response.data['productUpdate']['product']['metafields']['edges']
            ]

            variant_meta_ids = [
                {
                    'id': x['metafield']['id'].split('/')[-1],
                    'namespace': x['metafield']['namespace'],
                    'key': x['metafield']['key'],
                    'value': x['metafield']['value'],
                }
                for x in response.data['productUpdate']['product']['variants']['nodes']
                if x['metafield']
            ]

            return {
                'media_ids': media_ids,
                'option_ids': option_ids,
                'meta_ids': meta_ids,
                'variant_meta_ids': variant_meta_ids,
            }

        def delete(product_id: int = None, all=False, eh=ProcessOutErrorHandler):
            if product_id:
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                    operation_name='productDelete',
                )
                try:
                    deleted_product_id = int(response.data['productDelete']['deletedProductId'].split('/')[-1])
                    if deleted_product_id == product_id:
                        Shopify.logger.success(f'Product {product_id} deleted from Shopify.')
                    else:
                        eh.logger(f'No match. deleted_product_id: {deleted_product_id}, product_id: {product_id}')
                except:
                    Shopify.error_handler.add_error_v(error=f'Could not get deleted product ID: {response.data}')

                return response.data
            elif all:
                id_list = Shopify.Product.get(all=True)
                for i in id_list:
                    response = Shopify.Query(
                        document=Shopify.Product.queries,
                        variables={'id': f'{Shopify.Product.prefix}{i}'},
                        operation_name='productDelete',
                    )

        def publish(product_id: int, online_store=True, POS=True, shop=True, inbox=True, google=True):
            if online_store:
                variables = {
                    'id': f'{Shopify.Product.prefix}{product_id}',
                    'input': {'publicationId': creds.shopify_channel_online_store},
                }
                Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='publishablePublish'
                )
            if POS:
                variables = {
                    'id': f'{Shopify.Product.prefix}{product_id}',
                    'input': {'publicationId': creds.shopify_channel_pos},
                }
                Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='publishablePublish'
                )
            if shop:
                variables = {
                    'id': f'{Shopify.Product.prefix}{product_id}',
                    'input': {'publicationId': creds.shopify_channel_shop},
                }
                Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='publishablePublish'
                )
            if inbox:
                variables = {
                    'id': f'{Shopify.Product.prefix}{product_id}',
                    'input': {'publicationId': creds.shopify_channel_inbox},
                }
                Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='publishablePublish'
                )
            if google:
                variables = {
                    'id': f'{Shopify.Product.prefix}{product_id}',
                    'input': {'publicationId': creds.shopify_channel_google},
                }
                Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='publishablePublish'
                )

        def get_collections(product_id: int):
            response = Shopify.Query(
                document=Shopify.Product.queries,
                variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                operation_name='product',
            )
            return [x['node'] for x in response.data['product']['collections']['edges']]

        def get_collection_ids(product_id: int):
            return [x['id'].split('/')[-1] for x in Shopify.Product.get_collections(product_id=8348458516647)]

        def get_product_id_from_sku(sku: str):
            query = f"""
            SELECT PRODUCT_ID FROM SN_SHOP_PROD
            WHERE ITEM_NO = '{sku}'
            OR BINDING_ID = '{sku}'
            """

            response = Database.query(query)

            try:
                return response[0][0]
            except:
                return None

        class Variant:
            queries = './integration/queries/productVariant.graphql'
            prefix = 'gid://shopify/ProductVariant/'

            def get(product_id):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables={'id': f'{Shopify.Product.Variant.prefix}{product_id}'},
                    operation_name='productVariant',
                )
                return response.data

            def parse_bulk_variant_response(operation_name, variables, response):
                result = {}

                for i in variables['variants']:
                    sku = i['inventoryItem']['sku']
                    name = i['optionValues']['name']
                    result[sku] = {
                        'variant_id': [
                            x['id'].split('/')[-1]
                            for x in response.data[operation_name]['productVariants']
                            if x['sku'] == sku
                        ][0],
                        'option_value_id': [
                            x['id'].split('/')[-1]
                            for x in response.data[operation_name]['product']['options'][0]['optionValues']
                            if x['name'] == name
                        ][0],
                        'inventory_id': [
                            x['inventoryItem']['id'].split('/')[-1]
                            for x in response.data[operation_name]['productVariants']
                            if x['sku'] == sku
                        ][0],
                        'has_image': True
                        if [
                            x['image']['id'].split('/')[-1]
                            for x in response.data[operation_name]['productVariants']
                            if x['sku'] == sku and x['image'] is not None
                        ]
                        else False,
                        'variant_meta_ids': [
                            {
                                'id': x['metafield']['id'].split('/')[-1],
                                'namespace': x['metafield']['namespace'],
                                'key': x['metafield']['key'],
                                'value': x['metafield']['value'],
                            }
                            for x in response.data[operation_name]['productVariants']
                            if x['sku'] == sku and x['metafield']
                        ],
                    }

                return result

            def create_bulk(variables):
                operation_name = 'productVariantsBulkCreate'
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries, operation_name=operation_name, variables=variables
                )
                return Shopify.Product.Variant.parse_bulk_variant_response(operation_name, variables, response)

            def update_single(variables):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables=variables,
                    operation_name='productVariantUpdate',
                )
                metafield = response.data['productVariantUpdate']['productVariant']['metafield']
                variant_meta_ids = []
                if metafield:
                    variant_meta_ids.append(
                        {
                            'id': metafield['id'].split('/')[-1],
                            'namespace': metafield['namespace'],
                            'key': metafield['key'],
                            'value': metafield['value'],
                        }
                    )

                return {'variant_meta_ids': variant_meta_ids}

            def update_bulk(variables):
                operation_name = 'productVariantsBulkUpdate'
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries, variables=variables, operation_name=operation_name
                )
                return Shopify.Product.Variant.parse_bulk_variant_response(operation_name, variables, response)

            def delete(product_id, variant_id: int):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables={
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                        'variantsIds': [f'{Shopify.Product.Variant.prefix}{variant_id}'],
                    },
                    operation_name='bulkDeleteProductVariants',
                )
                return response.data

            class Image:
                queries = './integration/queries/media.graphql'
                prefix = 'gid://shopify/MediaImage/'

                def get(variant_id, product_id):
                    if product_id:
                        # Get all variant images for a product
                        return

                def create(product_id: int, variant_data: list):
                    print(variant_data)
                    variables = {
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                        'variantMedia': [{'variantId': x['id'], 'mediaIds': x['imageId']} for x in variant_data],
                    }
                    response = Shopify.Query(
                        document=Shopify.Product.Variant.queries,
                        variables=variables,
                        operation_name='productVariantAppendMedia',
                    )
                    return response.data

                def delete(product_id: int, variant_id, shopify_id):
                    variables = {
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                        'variantMedia': [
                            {
                                'mediaIds': [f'{Shopify.Product.Variant.Image.prefix}{shopify_id}'],
                                'variantId': f'{Shopify.Product.Variant.prefix}{variant_id}',
                            }
                        ],
                    }

                    response = Shopify.Query(
                        document=Shopify.Product.Variant.Image.queries,
                        variables=variables,
                        operation_name='productVariantDetachMedia',
                    )
                    return response.data

                def delete_all(sku, product_id=None):
                    """Delete all Media Associated with Variant from shopify."""
                    if not product_id:
                        product_id = Database.Shopify.Product.get_id(sku=sku)

                    if product_id and sku:
                        media_ids = Database.Shopify.Product.Variant.Media.Image.get(item_no=sku)
                    else:
                        Shopify.error_handler.add_error_v(
                            error='Product ID or SKU not provided',
                            origin='Shopify.Product.Variant.Image.delete_all',
                            traceback=tb(),
                        )
                        return

                    if media_ids:
                        Shopify.Product.Media.delete(product_id=product_id, media_type='image', media_ids=media_ids)
                        for i in media_ids:
                            Database.Shopify.Product.Media.Image.delete(product_id=product_id, image_id=i)

            class Metafield:
                def get(variant_id: int):
                    response = Shopify.Query(
                        document=Shopify.Product.Variant.queries,
                        variables={'id': f'gid://shopify/ProductVariant/{variant_id}'},
                        operation_name='productVariantMetafields',
                    )
                    try:
                        return response.data['productVariant']['metafield']['id'].split('/')[-1]
                    except:
                        return None

        class Media:
            queries = './integration/queries/media.graphql'

            def get(product_id: int, id_list=True):
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                    operation_name='product',
                )
                if id_list:
                    return [x['id'] for x in response.data['product']['media']['nodes']]
                else:
                    return [x for x in response.data['product']['media']['nodes']]

            def reorder(product):
                if not product.reorder_media_queue:
                    return
                variables = {'id': f'{Shopify.Product.prefix}{product.product_id}', 'moves': []}
                for m in product.reorder_media_queue:
                    if m.type == 'IMAGE':
                        id = f'{Shopify.Product.Media.Image.prefix}{m.shopify_id}'
                    elif m.type == 'EXTERNAL_VIDEO':
                        id = f'{Shopify.Product.Media.Video.prefix}{m.shopify_id}'
                    else:
                        raise Exception(f'Invalid media type: {m.type}')

                    print(f'ID: {id}')

                    variables['moves'].append({'id': id, 'newPosition': str(m.sort_order)})

                response = Shopify.Query(
                    document=Shopify.Product.Media.queries,
                    variables=variables,
                    operation_name='productReorderMedia',
                )
                return response.data

            def delete(product_id: int, media_type, media_ids: list = None, media_id: int = None):
                variables = None

                if media_type == 'video':
                    prefix = Shopify.Product.Media.Video.prefix
                elif media_type == 'image':
                    prefix = Shopify.Product.Media.Image.prefix
                elif media_type == 'all':
                    prefix = ''
                else:
                    message = 'Must include valid media type: video or image or all. If all, do not include media_id or media_ids'
                    raise Exception(message)

                if media_id:
                    # Delete single media
                    variables = {
                        'mediaIds': [f'{prefix}{media_id}'],
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                    }
                elif media_ids:
                    # Delete multiple media
                    variables = {
                        'mediaIds': [f'{prefix}{x}' for x in media_ids],
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                    }
                else:
                    # Delete media...
                    media_ids = Shopify.Product.Media.get(product_id)
                    delete_list = []
                    if media_type == 'video':
                        # Delete all videos
                        prefix = Shopify.Product.Media.Video.prefix
                        for i in media_ids:
                            if i.startswith(prefix):
                                delete_list.append(i)
                    elif media_type == 'image':
                        # Delete all images
                        prefix = Shopify.Product.Media.Image.prefix
                        for i in media_ids:
                            if i.startswith(prefix):
                                delete_list.append(i)
                    else:
                        # Delete all media
                        delete_list = media_ids
                    if delete_list:
                        variables = {'mediaIds': delete_list, 'productId': f'{Shopify.Product.prefix}{product_id}'}
                        ProcessOutErrorHandler.logger.info(f'Deleting {len(delete_list)} media items')
                    else:
                        ProcessOutErrorHandler.logger.info('No media items to delete')
                        return
                if variables:
                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables=variables,
                        operation_name='productDeleteMedia',
                    )
                    return response.data

            class Image:
                prefix = 'gid://shopify/MediaImage/'

                def get(product_id: int):
                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                        operation_name='productMedia',
                    )

                    return [x['id'].split('/')[-1] for x in response.data['product']['media']['nodes']]

                def create(image):
                    variables = {'productId': f'{Shopify.Product.prefix}{image.product_id}'}
                    variables['media'] = {'originalSource': image.image_url, 'mediaContentType': 'IMAGE'}
                    if image.description:
                        variables['media']['alt'] = image.alt_text

                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables=variables,
                        operation_name='productCreateMedia',
                    )

                    return response.data['productCreateMedia']['media'][0]['id'].split('/')[-1]

                def update(image):
                    variables = {
                        'productId': f'{Shopify.Product.prefix}{image.product_id}',
                        'media': {'id': f'{Shopify.Product.Media.Image.prefix}{image.shopify_id}'},
                    }
                    if image.description:
                        variables['media']['alt'] = image.description

                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables=variables,
                        operation_name='productUpdateMedia',
                    )
                    return response.data

                def delete(image=None, product_id=None, shopify_id=None, variant_id=None):
                    variables = None
                    if image:
                        variables = {
                            'mediaIds': [f'{Shopify.Product.Media.Image.prefix}{image.shopify_id}'],
                            'productId': f'{Shopify.Product.prefix}{image.product_id}',
                        }

                    elif shopify_id and product_id:
                        variables = {
                            'mediaIds': [f'{Shopify.Product.Media.Image.prefix}{shopify_id}'],
                            'productId': f'{Shopify.Product.prefix}{product_id}',
                        }

                    elif product_id:
                        id_list = Shopify.Product.Media.Image.get(product_id)
                        if id_list:
                            variables = {
                                'mediaIds': [f'{Shopify.Product.Media.Image.prefix}{x}' for x in id_list],
                                'productId': f'{Shopify.Product.prefix}{product_id}',
                            }
                        else:
                            return
                    if not variables:
                        return
                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables=variables,
                        operation_name='productDeleteMedia',
                    )
                    return response.data

            class Video:
                prefix = 'gid://shopify/ExternalVideo/'

                def delete(product_id: int):
                    # Get all Media for product
                    media_ids = Shopify.Product.Media.get(product_id, id_list=False)
                    print(media_ids)

        class Metafield:
            def get(product_id):
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                    operation_name='productMeta',
                )
                if response.data['product']:
                    if 'metafields' in response.data['product']:
                        metafields = [x['node'] for x in response.data['product']['metafields']['edges']]
                product_specifications = []
                if metafields:
                    for x in metafields:
                        if x['namespace'] == Metafield.Namespace.Product.specification:
                            product_specifications.append(
                                {'id': x['id'].split('/')[-1], 'key': x['key'], 'value': x['value']}
                            )
                product_status = []
                for x in metafields:
                    if x['namespace'] == Metafield.Namespace.Product.status:
                        product_status.append({'id': x['id'].split('/')[-1], 'key': x['key'], 'value': x['value']})
                return {'product_specifications': product_specifications, 'product_status': product_status}

        class Option:
            queries = './integration/queries/productOptions.graphql'
            prefix = 'gid://shopify/ProductOption/'

            def update(
                product_id: int,
                option_id: int,
                option_values_to_add: list = None,
                option_values_to_update: list = None,
                option_values_to_delete: list = None,
            ):
                if product_id and option_id:
                    variables = {
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                        'option': {'id': f'{Shopify.Product.Option.prefix}{option_id}'},
                        'variantStrategy': 'MANAGE',
                        'optionValuesToDelete': [],
                        'optionValuesToUpdate': [],
                        'optionValuesToAdd': [],
                    }
                else:
                    raise Exception('Product ID and Option ID are required')

                if option_values_to_add:
                    add_list = [f'gid://shopify/ProductOptionValue/{x}' for x in option_values_to_add]
                    variables['optionValuesToAdd'] = add_list

                if option_values_to_update:
                    update_list = [option_values_to_update]
                    variables['optionValuesToUpdate'] = update_list

                if option_values_to_delete:
                    delete_list = [f'gid://shopify/ProductOptionValue/{x}' for x in option_values_to_delete]
                    variables['optionValuesToDelete'] = delete_list

                if option_values_to_add or option_values_to_update or option_values_to_delete:
                    response = Shopify.Query(
                        document=Shopify.Product.Option.queries, variables=variables, operation_name='updateOption'
                    )

                    return response.data
                else:
                    return

            def delete(product_id: str, option_ids: list):
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'productId': f'{Shopify.Product.prefix}{product_id}', 'options': option_ids},
                    operation_name='deleteOptions',
                )
                return response.data

            def reorder(product: object):
                """Reorder options for a Catalog.Product object"""
                variables = {
                    'productId': f'{Shopify.Product.prefix}{product.product_id}',
                    'options': [{'id': f'{Shopify.Product.Option.prefix}{product.option_id}', 'values': []}],
                }

                # Sort variants by price
                product.variants.sort(key=lambda x: x.price_1)

                for variant in product.variants:
                    variables['options'][0]['values'].append(
                        {'id': f'{Shopify.Product.OptionValue.prefix}{variant.option_value_id}'}
                    )

                response = Shopify.Query(
                    document=Shopify.Product.Option.queries, variables=variables, operation_name='reorderOptions'
                )
                return response.data

        class OptionValue:
            prefix = 'gid://shopify/ProductOptionValue/'

        class Files:
            queries = './integration/queries/files.graphql'

            class StagedMediaUploadTarget:
                class StagedUploadParameter:
                    def __init__(self, response_data):
                        self.name = response_data['name']
                        self.value = response_data['value']

                def __init__(self, response_data):
                    self.url = response_data['url']
                    self.resourceUrl = response_data['resourceUrl']
                    self.parameters = [
                        Shopify.Product.Files.StagedMediaUploadTarget.StagedUploadParameter(i)
                        for i in response_data['parameters']
                    ]

            def create(file_list, variables: dict) -> list:
                """Create staged media upload targets and upload files to google cloud storage. Return list of URLs"""
                response = Shopify.Query(
                    document=Shopify.Product.Files.queries,
                    variables=variables,
                    operation_name='stagedUploadsCreate',
                )
                files = [
                    Shopify.Product.Files.StagedMediaUploadTarget(i)
                    for i in response.data['stagedUploadsCreate']['stagedTargets']
                ]

                url_list = []
                i = 0
                # make POST requests to upload files and include all parameters in the request body
                for file in files:
                    form_data = {}
                    for param in file.parameters:
                        form_data[param.name] = param.value
                    file_path = Path(file_list[i])
                    with open(file_path, 'rb') as f:
                        response = requests.post(url=file.url, files={'file': f}, data=form_data)
                        if 200 <= response.status_code < 300:
                            print(f'File {file_path.name} uploaded successfully. Code: {response.status_code}')
                            url_list.append({'file_path': file_list[i], 'url': file.resourceUrl})
                            i += 1
                        else:
                            raise Exception(f'File {file_path.name} failed to upload')

                return url_list

        class SEO:
            def get(product_id: int):
                Shopify.logger.info(f'Getting SEO for product {product_id}')
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                    operation_name='SEO',
                )
                if response.data['product']:
                    if 'seo' in response.data['product']:
                        return response.data['product']['seo']

            def update(product_id: int, title=None, description=None):
                variables = {'input': {'id': f'{Shopify.Product.prefix}{product_id}', 'seo': {}}}
                if title:
                    variables['input']['seo']['title'] = title

                if description:
                    variables['input']['seo']['description'] = description

                if title or description:
                    response = Shopify.Query(
                        document=Shopify.Product.queries, variables=variables, operation_name='productSEOupdate'
                    )
                    return response.data

    class Inventory:
        queries = './integration/queries/inventory.graphql'

        def get_inventory_ids(variant_id):
            response = Shopify.Query(
                document=Shopify.Product.queries,
                variables={'id': f'{Shopify.Product.Variant.prefix}{variant_id}'},
                operation_name='variantIventoryId',
            )
            return response.data['productVariant']['inventoryItem']['id'].split('/')[-1]

        def update(payload: dict):
            response = Shopify.Query(
                document=Shopify.Inventory.queries, variables=payload, operation_name='inventorySetQuantities'
            )
            return response.data

    class Collection:
        queries = './integration/queries/collections.graphql'
        prefix = 'gid://shopify/Collection/'

        def get(collection_id: int = None, collection_handle=None):
            if collection_id:
                response = Shopify.Query(
                    document=Shopify.Collection.queries,
                    variables={'id': f'{Shopify.Collection.prefix}{collection_id}'},
                    operation_name='collection',
                    verbose=False,
                )
                return response.data

            elif collection_handle:
                response = Shopify.Query(
                    document=Shopify.Collection.queries,
                    variables={'handle': collection_handle},
                    operation_name='collectionByHandle',
                    verbose=False,
                )
                return response.data

            # Get all collections and return list of IDs
            response = Shopify.Query(document=Shopify.Collection.queries, operation_name='collections')
            return [
                {
                    'id': x['node']['id'].split('/')[-1],
                    'title': x['node']['title'],
                    'description': x['node']['descriptionHtml'],
                    'handle': x['node']['handle'],
                }
                for x in response.data['collections']['edges']
            ]

        def backfill_collections_to_counterpoint():
            """Backfill HTML descriptions for all collections presently on Shopify"""
            id_list = Shopify.Collection.get()
            for i in id_list:
                html_description = Shopify.Collection.get(i)['collection']['descriptionHtml']
                html_description = (
                    html_description.replace(' data-mce-fragment="1"', '').replace('<!---->', '').replace("'", "''")
                )
                if html_description:
                    Database.Collection.backfill_html_description(i, html_description)

        def create(payload: dict):
            response = Shopify.Query(
                document=Shopify.Collection.queries, variables=payload, operation_name='CollectionCreate'
            )
            collection_id = response.data['collectionCreate']['collection']['id'].split('/')[-1]
            return collection_id

        def update(payload: dict):
            response = Shopify.Query(
                document=Shopify.Collection.queries, variables=payload, operation_name='collectionUpdate'
            )
            return response.data

        def delete(collection_id: int):
            response = Shopify.Query(
                document=Shopify.Collection.queries,
                variables={'input': {'id': f'{Shopify.Collection.prefix}{collection_id}'}},
                operation_name='collectionDelete',
            )
            return response.data

        def get_product_count(collection_id: int):
            return Shopify.Collection.get(collection_id=collection_id)['collection']['productsCount']['count']

        def get_out_of_stock_items(collection_id: int, eh=ProcessOutErrorHandler):
            """Get a list of out of stock items in a collection"""
            eh.logger.info(f'Getting out of stock items for collection {collection_id}')
            try:
                response = None
                variables = {'collectionID': f'{Shopify.Collection.prefix}{collection_id}', 'after': None}

                data = []

                while response is None or response.data['products']['pageInfo']['hasNextPage']:
                    if response is not None:
                        variables['after'] = response.data['products']['pageInfo']['endCursor']

                    response = Shopify.Query(
                        document=Shopify.Product.queries, variables=variables, operation_name='outOfStockProducts'
                    )

                    for edge in response.data['products']['edges']:
                        if not edge['node']['inCollection']:
                            continue

                        data.append(edge['node']['id'].split('/')[-1])

                eh.logger.success(f'Found {len(data)} out of stock items in collection {collection_id}')
                return data
            except Exception as e:
                eh.error_handler.add_error_v(
                    error=f'Error getting out of stock items for collection {collection_id}: {e}',
                    origin='Shopify.Collection.get_out_of_stock_items',
                )
                return []

        def reorder_250_items(collection_id: int, moves: list[MoveInput], eh=ProcessOutErrorHandler):
            """Reorder up to 250 items within a collection. ONLY WORKS ON MANUALLY SORTED COLLECTIONS"""
            eh.logger.info(f'Reordering {len(moves)} items in collection {collection_id}')

            response = Shopify.Query(
                document=Shopify.Collection.queries,
                variables={
                    'id': f'{Shopify.Collection.prefix}{collection_id}',
                    'moves': [move.get() for move in moves],
                },
                operation_name='collectionReorderProducts',
            )
            return response.data

        def reorder_items(collection_id: int, collection_of_moves: MovesCollection, eh=ProcessOutErrorHandler):
            """Reorder any amount of items using a MovesCollection. ONLY WORKS ON MANUALLY SORTED COLLECTIONS"""
            responses = []

            list_of_moves = collection_of_moves.get()

            eh.logger.info(f'Reordering {len(list_of_moves)} items in collection {collection_id}')

            for moves in list_of_moves:
                data = Shopify.Collection.reorder_250_items(collection_id=collection_id, moves=moves, eh=eh)
                responses.append(data)

            return responses

        def move_to_bottom(collection_id: int, product_id_list: list[int], eh=ProcessOutErrorHandler):
            """Move a list of items to the bottom of a collection"""
            eh.logger.info(
                f'Setting up moves for {len(product_id_list)} items to bottom of collection {collection_id}'
            )

            count = Shopify.Collection.get_product_count(collection_id=collection_id)
            mc = MovesCollection()
            for i, product_id in enumerate(product_id_list):
                move = MoveInput(item_id=product_id, position=count - i)
                mc.add(move)

            eh.logger.success('Setting up moves complete.')

            return Shopify.Collection.reorder_items(collection_id=collection_id, collection_of_moves=mc, eh=eh)

        def move_all_out_of_stock_to_bottom(eh=ProcessOutErrorHandler):
            """Move all out of stock items to the bottom of all collections"""

            collections = [int(x['id']) for x in Shopify.Collection.get()]

            eh.logger.info(f'Moving all out of stock items to bottom of {len(collections)} collections')

            responses = []

            for i, collection_id in enumerate(collections):
                items = [
                    int(x) for x in Shopify.Collection.get_out_of_stock_items(collection_id=collection_id, eh=eh)
                ]

                if len(items) == 0:
                    eh.logger.info(f'No out of stock items found in collection {collection_id}')
                    eh.logger.success(f'{i + 1}/{len(collections)} collections processed')
                    continue

                eh.logger.info(f'Found {len(items)} out of stock items in collection {collection_id}')

                response = Shopify.Collection.move_to_bottom(
                    collection_id=collection_id, product_id_list=items, eh=eh
                )

                eh.logger.success(f'{i + 1}/{len(collections)} collections processed')

                responses.append(response)

            eh.logger.success(f'Moved all out of stock items to bottom of {len(collections)} collections')

            return responses

        def change_sort_order_to_manual(collection_id: int):
            """Change sort order to manual for a collection"""
            response = Shopify.Query(
                document=Shopify.Collection.queries,
                variables={'input': {'id': f'{Shopify.Collection.prefix}{collection_id}', 'sortOrder': 'MANUAL'}},
                operation_name='collectionUpdate',
            )
            return response.data

        class Files:
            queries = './integration/queries/files.graphql'

            class StagedMediaUploadTarget:
                class StagedUploadParameter:
                    def __init__(self, response_data):
                        self.name = response_data['name']
                        self.value = response_data['value']

                def __init__(self, response_data):
                    self.url = response_data['url']
                    self.resourceUrl = response_data['resourceUrl']
                    self.parameters = [
                        Shopify.Collection.Files.StagedMediaUploadTarget.StagedUploadParameter(i)
                        for i in response_data['parameters']
                    ]
                    for i in response_data['parameters']:
                        if i['name'] == 'key':
                            self.key = i['value']
                    self.public_url = self.resourceUrl + self.key

            def create(file_list, variables: dict) -> list:
                """Create staged media upload targets and upload files to google cloud storage. Return list of URLs"""
                response = Shopify.Query(
                    document=Shopify.Collection.Files.queries,
                    variables=variables,
                    operation_name='stagedUploadsCreate',
                )
                files = [
                    Shopify.Collection.Files.StagedMediaUploadTarget(i)
                    for i in response.data['stagedUploadsCreate']['stagedTargets']
                ]

                url_list = []
                i = 0
                # make POST requests to upload files and include all parameters in the request body
                for file in files:
                    form_data = {}
                    for param in file.parameters:
                        form_data[param.name] = param.value
                    file_path = Path(file_list[i])
                    with open(file_path, 'rb') as f:
                        response = requests.post(url=file.url, files={'file': f}, data=form_data)
                        if 200 <= response.status_code < 300:
                            print(f'File {file_path.name} uploaded successfully. Code: {response.status_code}')
                            url_list.append({'file_path': file_list[i], 'url': file.public_url})
                            i += 1
                        else:
                            raise Exception(f'File {file_path.name} failed to upload')

                return url_list

    class Menu:
        queries = './integration/queries/menus.graphql'
        prefix = 'gid://shopify/Menu/'

        def get(menu_id: int):
            response = Shopify.Query(
                document=Shopify.Menu.queries,
                variables={'id': f'{Shopify.Menu.prefix}{menu_id}'},
                operation_name='menu',
            )
            return response.data

        def get_all():
            response = Shopify.Query(document=Shopify.Menu.queries, operation_name='menus')
            return response.data

        def create(payload: dict):
            response = Shopify.Query(document=Shopify.Menu.queries, variables=payload, operation_name='CreateMenu')
            return response.data

        def update(payload: dict):
            response = Shopify.Query(document=Shopify.Menu.queries, variables=payload, operation_name='UpdateMenu')
            return response.data

    class Channel:
        queries = './integration/queries/channels.graphql'

        def get_all():
            response = Shopify.Query(document=Shopify.Channel.queries, operation_name='publications')
            return response.data

    class Metafield:
        queries = './integration/queries/metafields.graphql'
        prefix = 'gid://shopify/Metafield/'

        def backfill_metafields_to_counterpoint():
            all_products = Shopify.Product.get_all()
            for product_id in all_products:
                response = Shopify.Product.get(product_id)
                try:
                    metafield_list = [
                        {'id': x['node']['id'].split('/')[-1], 'key': x['node']['key']}
                        for x in response['product']['metafields']['edges']
                    ]
                except:
                    metafield_list = None

                if not metafield_list:
                    continue
                # need implementation here
                pass

        def get(owner_id):
            variables = {'id': owner_id}
            response = Shopify.Query(
                document=Shopify.Metafield.queries, variables=variables, operation_name='getMetafield'
            )
            return response.data

        def set(owner_id: int, namespace: str, key: str, value: str, type: str):
            variables = {
                'metafields': [
                    {'ownerId': owner_id, 'namespace': namespace, 'key': key, 'value': value, 'type': type}
                ]
            }
            response = Shopify.Query(
                document=Shopify.Metafield.queries, variables=variables, operation_name='MetafieldsSet'
            )
            return response.data

        def delete(
            metafield_id: int = None, product_id: int = None, customer_id: int = None, variant_id: int = None
        ):
            if metafield_id:
                target_id = f'{Shopify.Metafield.prefix}{metafield_id}'
                print(f'Target ID: {target_id}')
                variables = {'input': {'id': target_id}}

                response = Shopify.Query(
                    document=Shopify.Metafield.queries, variables=variables, operation_name='metafieldDelete'
                )
                Shopify.logger.info(f'Metafield {metafield_id} deleted from Shopify.')
                return response.data

            elif product_id:
                response = Shopify.Product.get(product_id)
                meta_ids = [x['node']['id'].split('/')[-1] for x in response['product']['metafields']['edges']]

            elif customer_id:
                # Get all metafield ids for this customer from shopify
                response = Shopify.Customer.Metafield.get(customer_id)
                meta_ids = [x['node']['id'].split('/')[-1] for x in response['customer']['metafields']['edges']]

            elif variant_id:
                response = Shopify.Product.Variant.Metafield.get(variant_id)
                if not response:
                    return
                meta_ids = [response]

            if meta_ids:
                for meta_id in meta_ids:
                    Shopify.Metafield.delete(metafield_id=meta_id)

    class MetafieldDefinition:
        queries = './integration/queries/metafields.graphql'
        prefix = 'gid://shopify/MetafieldDefinition/'

        def get(metafield_id: int = None):
            if metafield_id:
                response = Shopify.Query(
                    document=Shopify.MetafieldDefinition.queries,
                    variables={'id': f'{Shopify.MetafieldDefinition.prefix}{metafield_id}'},
                    operation_name='metafieldDefinition',
                )

                result = {
                    'META_ID': response.data['metafieldDefinition']['id'].split('/')[-1],
                    'NAME': response.data['metafieldDefinition']['name'],
                    'DESCR': response.data['metafieldDefinition']['description'],
                    'NAME_SPACE': response.data['metafieldDefinition']['namespace'],
                    'META_KEY': response.data['metafieldDefinition']['key'],
                    'TYPE': response.data['metafieldDefinition']['type']['name'],
                    'PINNED_POS': response.data['metafieldDefinition']['pinnedPosition'],
                    'OWNER_TYPE': response.data['metafieldDefinition']['ownerType'],
                    'VALIDATIONS': [
                        {'NAME': x['name'], 'TYPE': x['type'], 'VALUE': x['value']}
                        for x in response.data['metafieldDefinition']['validations']
                    ],
                }

                if not response.data['metafieldDefinition']['pinnedPosition']:
                    result['PINNED_POS'] = ''

                return result

            result = []
            owner_types = ['PRODUCT', 'CUSTOMER', 'PRODUCTVARIANT', 'PRODUCTIMAGE', 'MEDIA_IMAGE']
            for owner in owner_types:
                response = Shopify.Query(
                    document=Shopify.MetafieldDefinition.queries,
                    operation_name='metafieldDefinitions',
                    variables={'ownerType': owner},
                )
                owner_data = response.data['metafieldDefinitions']['edges']
                for x in owner_data:
                    result.append(
                        {
                            'META_ID': x['node']['id'].split('/')[-1],
                            'NAME': x['node']['name'],
                            'DESCR': x['node']['description'],
                            'NAME_SPACE': x['node']['namespace'],
                            'META_KEY': x['node']['key'],
                            'TYPE': x['node']['type']['name'],
                            'PIN': 1 if x['node']['pinnedPosition'] else 0,
                            'PINNED_POS': x['node']['pinnedPosition'] if x['node']['pinnedPosition'] else 0,
                            'OWNER_TYPE': x['node']['ownerType'],
                            'VALIDATIONS': [
                                {'NAME': y['name'], 'TYPE': y['type'], 'VALUE': y['value']}
                                for y in x['node']['validations']
                            ],
                        }
                    )
            return result

        def create(payload=None):
            if not payload:
                # Create all default metafields from Database and replace META_ID with new META_ID
                metafields = Database.Shopify.Metafield_Definition.get()
                for i in metafields:
                    if i['NAME_SPACE'] == 'custom':
                        variables = {
                            'definition': {
                                'name': i['NAME'].title(),
                                'description': i['DESCR'],
                                # 'namespace': i['NAME_SPACE'],
                                'namespace': 'product-status',
                                'key': i['META_KEY'].replace(' ', '_').lower(),
                                'type': i['TYPE'],
                                'pin': True if i['PIN'] == 1 else False,
                                'ownerType': i['OWNER_TYPE'],
                            }
                        }

                        variables['definition']['validations'] = [
                            {'name': v['NAME'], 'value': v['VALUE']} for v in i['VALIDATIONS'] if v['NAME']
                        ]
                        if i['PIN'] == 1:
                            variables['definition']['pinnedPosition'] = i['PINNED_POS']

                        try:
                            response = Shopify.Query(
                                document=Shopify.MetafieldDefinition.queries,
                                variables=variables,
                                operation_name='CreateMetafieldDefinition',
                            )
                        except:
                            print(f'Error creating {i["NAME"]}')
                            continue

                        metafield_id = response.data['metafieldDefinitionCreate']['createdDefinition']['id'].split(
                            '/'
                        )[-1]

                        update_values = {
                            'META_ID': metafield_id,
                            'NAME': i['NAME'],
                            'DESCR': i['DESCR'],
                            'NAME_SPACE': i['NAME_SPACE'],
                            'META_KEY': i['META_KEY'],
                            'TYPE': i['TYPE'],
                            'PIN': i['PIN'],
                            'PINNED_POS': i['PINNED_POS'],
                            'OWNER_TYPE': i['OWNER_TYPE'],
                        }
                        # This doesn't work yet...
                        # Database.Shopify.Metafield_Definition.update(i['META_ID'], update_values)

            else:
                # Create single metafield from payload
                response = Shopify.Query(
                    document=Shopify.MetafieldDefinition.queries,
                    variables=payload,
                    operation_name='CreateMetafieldDefinition',
                )
                metafield_id = response.data['metafieldDefinitionCreate']['createdDefinition']['id'].split('/')[-1]

                insert_values = {
                    'META_ID': metafield_id,
                    'NAME': payload['name'],
                    'DESCR': payload['description'],
                    'NAME_SPACE': 'test_data',
                    'META_KEY': payload['name'].replace(' ', '_').lower(),
                    'TYPE': payload['type'],
                    'PIN': 1,
                    'PINNED_POS': 0,
                    'OWNER_TYPE': 'PRODUCT',
                }

                Database.Shopify.Metafield_Definition.insert(insert_values)

        def delete(metafield_def_id: int = None):
            if metafield_def_id:
                # Delete single metafield from Shopify
                response = Shopify.Query(
                    document=Shopify.MetafieldDefinition.queries,
                    variables={
                        'id': f'{Shopify.MetafieldDefinition.prefix}{metafield_def_id}',
                        'deleteAllAssociatedMetafields': True,
                    },
                    operation_name='DeleteMetafieldDefinition',
                )
                return response.data
            else:
                # Delete all metafields from Shopify
                metafields = Database.Shopify.Metafield_Definition.get()
                for i in metafields:
                    Shopify.MetafieldDefinition.delete(i['META_ID'])

        def sync():
            # Delete all metafield definitions from Database
            Database.Shopify.Metafield_Definition.delete()
            # Get all metafields from Shopify and insert into Database
            response = Shopify.MetafieldDefinition.get()
            if response:
                for res in response:
                    Database.Shopify.Metafield_Definition.insert(res)

    class Webhook:
        queries = './integration/queries/webhooks.graphql'
        prefix = 'gid://shopify/WebhookSubscription'
        format = 'JSON'
        topics = [
            {'topic': 'ORDERS_CREATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.order_create}'},
            {'topic': 'REFUNDS_CREATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.refund_create}'},
            {'topic': 'DRAFT_ORDERS_CREATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.draft_create}'},
            {'topic': 'DRAFT_ORDERS_UPDATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.draft_update}'},
            {'topic': 'CUSTOMERS_CREATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.customer_create}'},
            {'topic': 'CUSTOMERS_UPDATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.customer_update}'},
            {'topic': 'PRODUCTS_UPDATE', 'url': f'{creds.Company.API.endpoint}{Route.Shopify.product_update}'},
            {
                'topic': 'VARIANTS_OUT_OF_STOCK',
                'url': f'{creds.Company.API.endpoint}{Route.Shopify.variant_out_of_stock}',
            },
        ]

        def get(id='', ids_only=False):
            if id:
                response = Shopify.Query(
                    document=Shopify.Webhook.queries,
                    variables={'id': f'{Shopify.Webhook.prefix}/{id}'},
                    operation_name='webhookSubscription',
                )
                return response.data
            else:
                response = Shopify.Query(document=Shopify.Webhook.queries, operation_name='webhookSubscriptions')
                if ids_only:
                    return [x['node']['id'].split('/')[-1] for x in response.data['webhookSubscriptions']['edges']]
                return response.data

        def create(topic=None, address=None) -> int:
            if topic and address:
                response = Shopify.Query(
                    document=Shopify.Webhook.queries,
                    variables={'topic': topic, 'webhookSubscription': address, 'format': Shopify.Webhook.format},
                    operation_name='webhookSubscriptionCreate',
                )

                Database.Shopify.Webhook.insert(
                    {
                        'TOPIC': topic,
                        'HOOK_ID': response.data['webhookSubscriptionCreate']['webhookSubscription']['id'].split(
                            '/'
                        )[-1],
                        'DESTINATION': address,
                        'FORMAT': Shopify.Webhook.format,
                        'DOMAIN': 'Shopify',
                    }
                )

                return response.data['webhookSubscriptionCreate']['webhookSubscription']['id'].split('/')[-1]

            # Create all default webhooks
            result = []

            for topic in Shopify.Webhook.topics:
                response = Shopify.Query(
                    document=Shopify.Webhook.queries,
                    variables={
                        'topic': topic['topic'],
                        'webhookSubscription': {'callbackUrl': topic['url'], 'format': Shopify.Webhook.format},
                    },
                    operation_name='webhookSubscriptionCreate',
                )
                result.append(
                    {
                        'TOPIC': topic['topic'],
                        'HOOK_ID': response.data['webhookSubscriptionCreate']['webhookSubscription']['id'].split(
                            '/'
                        )[-1],
                        'DESTINATION': topic['url'],
                        'FORMAT': Shopify.Webhook.format,
                        'DOMAIN': 'Shopify',
                    }
                )
            for i in result:
                Database.Shopify.Webhook.insert(i)
            return

            response = Shopify.Query(
                document=Shopify.Webhook.queries,
                variables={'topic': topic, 'address': address},
                operation_name='webhookSubscriptionCreate',
            )
            return response.data['webhookSubscriptionCreate']['webhookSubscription']['id'].split('/')[-1]

        def update(id, topic, address):
            response = Shopify.Query(
                document=Shopify.Webhook.queries,
                variables={'id': f'{Shopify.Webhook.prefix}/{id}', 'topic': topic, 'address': address},
                operation_name='webhookSubscriptionUpdate',
            )
            return response.data

        def delete(id: int = None):
            if id:
                response = Shopify.Query(
                    document=Shopify.Webhook.queries,
                    variables={'id': f'{Shopify.Webhook.prefix}/{id}'},
                    operation_name='webhookSubscriptionDelete',
                )
                Database.Shopify.Webhook.delete(id)
                return response.data
            # Delete all webhooks
            ids = Shopify.Webhook.get(ids_only=True)
            for i in ids:
                Shopify.Webhook.delete(i)

        def refresh():
            """Delete all webhooks and create all default webhooks"""
            # Delete all webhooks from Shopify
            Shopify.Webhook.delete()
            # Create all default webhooks
            Shopify.Webhook.create()

    class Discount:
        queries = './integration/queries/discounts.graphql'
        prefix = 'gid://shopify/DiscountCodeNode/'

        def get(discount_id=None):
            if discount_id:
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.prefix}{discount_id}'},
                    operation_name='discount',
                )
                return response.data
            else:
                response = Shopify.Query(
                    document=Shopify.Discount.queries, variables={'first': 250}, operation_name='discounts'
                )
                return response.data

        class Code:
            # Discounts that have a code
            def activate(discount_id):
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.prefix}{discount_id}'},
                    operation_name='discountCodeActivate',
                )
                return response.data

            def deactivate(discount_id):
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.prefix}{discount_id}'},
                    operation_name='discountCodeDeactivate',
                )
                return response.data

            def delete(discount_id: int):
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.prefix}{discount_id}'},
                    operation_name='discountCodeDelete',
                )
                return response.data

            class Basic:
                @staticmethod
                def create(
                    name,
                    amount,
                    min_purchase,
                    code,
                    max_uses,
                    expiration,
                    product_variants_to_add=[],
                    product_variants_to_remove=[],
                    products_to_add=[],
                    products_to_remove=[],
                    enabled=True,
                    eh=None,
                ):
                    if eh is None:
                        eh = Shopify.error_handler
                    try:
                        variables = {
                            'basicCodeDiscount': {
                                'appliesOncePerCustomer': True,
                                'code': code,
                                'combinesWith': {
                                    'orderDiscounts': False,
                                    'productDiscounts': False,
                                    'shippingDiscounts': False,
                                },
                                'customerGets': {
                                    'items': {
                                        'all': True,
                                        'products': {
                                            'productVariantsToAdd': product_variants_to_add,
                                            'productVariantsToRemove': product_variants_to_remove,
                                            'productsToAdd': products_to_add,
                                            'productsToRemove': products_to_remove,
                                        },
                                    },
                                    'value': {'discountAmount': {'amount': amount, 'appliesOnEachItem': False}},
                                },
                                'customerSelection': {'all': True},
                                'endsAt': local_to_utc(expiration).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                'minimumRequirement': {'subtotal': {'greaterThanOrEqualToSubtotal': min_purchase}},
                                'startsAt': local_to_utc(datetime.now()).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                'title': name,
                                'usageLimit': max_uses,
                            }
                        }

                        response = Shopify.Query(
                            document=Shopify.Discount.queries,
                            variables=variables,
                            operation_name='discountCodeBasicCreate',
                        )
                        id = response.data['discountCodeBasicCreate']['codeDiscountNode']['id']
                        discount_id = id.split('/')[-1]

                        eh.logger.success(f'Discount ID: {discount_id} created on Shopify')
                        return discount_id
                    except:
                        eh.add_error_v(f'Error creating discount: {variables}', origin='shopify_api.py')
                        return None

        class Automatic:
            # Discounts that are automatically applied
            prefix = 'gid://shopify/DiscountAutomaticNode/'

            def activate(discount_id):
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.Automatic.prefix}{discount_id}'},
                    operation_name='discountAutomaticActivate',
                )
                return response.data

            def deactivate(discount_id):
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.Automatic.prefix}{discount_id}'},
                    operation_name='discountAutomaticDeactivate',
                )
                return response.data

            def delete(discount_id):
                response = Shopify.Query(
                    document=Shopify.Discount.queries,
                    variables={'id': f'{Shopify.Discount.Automatic.prefix}{discount_id}'},
                    operation_name='discountAutomaticDelete',
                )
                Shopify.logger.success(f'Discount ID: {discount_id} deleted on Shopify')
                return response.data

            class Bxgy:
                # Buy X, Get Y Discounts that are applied automatically
                def create(variables):
                    if not variables:
                        return
                    response = Shopify.Query(
                        document=Shopify.Discount.queries,
                        variables=variables,
                        operation_name='discountAutomaticBxgyCreate',
                    )
                    promotion_id = response.data['discountAutomaticBxgyCreate']['automaticDiscountNode'][
                        'id'
                    ].split('/')[-1]
                    Shopify.logger.success(f'Promotion ID: {promotion_id} created on Shopify')
                    return promotion_id

                def update(variables):
                    if not variables:
                        return
                    response = Shopify.Query(
                        document=Shopify.Discount.queries,
                        variables=variables,
                        operation_name='discountAutomaticBxgyUpdate',
                    )
                    promotion_id = response.data['discountAutomaticBxgyUpdate']['automaticDiscountNode'][
                        'id'
                    ].split('/')[-1]
                    Shopify.logger.success(f'Promotion ID: {promotion_id} updated on Shopify')
                    return response.data


def refresh_order(tkt_no):
    """Delete order from PS_DOC_HDR and associated tables and rebuild from Shopify Data"""
    Database.Counterpoint.OpenOrder.delete(tkt_no=tkt_no)

    from integration.orders import Order as ShopifyOrder

    order_id = Shopify.Order.get_id_from_tkt_no(tkt_no[1:])
    if order_id:
        shopify_order = ShopifyOrder(order_id)
        shopify_order.post_shopify_order()


if __name__ == '__main__':
    verbose = True
    print(Shopify.Product.get(8308193460391))
