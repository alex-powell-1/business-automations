from setup import creds
import requests
import json
from setup.error_handler import ProcessOutErrorHandler
from pathlib import Path
from integration.database import Database
from shortuuid import ShortUUID
from setup.email_engine import Email

verbose_print = True


class Shopify:
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler
    token = creds.shopify_admin_token
    shop_url = creds.shopify_shop_url
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}

    def get_all_products(Shopify):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products.json'
        response = requests.get(url, headers=Shopify.headers)
        return response.json()

    def get_product(product_id: int):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
        response = requests.get(url, headers=Shopify.headers)
        return response.json()

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
                        if i == 'Key must be unique within this namespace on this resource':
                            product_id = variables['input']['id'].split('/')[-1]
                            Shopify.Product.Metafield.delete(product_id=product_id)
                            Database.Shopify.Product.Metafield.delete(product_id=product_id)
                            for error in self.user_errors:
                                if error == 'Key must be unique within this namespace on this resource':
                                    # Remove from user errors
                                    self.user_errors.remove(error)
                            # Re-run query
                            self.__init__(document, variables, operation_name)
                        if i == 'Metafield does not exist':
                            pass
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

            shopify_products = []

            for _item in shopify_order['node']['lineItems']['edges']:
                item = _item['node']

                def get_money(money: dict):
                    return money['presentmentMoney']['amount']

                price = float(get_money(item['originalTotalSet']))

                item['isGiftCard'] = False

                if item['sku'] is not None:
                    item['isGiftCard'] = 'GFC' in item['sku']

                if item['name'] is None:
                    item['name'] = ''

                pl = {
                    'id': item['id'],
                    'sku': 'SERVICE' if item['name'].lower() == 'service' else item['sku'],
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
                    'is_refunded': False,
                    'quantity_refunded': 0,
                    'refund_amount': 0,
                    'return_id': 0,
                    'fixed_shipping_cost': 0,
                    'gift_certificate_id': None,
                    'discounted_total_inc_tax': get_money(item['discountedTotalSet']),
                    'applied_discounts': [],
                }

                is_refunded = False
                quantity_refunded = 0

                if len(snode['refunds']) > 0:
                    for refunds in snode['refunds']:
                        for refund in refunds['refundLineItems']['edges']:
                            if refund['node']['lineItem']['id'] == item['id']:
                                is_refunded = True
                                quantity_refunded = int(refund['node']['quantity'])

                pl['is_refunded'] = is_refunded
                pl['quantity_refunded'] = quantity_refunded

                def send_gift_card():
                    email = snode['email']
                    print('Email: ', email)

                    name = snode['billingAddress']['firstName'] + ' ' + snode['billingAddress']['lastName']
                    code = pl['gift_certificate_id']['code']
                    Email.Customer.GiftCard.send(name=name, email=email, gc_code=code, amount=price)

                if item['isGiftCard'] and snode['displayFulfillmentStatus'] == 'UNFULFILLED':

                    def has_code(code):
                        query = f"""
                        SELECT GFC_NO FROM SY_GFC
                        WHERE GFC_NO = '{code}'
                        """

                        response = Database.db.query(query)
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
                    if send and not is_refunded:
                        send_gift_card()

                shopify_products.append(pl)

            def get_money(money: dict):
                return money['presentmentMoney']['amount']

            try:
                shippingCost = float(get_money(snode['shippingLine']['discountedPriceSet']))
            except:
                shippingCost = 0

            hdsc = float(get_money(snode['totalDiscountsSet']))

            subtotal = float(get_money(snode['currentSubtotalPriceSet'])) + hdsc - shippingCost
            total = float(get_money(snode['currentTotalPriceSet']))

            status = snode['displayFulfillmentStatus']

            if len(snode['refunds']) > 0:
                status = 'Partially Refunded'

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
                'items_total': snode['subtotalLineItemsQuantity'],
                'items_shipped': 0,  # TODO: Add items shipped
                'payment_method': None,  # TODO: Add payment method
                'payment_status': snode['displayFinancialStatus'],
                'refunded_amount': '0.0000',  # TODO: Add refunded amount
                'store_credit_amount': '0.0000',  # TODO: Add store credit amount
                'gift_certificate_amount': '0.0000',  # TODO: Add gift certificate amount
                'customer_message': snode['note'],
                'discount_amount': '0.0000',  # TODO: Add discount amount
                'coupon_discount': '0.0000',  # TODO: Add coupon discount
                'shipping_address_count': 1,  # TODO: Add shipping address count
                'billing_address': {
                    'first_name': snode['billingAddress']['firstName'],
                    'last_name': snode['billingAddress']['lastName'],
                    'company': snode['billingAddress']['company'],
                    'street_1': snode['billingAddress']['address1'],
                    'street_2': snode['billingAddress']['address2'],
                    'city': snode['billingAddress']['city'],
                    'state': snode['billingAddress']['province'],
                    'zip': snode['billingAddress']['zip'],
                    'country': snode['billingAddress']['country'],
                    'phone': snode['billingAddress']['phone'],
                    'email': snode['email'],
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
                            'phone': (snode['shippingAddress'] or {'phone': None})['phone'],
                            'email': snode['email'],
                        }
                    ]
                },
                'coupons': {'url': []},
                'transactions': {'data': []},
            }

            if hdsc > 0:
                bc_order['coupons']['url'] = [{'amount': hdsc}]

            # transactions = []

            # for transaction in snode['transactions']:
            #     amount = float(get_money(transaction['amountSet']))

            #     if transaction['gateway'] == 'gift_card':
            #         transaction['gateway'] = 'gift_certificate'

            #     transactions.append(
            #         {
            #             'method': transaction['gateway'],
            #             'amount': amount,
            #             'gift_certificate': {'code': 'ABC123', 'remaining_balance': 0},
            #         }
            #     )

            # bc_order['transactions']['data'] = transactions

            return bc_order

        def get_orders_not_in_cp():
            query = """
            SELECT TKT_NO FROM PS_DOC_HDR WHERE STR_ID = 'WEB'
            """

            response = Database.db.query(query)
            try:
                tkt_nos = [x[0].replace('S', '') for x in response]

                print(tkt_nos)

                orders = []

                for _order in Shopify.Order.get_all()['orders']['edges']:
                    order = _order['node']

                    if order['name'] not in tkt_nos:
                        orders.append(order)

                return orders

            except:
                return []

        @staticmethod
        def create_gift_card(balance: float):
            input = {'initialValue': balance}

            response = Shopify.Query(
                document=Shopify.Order.queries, operation_name='giftCardCreate', variables={'input': input}
            )

            if response.errors or response.user_errors:
                raise Exception(f'Error creating gift card: {response.errors}\nUser Errors: {response.user_errors}')

            return response.data

    class Customer:
        queries = './integration/queries/customers.graphql'
        prefix = 'gid://shopify/Customer/'

        def get(customer_id: int = None, all=False):
            if customer_id:
                variables = {'id': f'gid://shopify/Customer/{customer_id}'}
                response = Shopify.Query(
                    document=Shopify.Customer.queries, variables=variables, operation_name='customer'
                )
                return response.data
            elif all:
                id_list = []
                variables = {'first': 250}
                response = Shopify.Query(
                    document=Shopify.Customer.queries, variables=variables, operation_name='customers'
                )
                id_list += [x['node']['id'].split('/')[-1] for x in response.data['customers']['edges']]

                while response.data['customers']['pageInfo']['hasNextPage']:
                    variables['after'] = response.data['customers']['pageInfo']['endCursor']
                    response = Shopify.Query(
                        document=Shopify.Customer.queries, variables=variables, operation_name='customers'
                    )
                    id_list += [x['node']['id'].split('/')[-1] for x in response.data['customers']['edges']]

                return id_list

        def create(payload):
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=payload, operation_name='customerCreate'
            )
            return response.data['customerCreate']['customer']['id'].split('/')[-1]

        def update(customer_id: int, fst_name, lst_name, addresses=None, phone=None, email=None):
            variables = Shopify.Customer.get_customer_payload(
                cp_cust_no=None,
                f_nam=fst_name,
                l_nam=lst_name,
                sh_cust_no=customer_id,
                addr_lst=addresses,
                phone=phone,
                email=email,
            )
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=variables, operation_name='customerUpdate'
            )
            return response.data

        def delete(customer_id: int = None, all=False):
            if customer_id:
                response = Shopify.Query(
                    document=Shopify.Customer.queries,
                    variables={'id': f'gid://shopify/Customer/{customer_id}'},
                    operation_name='customerDelete',
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

        class StoreCredit:
            queries = './integration/queries/storeCredit.graphql'

            def get(customer_id: int):
                variables = {'id': f'gid://shopify/Customer/{customer_id}'}
                response = Shopify.Query(
                    document=Shopify.Customer.StoreCredit.queries,
                    variables=variables,
                    operation_name='storeCreditAccount',
                )
                return response.data

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
                return response.data

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
                return response.data

    class Product:
        queries = './integration/queries/products.graphql'
        prefix = 'gid://shopify/Product/'

        def get(product_id: int = None):
            if product_id:
                variables = {'id': f'{Shopify.Product.prefix}{product_id}'}
                response = Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='product'
                )
                return response.data

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
                'inventory_ids': inventory_ids,
                'meta_ids': meta_ids,
            }
            return result

        def update(product_payload):
            response = Shopify.Query(
                document=Shopify.Product.queries,
                operation_name='UpdateProductWithNewMedia',
                variables=product_payload,
            )
            if response.errors or response.user_errors:
                raise Exception(
                    f'Error: {response.errors}\nUser Error: {response.user_errors}\nPayload: {product_payload}'
                )
            # result = {}
            # for i in product_payload['media']:
            #     # Get the image name from the payload and then match it with the parsed response URL
            #     pass

            media_ids = [
                x['id'].split('/')[-1] for x in response.data['productUpdate']['product']['media']['nodes']
            ]
            option_ids = [x['id'].split('/')[-1] for x in response.data['productUpdate']['product']['options']]
            meta_ids = [
                {'id': x['node']['id'].split('/')[-1], 'namespace': x['node']['namespace'], 'key': x['node']['key']}
                for x in response.data['productUpdate']['product']['metafields']['edges']
            ]

            return {'media_ids': media_ids, 'option_ids': option_ids, 'meta_ids': meta_ids}

        def delete(product_id: int = None, all=False):
            if product_id:
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                    operation_name='productDelete',
                )
                return response.data
            elif all:
                id_list = Shopify.Product.get(all=True)
                for i in id_list:
                    response = Shopify.Query(
                        document=Shopify.Product.queries,
                        variables={'id': f'{Shopify.Product.prefix}{i}'},
                        operation_name='productDelete',
                    )
                    print(response.data)

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

        class Variant:
            queries = './integration/queries/productVariant.graphql'
            prefix = 'gid://shopify/ProductVariant/'

            def get(product_id):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables={'id': f'{Shopify.Product.prefix}{product_id}'},
                    operation_name='productVariants',
                )
                return response.data

            def create_bulk(variables):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    operation_name='productVariantsBulkCreate',
                    variables=variables,
                )

                variant_ids = [
                    x['id'].split('/')[-1] for x in response.data['productVariantsBulkCreate']['productVariants']
                ]
                option_value_ids = [
                    x['id'].split('/')[-1]
                    for x in response.data['productVariantsBulkCreate']['product']['options'][0]['optionValues']
                ]
                inventory_ids = [
                    x['inventoryItem']['id'].split('/')[-1]
                    for x in response.data['productVariantsBulkCreate']['productVariants']
                ]
                return {
                    'variant_ids': variant_ids,
                    'option_value_ids': option_value_ids,
                    'inventory_ids': inventory_ids,
                }

            def update_single(variables):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables=variables,
                    operation_name='productVariantUpdate',
                )
                return response.data

            def update_bulk(variables):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables=variables,
                    operation_name='productVariantsBulkUpdate',
                )

                result = {}
                for i in variables['variants']:
                    sku = i['inventoryItem']['sku']
                    name = i['optionValues']['name']
                    result[sku] = {
                        'variant_id': [
                            x['id'].split('/')[-1]
                            for x in response.data['productVariantsBulkUpdate']['productVariants']
                            if x['sku'] == sku
                        ][0],
                        'option_value_id': [
                            x['id'].split('/')[-1]
                            for x in response.data['productVariantsBulkUpdate']['product']['options'][0][
                                'optionValues'
                            ]
                            if x['name'] == name
                        ][0],
                        # 'image_id': [
                        #     x['image']['id'].split('/')[-1]
                        #     for x in response.data['productVariantsBulkUpdate']['productVariants']
                        #     if x['sku'] == sku and x['image'] is not None
                        # ],
                        'has_image': True
                        if [
                            x['image']['id'].split('/')[-1]
                            for x in response.data['productVariantsBulkUpdate']['productVariants']
                            if x['sku'] == sku and x['image'] is not None
                        ]
                        else False,
                    }

                # variant_ids = [
                #     x['id'].split('/')[-1] for x in response.data['productVariantsBulkUpdate']['productVariants']
                # ]
                # option_value_ids = [
                #     x['id'].split('/')[-1]
                #     for x in response.data['productVariantsBulkUpdate']['product']['options'][0]['optionValues']
                # ]
                # image_ids = [
                #     x['image']['id'].split('/')[-1]
                #     for x in response.data['productVariantsBulkUpdate']['productVariants']
                #     if x['image'] is not None
                # ]

                # result = {'option_value_ids': option_value_ids, 'variant_ids': variant_ids, 'image_ids': image_ids}
                print(result)
                return result

            def delete(variant_id: int):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables={'id': f'{Shopify.Product.Variant.prefix}{variant_id}'},
                    operation_name='productVariantDelete',
                )
                return response.data

            class Image:
                queries = './integration/queries/media.graphql'
                prefix = 'gid://shopify/MediaImage/'

                def get(variant_id: int = None, product_id: int = None):
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

                def delete(product_id: int, variant_id, image_id):
                    variables = {
                        'productId': f'{Shopify.Product.prefix}{product_id}',
                        'variantMedia': [
                            {
                                'mediaIds': [f'{Shopify.Product.Variant.Image.prefix}{image_id}'],
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

        class Media:
            queries = './integration/queries/media.graphql'

            def reorder(product):
                variables = {'id': f'{Shopify.Product.prefix}{product.product_id}', 'moves': []}
                for image in product.images:
                    print(f'Image Name: {image.name}, Image ID: {image.image_id}, Sort Order: {image.sort_order}')
                    variables['moves'].append(
                        {
                            'id': f'{Shopify.Product.Media.Image.prefix}{image.image_id}',
                            'newPosition': str(image.sort_order + 1),
                        }
                    )
                response = Shopify.Query(
                    document=Shopify.Product.Media.queries,
                    variables=variables,
                    operation_name='productReorderMedia',
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
                        'media': {'id': f'{Shopify.Product.Media.Image.prefix}{image.image_id}'},
                    }
                    if image.description:
                        variables['media']['alt'] = image.description

                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables=variables,
                        operation_name='productUpdateMedia',
                    )
                    return response.data

                def delete(image=None, product_id=None, image_id=None, variant_id=None):
                    if image:
                        variables = {
                            'mediaIds': [f'{Shopify.Product.Media.Image.prefix}{image.image_id}'],
                            'productId': f'{Shopify.Product.prefix}{image.product_id}',
                        }

                    elif image_id and product_id:
                        variables = {
                            'mediaIds': [f'{Shopify.Product.Media.Image.prefix}{image_id}'],
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

                    response = Shopify.Query(
                        document=Shopify.Product.Media.queries,
                        variables=variables,
                        operation_name='productDeleteMedia',
                    )
                    return response.data

            class Video:
                pass

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
                variables = {
                    'productId': f'{Shopify.Product.prefix}{product_id}',
                    'option': {'id': f'{Shopify.Product.Option.prefix}{option_id}'},
                    'variantStrategy': 'MANAGE',
                    'optionValuesToDelete': [],
                    'optionValuesToUpdate': [],
                    'optionValuesToAdd': [],
                }

                if option_values_to_add:
                    add_list = [f'gid://shopify/ProductOptionValue/{x}' for x in option_values_to_add]
                    variables['optionValuesToAdd'] = add_list

                if option_values_to_update:
                    update_list = [f'gid://shopify/ProductOptionValue/{x}' for x in option_values_to_update]
                    variables['optionValuesToUpdate'] = update_list

                if option_values_to_delete:
                    delete_list = [f'gid://shopify/ProductOptionValue/{x}' for x in option_values_to_delete]
                    variables['optionValuesToDelete'] = delete_list

                response = Shopify.Query(
                    document=Shopify.Product.Option.queries, variables=variables, operation_name='updateOption'
                )

                return response.data

            def delete(product_id: str, option_ids: list):
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'productId': f'{Shopify.Product.prefix}{product_id}', 'options': option_ids},
                    operation_name='deleteOptions',
                )
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
                    document=Shopify.Product.Metafield.queries, variables=variables, operation_name='getMetafield'
                )
                return response.data

            def set(owner_id: int, namespace: str, key: str, value: str, type: str):
                variables = {
                    'metafields': [
                        {'ownerId': owner_id, 'namespace': namespace, 'key': key, 'value': value, 'type': type}
                    ]
                }
                response = Shopify.Query(
                    document=Shopify.Product.Metafield.queries, variables=variables, operation_name='MetafieldsSet'
                )
                return response.data

            def delete(metafield_id: int = None, product_id: int = None):
                if metafield_id:
                    variables = {'input': {'id': f'{Shopify.Product.Metafield.prefix}{metafield_id}'}}

                    response = Shopify.Query(
                        document=Shopify.Product.Metafield.queries,
                        variables=variables,
                        operation_name='metafieldDelete',
                    )
                    return response.data
                elif product_id:
                    response = Shopify.Product.get(product_id)
                    variables = {
                        'metafields': [
                            # {
                            #     'ownerId': x['node']['id'],
                            #     'namespace': x['node']['namespace'],
                            #     'key': x['node']['key'],
                            # }
                            x['node']['id']
                            for x in response['product']['metafields']['edges']
                        ]
                    }
                    # # remove variables with global namespace
                    # variables['metafields'] = [x for x in variables['metafields'] if x['namespace'] != 'global']
                    # response = Shopify.Query(
                    #     document=Shopify.Product.Metafield.queries,
                    #     variables=variables,
                    #     operation_name='metafieldsDelete',
                    # )
                    for i in variables['metafields']:
                        Shopify.Product.Metafield.delete(metafield_id=i.split('/')[-1])

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

        def get(collection_id: int):
            response = Shopify.Query(
                document=Shopify.Collection.queries,
                variables={'id': f'{Shopify.Collection.prefix}{collection_id}'},
                operation_name='collection',
                verbose=False,
            )
            return response.data

        def get_all():
            response = Shopify.Query(document=Shopify.Collection.queries, operation_name='collections')
            id_list = [x['node']['id'].split('/')[-1] for x in response.data['collections']['edges']]
            return id_list

        def backfill_collections_to_counterpoint():
            """Backfill HTML descriptions for all collections presently on Shopify"""
            id_list = Shopify.Collection.get_all()
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

            response = Shopify.Query(
                document=Shopify.MetafieldDefinition.queries, operation_name='metafieldDefinitions'
            )
            return [
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
                for x in response.data['metafieldDefinitions']['edges']
            ]

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
                    'NAME': i['name'],
                    'NAME_SPACE': 'test_data',
                    'META_KEY': i['name'].replace(' ', '_').lower(),
                    'TYPE': i['type'],
                    'PIN': 1,
                    'PINNED_POS': 0,
                    'OWNER_TYPE': 'PRODUCT',
                }

                Database.Metafield_Definition.insert(insert_values)

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
            for res in response:
                Database.Shopify.Metafield_Definition.insert(res)

    class Webhook:
        queries = './integration/queries/webhooks.graphql'
        prefix = 'gid://shopify/WebhookSubscription'
        format = 'JSON'
        topics = ['ORDERS_CREATE', 'REFUNDS_CREATE']
        address = f'{creds.ngrok_domain}/shopify'

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

        def create(topic=None, address=None, default=False) -> int:
            if default:
                # Create all default webhooks
                result = []
                for topic in Shopify.Webhook.topics:
                    response = Shopify.Query(
                        document=Shopify.Webhook.queries,
                        variables={
                            'topic': topic,
                            'webhookSubscription': {
                                'callbackUrl': Shopify.Webhook.address,
                                'format': Shopify.Webhook.format,
                            },
                        },
                        operation_name='webhookSubscriptionCreate',
                    )
                    result.append(
                        {
                            'TOPIC': topic,
                            'HOOK_ID': response.data['webhookSubscriptionCreate']['webhookSubscription'][
                                'id'
                            ].split('/')[-1],
                            'DESTINATION': Shopify.Webhook.address,
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

        def delete(id='', all=False):
            if all:
                ids = Shopify.Webhook.get(ids_only=True)
                for i in ids:
                    Shopify.Webhook.delete(i)
            else:
                response = Shopify.Query(
                    document=Shopify.Webhook.queries,
                    variables={'id': f'{Shopify.Webhook.prefix}/{id}'},
                    operation_name='webhookSubscriptionDelete',
                )
                Database.Shopify.Webhook.delete(id)
                return response.data

    class Promotion:
        queries = './integration/queries/promotion.graphql'


if __name__ == '__main__':
    # all_ids = Shopify.Product.get()
    # for product in all_ids:
    #     Shopify.Product.publish(product)

    Shopify.Order.get(5588776222887)
