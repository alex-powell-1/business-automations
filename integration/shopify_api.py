from setup import creds
import requests
import json
from setup.error_handler import ProcessOutErrorHandler
import re
from pathlib import Path
from integration.database import Database


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
                raise Exception(
                    f'Operation Name: {operation_name}\n\nError: {self.errors}\n\nUser Error: {self.user_errors}\n\nVariables: {variables}'
                )
            if verbose:
                print(operation_name, self)

        def __str__(self):
            return json.dumps(self.response, indent=4)

        def execute_query(self, document, variables=None, operation_name=None):
            query_doc = Path(document).read_text()
            endpoint = f'https://{Shopify.shop_url}/admin/api/2024-07/graphql.json'
            payload = {'query': query_doc, 'variables': variables, 'operationName': operation_name}
            response = requests.post(endpoint, headers=Shopify.headers, json=payload)
            return response.json()

    class Customer:
        queries = './integration/queries/customers.graphql'

        def get_customer_payload(
            cp_cust_no, f_nam, l_nam, sh_cust_no: int, addr_lst: list = None, phone=None, email=None
        ):
            variables = {
                'input': {
                    'firstName': f_nam,
                    'lastName': l_nam,
                    'addresses': [],
                    'metafields': [
                        {
                            'namespace': 'counterpoint',
                            'key': 'customer_number',
                            'type': 'single_line_text_field',
                            'value': cp_cust_no,
                        }
                    ],
                }
            }
            # Add optional fields if they are provided
            if sh_cust_no:
                variables['input']['id'] = f'gid://shopify/Customer/{sh_cust_no}'

            if email:
                variables['input']['email'] = email
                variables['input']['emailMarketingConsent'] = {'marketingState': 'SUBSCRIBED'}

            if phone:
                variables['input']['phone'] = phone

            if addr_lst:
                for i in addr_lst:
                    variables['input']['addresses'].append(
                        {
                            'address1': i['address'],
                            'city': i['city'],
                            'phone': i['phone'],
                            'provinceCode': i['state'],
                            'zip': i['zip'],
                            'lastName': i['last_name'],
                            'firstName': i['first_name'],
                            'country': i['country'],
                        }
                    )

            return variables

        def get(customer_id: int):
            variables = {'id': f'gid://shopify/Customer/{customer_id}'}
            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=variables, operation_name='customer'
            )
            return response.data

        def create(customer_number, fst_name, lst_name, address_list=None, phone=None, email=None):
            variables = Shopify.Customer.get_customer_payload(
                cp_cust_no=customer_number,
                f_nam=fst_name,
                l_nam=lst_name,
                sh_cust_no=None,
                addr_lst=address_list,
                phone=phone,
                email=email,
            )

            response = Shopify.Query(
                document=Shopify.Customer.queries, variables=variables, operation_name='customerCreate'
            )
            # return new customer ID
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

        def delete(customer_id: int):
            response = Shopify.Query(
                document=Shopify.Customer.queries,
                variables={'id': f'gid://shopify/Customer/{customer_id}'},
                operation_name='customerDelete',
            )
            return response.data

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

        def get(product_id: int):
            variables = {'id': f'gid://shopify/Product/{product_id}'}
            response = Shopify.Query(
                document=Shopify.Product.queries, variables=variables, operation_name='product'
            )
            return response.data

        def create_bound_product(product_payload, variant_payload) -> tuple:
            # Step 1: Create base product and associated media and dummy option.
            prod_id, media_ids, option_ids, option_value_ids, variant_ids = Shopify.Product.create(
                product_payload, variant_payload
            )
            # Step 2: Create new variants in bulk.
            #   a) Assign product ID to variant payload.
            variant_payload['productId'] = f'gid://shopify/Product/{prod_id}'
            variant_ids, option_value_ids = Shopify.Product.Variant.create_bulk(variant_payload)

            # Step 3: Delete the default variant
            Shopify.Product.Option.delete(product_id=prod_id, option_ids=option_ids[0])

            return {
                'product_id': prod_id,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'media_ids': media_ids,
                'variant_ids': variant_ids,
            }

        def create_single_product(product_payload, variant_payload, inventory_payload) -> tuple:
            # Step 1: Create base product and associated media.
            base_product_response = Shopify.Product.create(product_payload)

            # Step 2: Update the price, sku, etc of the default variant.

            # sample_variant_payload = {
            #     'input': {
            #         'position': 1,
            #         'price': '100.00',
            #         'compareAtPrice': '105.00',
            #         'inventoryPolicy': 'DENY',
            #         'inventoryQuantities': [{'locationId': creds.shopify_location_id, 'availableQuantity': 10}],
            #         'inventoryItem': {
            #             'sku': '1234',
            #             'tracked': True,
            #             'cost': 19.99,
            #             'measurement': {'weight': {'value': 10, 'unit': 'POUNDS'}},
            #             'requiresShipping': False,
            #         },
            #         'taxable': False,
            #     }
            # }

            default_variant_id = base_product_response['variant_ids'][0]

            variant_payload['input']['id'] = f'gid://shopify/ProductVariant/{default_variant_id}'

            update_base_response = Shopify.Query(
                document=Shopify.Product.queries, operation_name='productVariantUpdate', variables=variant_payload
            )

            return {
                'product_id': prod_id,
                'media_ids': media_ids,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'variant_ids': variant_ids,
                'inventory_item_ids': inventory_item_ids,
            }

        def create_base_product(product_payload) -> tuple:
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

            result = {
                'product_id': prod_id,
                'media_ids': media_ids,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'variant_ids': variant_ids,
                'inventory_ids': inventory_ids,
            }
            return result

        def update(product_payload, variant_payload):
            # Step 1: Update Product
            operation_name = 'productUpdate'
            response = Shopify.Query(
                document=Shopify.Product.queries, operation_name=operation_name, variables=product_payload
            )
            if response.errors or response.user_errors:
                raise Exception(
                    f'Error: {response.errors}\nUser Error: {response.user_errors}\nPayload: {product_payload}'
                )

            print('\nProduct update', response)

            prod_id = response.data[operation_name]['product']['id'].split('/')[-1]
            media_ids = [x['id'].split('/')[-1] for x in response.data[operation_name]['product']['media']['nodes']]
            option_ids = [x['id'].split('/')[-1] for x in response.data[operation_name]['product']['options']]

            # Step 2: Update Variants
            operation_name = 'productVariantsBulkUpdate'
            response = Shopify.Query(
                document=Shopify.Product.queries, operation_name=operation_name, variables=variant_payload
            )

            variant_ids = [x['id'].split('/')[-1] for x in response.data[operation_name]['productVariants']]
            option_value_ids = [
                x['id'].split('/')[-1]
                for x in response.data[operation_name]['product']['options'][0]['optionValues']
            ]

            return {
                'product_id': prod_id,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'media_ids': media_ids,
                'variant_ids': variant_ids,
            }

        def delete(product_id: int):
            url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
            response = requests.delete(url, headers=Shopify.headers)
            return response.json()

        def publish(product_id: int):
            # variables = {
            #     'input': {
            #         'id': f'gid://shopify/Product/{product_id}',
            #         'productPublications': [{'publicationId': creds.shopify_online_store_channel_id}],
            #     }
            # }

            # response = Shopify.Query(
            #     document=Shopify.Product.queries, variables=variables, operation_name='productPublish'
            # )
            # return response.data
            variables = {
                'id': f'gid://shopify/Product/{product_id}',
                'input': {'publicationId': creds.shopify_online_store_channel_id},
            }
            response = Shopify.Query(
                document=Shopify.Product.queries, variables=variables, operation_name='publishablePublish'
            )
            return response.data

        class Variant:
            queries = './integration/queries/productVariant.graphql'

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

            def delete(variant_id: int):
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables={'id': f'gid://shopify/ProductVariant/{variant_id}'},
                    operation_name='productVariantDelete',
                )
                return response.data

            def add_images(product_id: int, variant_data: list):
                print(variant_data)
                variables = {
                    'productId': f'gid://shopify/Product/{product_id}',
                    'variantMedia': [{'variantId': x['id'], 'mediaIds': x['imageId']} for x in variant_data],
                }
                response = Shopify.Query(
                    document=Shopify.Product.Variant.queries,
                    variables=variables,
                    operation_name='productVariantAppendMedia',
                )
                return response.data

        class Option:
            queries = './integration/queries/productOption.graphql'

            def update(
                product_id: int,
                option_id: int,
                option_values_to_add: list = None,
                option_values_to_update: list = None,
                option_values_to_delete: list = None,
            ):
                variables = {
                    'productId': f'gid://shopify/Product/{product_id}',
                    'option': {'id': f'gid://shopify/ProductOption/{option_id}'},
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
                    variables={'productId': f'gid://shopify/Product/{product_id}', 'options': option_ids},
                    operation_name='deleteOptions',
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
                        Shopify.Files.StagedMediaUploadTarget.StagedUploadParameter(i)
                        for i in response_data['parameters']
                    ]

            def create(file_list, variables: dict) -> list:
                """Create staged media upload targets and upload files to google cloud storage. Return list of URLs"""
                response = Shopify.Query(
                    document=Shopify.Files.queries, variables=variables, operation_name='stagedUploadsCreate'
                )
                files = [
                    Shopify.Files.StagedMediaUploadTarget(i)
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
                document='./integration/queries/products.graphql',
                variables={'id': f'gid://shopify/ProductVariant/{variant_id}'},
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

        def get(collection_id: int):
            response = Shopify.Query(
                document=Shopify.Collection.queries,
                variables={'id': f'gid://shopify/Collection/{collection_id}'},
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
                variables={'id': f'gid://shopify/Collection/{collection_id}'},
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
        queries = './integration/queries/menu.graphql'

        def get(menu_id: int):
            response = Shopify.Query(
                document=Shopify.Menu.queries,
                variables={'id': f'gid://shopify/Menu/{menu_id}'},
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
        queries = './integration/queries/channel.graphql'

        def get_all():
            response = Shopify.Query(document=Shopify.Channel.queries, operation_name='publications')
            return response.data

    class MetafieldDefinition:
        queries = './integration/queries/metafields.graphql'

        def get(metafield_id: int):
            response = Shopify.Query(
                document=Shopify.MetafieldDefinition.queries,
                variables={'id': f'gid://shopify/MetafieldDefinition/{metafield_id}'},
                operation_name='metafieldDefinition',
            )
            return response.data

        def create(variables: dict):
            response = Shopify.Query(
                document=Shopify.MetafieldDefinition.queries,
                variables=variables,
                operation_name='CreateMetafieldDefinition',
            )
            metafield_id = response.data['metafieldDefinitionCreate']['createdDefinition']['id'].split('/')[-1]
            return metafield_id

        def create_default():
            """Create default metafields for products"""
            cf_list = [
                {'name': 'botanical name', 'type': 'single_line_text_field'},
                {'name': 'climate zone', 'type': 'single_line_text_field'},
                {'name': 'plant type', 'type': 'single_line_text_field'},
                {'name': 'type', 'type': 'single_line_text_field'},
                {'name': 'mature height', 'type': 'single_line_text_field'},
                {'name': 'mature width', 'type': 'single_line_text_field'},
                {'name': 'sun exposure', 'type': 'single_line_text_field'},
                {'name': 'bloom time', 'type': 'single_line_text_field'},
                {'name': 'bloom color', 'type': 'single_line_text_field'},
                {'name': 'attracts pollinators', 'type': 'boolean'},
                {'name': 'growth rate', 'type': 'single_line_text_field'},
                {'name': 'deer resistant', 'type': 'boolean'},
                {'name': 'soil type', 'type': 'single_line_text_field'},
                {'name': 'color', 'type': 'single_line_text_field'},
                {'name': 'size', 'type': 'single_line_text_field'},
            ]

            for i in cf_list[::-1]:
                variables = {
                    'definition': {
                        'name': i['name'].title(),
                        'namespace': 'test_data',
                        'key': i['name'].replace(' ', '_').lower(),
                        'type': i['type'],
                        'pin': True,
                        'ownerType': 'PRODUCT',
                    }
                }
                meta_id = Shopify.MetafieldDefinition.create(variables)
                print(meta_id)

                insert_values = {
                    'META_ID': meta_id,
                    'NAME': i['name'],
                    'NAME_SPACE': 'test_data',
                    'META_KEY': i['name'].replace(' ', '_').lower(),
                    'TYPE': i['type'],
                    'PIN': 1,
                    'OWNER_TYPE': 'PRODUCT',
                }

                Database.Metafield_Definition.insert(insert_values)

        def delete(metafield_def_id: int):
            response = Shopify.Query(
                document=Shopify.MetafieldDefinition.queries,
                variables={
                    'id': f'gid://shopify/MetafieldDefinition/{metafield_def_id}',
                    'deleteAllAssociatedMetafields': True,
                },
                operation_name='DeleteMetafieldDefinition',
            )
            return response.data

        def delete_all():
            metafields = Database.Metafield_Definition.get_all()
            for i in metafields:
                Shopify.MetafieldDefinition.delete(i[0])
                Database.Metafield_Definition.delete(i[0])


if __name__ == '__main__':
    payload = {
        'id': f'gid://shopify/Menu/{creds.shopify_main_menu_id}',
        'title': 'Main Menu Test 2',
        'handle': 'main-menu-2',
        'items': [],
    }

    # Shopify.Menu.update(payload)

    # Shopify.Menu.create(payload)

    # Shopify.Customer.delete(8859520500006)

    # Shopify.Customer.create(
    #     customer_number='OL-12345',
    #     fst_name='Daniel',
    #     lst_name='Hernandez',
    #     address_list=[
    #         {
    #             'address': '345 Tron Ave NW',
    #             'city': 'Valdese',
    #             'state': 'NC',
    #             'zip': '28690',
    #             'phone': '8282341265',
    #             'first_name': 'Alex',
    #             'last_name': 'Powell',
    #             'country': 'US',
    #         }
    #     ],
    #     phone='18282341265',
    #     email='alexpow@gmail.com',

    Shopify.Menu.get_all()
    # test = {
    #     'id': f'gid://shopify/Menu/{creds.shopify_main_menu_id}',
    #     'title': 'Main Menu',
    #     'handle': 'main-menu',
    #     'items': [],
    # }

    # item = {
    #     'title': 'Landscape Design',
    #     'type': 'PAGE',
    #     'resourceId': 'gid://shopify/Page/256367984934',
    #     'items': [],
    # }
    # test['items'].append(item)
    # Shopify.Menu.update(test)

    # Shopify.Menu.get(256367984934)
