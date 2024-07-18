from setup import creds
import requests
import json
from setup.error_handler import ProcessOutErrorHandler
import re
from pathlib import Path


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
        def __init__(self, document, variables=None, operation_name=None):
            self.response = self.execute_query(document, variables, operation_name)
            self.data = self.response['data'] if 'data' in self.response else None
            self.errors = self.response['errors'] if 'errors' in self.response else None
            self.user_errors = []
            if self.data:
                for i in self.data:
                    for j in self.data[i]:
                        if j == 'userErrors':
                            for k in self.data[i][j]:
                                self.user_errors.append(k['message'])

            if self.errors:
                Shopify.error_handler.add_error_v(f'Error: {self.errors}')

            if self.user_errors:
                Shopify.error_handler.add_error_v(
                    f'User Error: {self.user_errors}\nResponse: {json.dumps(self.response, indent=4)}'
                )
            if self.errors or self.user_errors:
                raise Exception(f'Error: {self.errors}\nUser Error: {self.user_errors}\n\Variables: {variables}')

            print(self)

        def __str__(self):
            return json.dumps(self.response, indent=4)

        def execute_query(self, document, variables=None, operation_name=None):
            query_doc = Path(document).read_text()
            endpoint = f'https://{Shopify.shop_url}/admin/api/2024-07/graphql.json'
            payload = {'query': query_doc, 'variables': variables, 'operationName': operation_name}
            response = requests.post(endpoint, headers=Shopify.headers, json=payload)
            return response.json()

    class Product:
        queries = './integration/queries/products.graphql'

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
            prod_id, media_ids, option_ids, option_value_ids, variant_ids = Shopify.Product.create(product_payload)
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

            default_variant_id = variant_ids[0]
            variant_payload['input']['id'] = f'gid://shopify/ProductVariant/{default_variant_id}'

            response = Shopify.Query(
                document=Shopify.Product.queries, operation_name='productVariantUpdate', variables=variant_payload
            )
            inventory_item_id = response.data['productVariantUpdate']['productVariant']['inventoryItem']['id']
            inventory_payload['input']['quantities'][0]['inventoryItemId'] = inventory_item_id

            # Step 3: Update the inventory of the default variant.
            Shopify.Inventory.update(inventory_payload)

            return {
                'product_id': prod_id,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'media_ids': media_ids,
                'variant_ids': variant_ids,
            }

        def create(product_payload) -> tuple:
            """Create product on shopify and return tuple of product ID, media IDs, and variant IDs"""
            # Step 1: Create base product and associated media. Default Variant is created.

            response = Shopify.Query(
                document=Shopify.Product.queries, operation_name='CreateProductWithNewMedia', variables=product_payload
            )
            print('\nProduct create', response)

            prod_id = response.data['productCreate']['product']['id'].split('/')[-1]
            media_ids = [x['id'].split('/')[-1] for x in response.data['productCreate']['product']['media']['nodes']]
            option_ids = [x['id'].split('/')[-1] for x in response.data['productCreate']['product']['options']]
            option_value_ids = [
                x['id'].split('/')[-1] for x in response.data['productCreate']['product']['options'][0]['optionValues']
            ]
            variant_ids = [
                x['id'].split('/')[-1] for x in response.data['productCreate']['product']['variants']['nodes']
            ]

            return prod_id, media_ids, option_ids, option_value_ids, variant_ids

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

            print('\nVariant update', response)

            variant_ids = [x['id'].split('/')[-1] for x in response.data[operation_name]['productVariants']]
            option_value_ids = [
                x['id'].split('/')[-1] for x in response.data[operation_name]['product']['options'][0]['optionValues']
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

        class Variant:
            def create_bulk(variables):
                response = Shopify.Query(
                    document=Shopify.Product.queries, operation_name='productVariantsBulkCreate', variables=variables
                )

                print('\nVariant create', response)
                variant_ids = [
                    x['id'].split('/')[-1] for x in response.data['productVariantsBulkCreate']['productVariants']
                ]
                option_value_ids = [
                    x['id'].split('/')[-1]
                    for x in response.data['productVariantsBulkCreate']['product']['options'][0]['optionValues']
                ]
                return variant_ids, option_value_ids

            def update_single(variables):
                response = Shopify.Query(
                    document=Shopify.Product.queries, variables=variables, operation_name='productVariantUpdate'
                )
                print('\nVariant update', response)
                return response.data

        class Option:
            def delete(product_id: str, option_ids: list):
                response = Shopify.Query(
                    document=Shopify.Product.queries,
                    variables={'productId': f'gid://shopify/Product/{product_id}', 'options': option_ids},
                    operation_name='deleteOptions',
                )
                print('\nOption delete', response)
                return response.data

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
            # sample_inventory_payload = {
            #     'input': {
            #         'name': 'available',
            #         'reason': 'correction',
            #         'ignoreCompareQuantity': True,
            #         'quantities': [
            #             {'locationId': creds.shopify_location_id, 'quantity': 225}
            #         ],
            #     }
            # }

            response = Shopify.Query(
                document=Shopify.Inventory.queries, variables=payload, operation_name='inventorySetQuantities'
            )
            print('\nInventory update', response)
            return response.data

    class Collection:
        queries = './integration/queries/collections.graphql'

        def create(payload: dict):
            response = Shopify.Query(
                document=Shopify.Collection.queries, variables=payload, operation_name='CollectionCreate'
            )
            print('\nCollection create', response)
            collection_id = response.data['collectionCreate']['collection']['id'].split('/')[-1]
            return collection_id

        def update(payload: dict):
            response = Shopify.Query(
                document=Shopify.Collection.queries, variables=payload, operation_name='collectionUpdate'
            )
            print('\nCollection update', response)
            return response.data

        def delete(collection_id: int):
            response = Shopify.Query(
                document=Shopify.Collection.queries,
                variables={'id': f'gid://shopify/Collection/{collection_id}'},
                operation_name='collectionDelete',
            )
            print('\nCollection delete', response)
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
                    Shopify.Files.StagedMediaUploadTarget.StagedUploadParameter(i) for i in response_data['parameters']
                ]

        def create(file_list, variables: dict) -> list:
            """Create staged media upload targets and upload files to google cloud storage. Return list of URLs"""
            response = Shopify.Query(
                document=Shopify.Files.queries, variables=variables, operation_name='stagedUploadsCreate'
            )
            files = [
                Shopify.Files.StagedMediaUploadTarget(i) for i in response.data['stagedUploadsCreate']['stagedTargets']
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
                        print(response.status_code, response.text)
                        raise Exception(f'File {file_path.name} failed to upload')

            return url_list


if __name__ == '__main__':
    product_variables = {
        'input': {
            'descriptionHtml': 'testing',
            'productType': 'car',
            'status': 'ACTIVE',
            'title': 'Wagon',
            'vendor': 'Volkswagon',
        }
    }

    variant_variant = {
        'input': {
            'position': 1,
            'price': '100.00',
            'compareAtPrice': '105.00',
            'inventoryPolicy': 'DENY',
            'inventoryQuantities': [{'locationId': creds.shopify_location_id, 'availableQuantity': 10}],
            'inventoryItem': {
                'sku': '1234',
                'tracked': True,
                'cost': 19.99,
                'measurement': {'weight': {'value': 10, 'unit': 'POUNDS'}},
                'requiresShipping': False,
            },
            'taxable': False,
        }
    }

    sample_inventory_payload = {
        'input': {
            'name': 'available',
            'reason': 'correction',
            'ignoreCompareQuantity': True,
            'quantities': [{'locationId': creds.shopify_location_id, 'quantity': 225}],
        }
    }

    # Shopify.Query(
    #     document='./integration/queries/products.graphql',
    #     variables={'id': 'gid://shopify/Product/9485153075494'},
    #     operation_name='variantInventoryId',
    # )

    print(Shopify.Inventory.get_inventory_ids(variant_id=49258794058022))
