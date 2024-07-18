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

    class Response:
        def __init__(self, response):
            self.response = response
            self.data = response['data'] if 'data' in response else None
            self.errors = response['errors'] if 'errors' in response else None
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

        def __str__(self):
            return json.dumps(self.response, indent=4)

    class Product:
        queries = './integration/queries/products.graphql'

        def create(product_payload, variant_payload) -> tuple:
            """Create product on shopify and return tuple of product ID, media IDs, and variant IDs"""
            # Step 1: Create base product and associated media. Default Variant is created.

            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Product.queries,
                    operation_name='CreateProductWithNewMedia',
                    variables=product_payload,
                )
            )
            if response.errors or response.user_errors:
                raise Exception(
                    f'Error: {response.errors}\nUser Error: {response.user_errors}\n\nPayload: {product_payload}'
                )
            print(response)

            prod_id = response.data['productCreate']['product']['id'].split('/')[-1]
            variant_payload['productId'] = f'gid://shopify/Product/{prod_id}'
            prod_id = prod_id.split('/')[-1]  # cleanup for db storage
            media_ids = [x['id'].split('/')[-1] for x in response.data['productCreate']['product']['media']['nodes']]
            option_ids = [x['id'].split('/')[-1] for x in response.data['productCreate']['product']['options']]

            print(variant_payload)

            # Step 2: Create new variants
            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Product.queries,
                    operation_name='productVariantsBulkCreate',
                    variables=variant_payload,
                )
            )
            print('\nVariant create', response)
            variant_ids = [
                x['id'].split('/')[-1] for x in response.data['productVariantsBulkCreate']['productVariants']
            ]
            option_value_ids = [
                x['id'].split('/')[-1]
                for x in response.data['productVariantsBulkCreate']['product']['options'][0]['optionValues']
            ]

            return {
                'product_id': prod_id,
                'option_ids': option_ids,
                'option_value_ids': option_value_ids,
                'media_ids': media_ids,
                'variant_ids': variant_ids,
            }

        def update(product_payload, variant_payload):
            # Step 1: Update Product
            operation_name = 'productUpdate'
            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Product.queries, operation_name=operation_name, variables=product_payload
                )
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
            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Product.queries, operation_name=operation_name, variables=variant_payload
                )
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

    class Collection:
        queries = './integration/queries/collections.graphql'

        def create(payload: dict):
            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Collection.queries, variables=payload, operation_name='CollectionCreate'
                )
            )
            print('\nCollection create', response)
            collection_id = response.data['collectionCreate']['collection']['id'].split('/')[-1]
            return collection_id

        def update(payload: dict):
            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Collection.queries, variables=payload, operation_name='collectionUpdate'
                )
            )
            print('\nCollection update', response)
            return response

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
            response = Shopify.Response(
                Shopify.execute_query(
                    document=Shopify.Files.queries, variables=variables, operation_name='stagedUploadsCreate'
                )
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

    def execute_query(document, variables=None, operation_name=None):
        query_doc = Path(document).read_text()
        endpoint = f'https://{Shopify.shop_url}/admin/api/2024-07/graphql.json'
        payload = {'query': query_doc, 'variables': variables, 'operationName': operation_name}
        response = requests.post(endpoint, headers=Shopify.headers, json=payload)
        return response.json()


if __name__ == '__main__':
    variables = {
        'input': [{'filename': '10240.jpg', 'mimeType': 'image/jpg', 'httpMethod': 'POST', 'resource': 'IMAGE'}]
    }
    filepath = [f'{creds.photo_path}/10240.jpg']
    print(Shopify.Files.create(file_path_list=filepath, variables=variables))
