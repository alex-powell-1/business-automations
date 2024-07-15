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

    def create_product(payload):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products.json'
        response = requests.post(url, headers=Shopify.headers, json=payload)
        print(f'Shopify POST Code: {response.status_code}.')
        print('Response:', json.dumps(response.json(), indent=4))
        if response.status_code in [200, 201, 207]:
            Shopify.logger.success(f'Shopify POST Code: {response.status_code}.')
            return True, response
        else:
            Shopify.error_handler.add_error_v(
                error=f'Shopify POST: Response Code: {response.status_code}\n'
                f'Payload: {payload}\n'
                f'Response: {json.dumps(response.json(), indent=4)}',
                origin='shopify_api.py --> create_product()',
            )
            return False, response

    def update_product(product_id: int, payload: dict):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
        response = requests.put(url, headers=Shopify.headers, json=payload)
        return response.json()

    def delete_product(product_id: int):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
        response = requests.delete(url, headers=Shopify.headers)
        return response.json()

    def execute_query(document, variables=None, operation_name=None):
        document = Path(document).read_text()
        endpoint = f'https://{Shopify.shop_url}/admin/api/2024-07/graphql.json'
        payload = {'query': document, 'variables': variables, 'operationName': operation_name}
        response = requests.post(endpoint, headers=Shopify.headers, json=payload)
        return response.json()


if __name__ == '__main__':
    variables = {
        'input': {
            'title': 'Banana',
            'productType': 'Helmet',
            'vendor': 'Nova',
            'descriptionHtml': '<p>Protect your head with this green helmet.</p>',
            'productOptions': [{'name': 'Option', 'values': [{'name': '9999 Gallon'}]}],
            'seo': {'title': 'Green Helmet', 'description': 'Protect your head with this green helmet.'},
            'status': 'ACTIVE',
            'tags': ['Helmet', 'Green', 'Bike'],
        },
        'media': [
            {
                'originalSource': 'https://termite-enormous-hornet.ngrok-free.app/files/catalog_images/product_images/B0001-2-9407.jpg',
                'alt': 'Gray helmet for bikers',
                'mediaContentType': 'IMAGE',
            },
            {
                'originalSource': 'https://www.youtube.com/watch?v=4L8VbGRibj8&list=PLlMkWQ65HlcEoPyG9QayqEaAu0ftj0MMz',
                'alt': 'Testing helmet resistance against impacts',
                'mediaContentType': 'EXTERNAL_VIDEO',
            },
        ],
    }

    file_path = './integration/queries/products.graphql'

    response = Shopify.execute_query(
        document=file_path, operation_name='CreateProductWithNewMedia', variables=variables
    )
    prod_id = response['data']['productCreate']['product']['id']
    target_del_variant_id = response['data']['productCreate']['product']['variants']['nodes'][0]['id']

    productVariantsBulkCreateVariables = {
        'productId': prod_id,
        'variants': [
            {
                'inventoryItem': {'cost': 50, 'tracked': True, 'requiresShipping': False, 'sku': '200926'},
                'inventoryPolicy': 'DENY',
                'inventoryQuantities': {'availableQuantity': 25, 'locationId': 'gid://shopify/Location/99670065446'},
                'price': 114.99,
                'compareAtPrice': 139.99,
                'optionValues': {'optionName': 'Option', 'name': '3 Gallon'},
            },
            {
                'inventoryItem': {'cost': 60, 'tracked': True, 'requiresShipping': False, 'sku': '234sdf'},
                'inventoryPolicy': 'DENY',
                'inventoryQuantities': {'availableQuantity': 25, 'locationId': 'gid://shopify/Location/99670065446'},
                'price': 204.99,
                'compareAtPrice': 239.99,
                'optionValues': {'optionName': 'Option', 'name': '5 Gallon'},
            },
        ],
    }

    response = Shopify.execute_query(
        document=file_path, operation_name='productVariantsBulkCreate', variables=productVariantsBulkCreateVariables
    )
    print(json.dumps(response, indent=4))

    del_variant_vars = {'id': target_del_variant_id}
    del_response = Shopify.execute_query(
        document=file_path, operation_name='productVariantDelete', variables=del_variant_vars
    )
    print('Deleted Variant:')
    print(json.dumps(del_response, indent=4))
