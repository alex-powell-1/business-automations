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

    class Product:
        queries = './integration/queries/products.graphql'

        def create(product_payload, variant_payload) -> tuple:
            """Create product on shopify and return tuple of product ID, media IDs, and variant IDs"""
            # Step 1: Create base product and associated media. Default Variant is created.
            response = Shopify.execute_query(
                document=Shopify.Product.queries, operation_name='CreateProductWithNewMedia', variables=product_payload
            )
            if response and 'userErrors' in response:
                Shopify.error_handler.add_error_v(f'Error creating product: {response["userErrors"]}')
                raise Exception(f'Error creating product: {response["userErrors"]}')

            print('\nProduct create')
            print(json.dumps(response, indent=4))
            # Product ID to be used for creating new variants and to write to the database
            prod_id = response['data']['productCreate']['product']['id']
            # Media IDs to be used for writing to the database
            media_ids = [x['id'] for x in response['data']['productCreate']['product']['media']['nodes']]
            option_ids = [x['id'] for x in response['data']['productCreate']['product']['options']]
            # Variant ID to be deleted after creating new variants
            base_var_id = response['data']['productCreate']['product']['variants']['nodes'][0]['id']
            # Add product ID to the variant payload
            variant_payload['productId'] = prod_id

            # Step 2: Create new variants
            response = Shopify.execute_query(
                document=Shopify.Product.queries, operation_name='productVariantsBulkCreate', variables=variant_payload
            )
            if response and 'userErrors' in response:
                Shopify.error_handler.add_error_v(f'Error creating variants: {response["userErrors"]}')
                raise Exception(f'Error creating variants: {response["userErrors"]}')

            print('\nVariant create')
            print(json.dumps(response, indent=4))
            variant_ids = [x['id'] for x in response['data']['productVariantsBulkCreate']['productVariants']]

            # Step 3: Delete the default variant
            del_variant_vars = {'id': base_var_id}
            response = Shopify.execute_query(
                document=Shopify.Product.queries, operation_name='productVariantDelete', variables=del_variant_vars
            )
            if response and 'userErrors' in response:
                Shopify.error_handler.add_error_v(f'Error deleting base variant: {response["userErrors"]}')
                raise Exception(f'Error deleting base variant: {response["userErrors"]}')

            print('\nBase Variant delete', json.dumps(response, indent=4))
            # Return the product ID, media IDs, and variant IDs

            return prod_id, option_ids, media_ids, variant_ids

        def update(product_payload, variant_payload):
            # Step 1: Update Product
            response = Shopify.execute_query(
                document=Shopify.Product.queries, operation_name='productUpdate', variables=product_payload
            )
            if response and 'userErrors' in response:
                Shopify.error_handler.add_error_v(f'Error updating product: {response["userErrors"]}')
                raise Exception(f'Error updating product: {response["userErrors"]}')
            print('\nProduct update')
            print(json.dumps(response, indent=4))
            # Step 2: Update Variants
            response = Shopify.execute_query(
                document=Shopify.Product.queries, operation_name='productVariantsBulkUpdate', variables=product_payload
            )
            if response and 'userErrors' in response:
                Shopify.error_handler.add_error_v(f'Error creating product: {response["userErrors"]}')
                raise Exception(f'Error creating product: {response["userErrors"]}')
            print('\nVariant update')
            print(json.dumps(response, indent=4))

        def delete(product_id: int):
            url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
            response = requests.delete(url, headers=Shopify.headers)
            return response.json()

    class Collection:
        queries = './integration/queries/collections.graphql'

        def create(payload: dict):
            response = Shopify.execute_query(
                document=Shopify.Collection.queries, variables=payload, operation_name='CollectionCreate'
            )
            if response and 'userErrors' in response:
                Shopify.error_handler.add_error_v(f'Error creating collection: {response["userErrors"]}')
                raise Exception(f'Error creating collection: {response["userErrors"]}')
            print('\nCollection create')
            print(json.dumps(response, indent=4))
            collection_id = response['data']['collectionCreate']['collection']['id']
            return collection_id

        def update(payload: dict):
            response = Shopify.execute_query(
                document=Shopify.Collection.queries, variables=payload, operation_name='collectionUpdate'
            )
            if response and 'errors' in response:
                print(payload)
                Shopify.error_handler.add_error_v(f'Error updating collection: {response["errors"]}')
                raise Exception(f'Error updating collection: {response["errors"]}')
            print('\nCollection update')
            print(json.dumps(response, indent=4))
            return response

    def execute_query(document, variables=None, operation_name=None):
        query_doc = Path(document).read_text()
        endpoint = f'https://{Shopify.shop_url}/admin/api/2024-07/graphql.json'
        payload = {'query': query_doc, 'variables': variables, 'operationName': operation_name}
        response = requests.post(endpoint, headers=Shopify.headers, json=payload)
        return response.json()


if __name__ == '__main__':
    pass
