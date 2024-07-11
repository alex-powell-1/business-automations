import shopify
from setup import creds
from pathlib import Path
import json


class ShopifyGraphQL:
    shop_url = creds.shopify_shop_url
    api_version = '2024-07'
    private_app_password = creds.shopify_admin_token

    @staticmethod
    def execute_query(query: str):
        with shopify.Session.temp(
            ShopifyGraphQL.shop_url, ShopifyGraphQL.api_version, ShopifyGraphQL.private_app_password
        ):
            shopify.GraphQL().execute(query)


class ShopifySession:
    shop_url = creds.shopify_shop_url
    api_version = '2024-07'
    private_app_password = creds.shopify_admin_token

    def __init__(self):
        self.session = shopify.Session(
            ShopifySession.shop_url, ShopifySession.api_version, ShopifySession.private_app_password
        )
        self.activate_session()

    def activate_session(self):
        shopify.ShopifyResource.activate_session(self.session)

    @staticmethod
    def deactivate():
        shopify.ShopifyResource.clear_session()

    def close(self):
        ShopifySession.deactivate()

    def get_all_products(self) -> list[shopify.Product]:
        return shopify.Product.find()

    def get_product(self, product_id: str) -> shopify.Product:
        return shopify.Product.find(product_id)

    def create_product(self, variables):
        document = Path('./integration/GraphQL_queries/product_queries.graphql').read_text()
        response = session.execute_query(query=document, variables=variables, operation_name='productCreate')
        return json.loads(response)

    def update_product(self, product_id: int, payload: dict):
        product = session.get_product(product_id=product_id)
        return product._update(payload)

    def execute_query(self, query: str, variables: dict = None, operation_name: str = None):
        return shopify.GraphQL().execute(query=query, variables=variables, operation_name=operation_name)


if __name__ == '__main__':
    session = ShopifySession()
    variables = {'title': 'Apple', 'productType': 'Fruit', 'vendor': 'Fruit Stand'}
    response = session.create_product(variables={'input': variables})
    id = response['data']['productCreate']['product']['id'].split('/')[-1]
    print(id)
