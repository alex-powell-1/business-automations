import shopify
from setup import creds
import random
from datetime import datetime


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

    def get_rand_product(self) -> shopify.Product:
        return shopify.Product.find(random.choice(self.get_all_products()).id)


if __name__ == '__main__':
    session = ShopifySession()

    print(session.get_rand_product().title)

    session.close()
