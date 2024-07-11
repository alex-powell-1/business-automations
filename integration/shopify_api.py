from setup import creds
import shopify
import os
import binascii
import requests

# shopify.Session.setup(api_key=creds.shopify_client_id, secret=creds.shopify_secret)


# api_version = '2024-07'
# state = binascii.b2a_hex(os.urandom(15)).decode('utf-8')
# redirect_uri = 'http://localhost:53158/auth/shopify/callback'
# scopes = ['read_products', 'read_orders']

# newSession = shopify.Session(creds.shopify_shop_url, api_version)
# auth_url = newSession.create_permission_url(scopes, redirect_uri, state)
# print(auth_url)


# session = shopify.Session(creds.shopify_shop_url, api_version)
# access_token = session.request_token(params=request_params)  # request_token will validate hmac and timing attacks
# # you should save the access token now for future use.
# print(access_token)

# # redirect to auth_url

# # from setup import creds
# # import requests
# # import shopify

# # class ShopifyAPI:
# # 	shop_url = creds.shopify_shop_url
# #     client_id = creds.shopify_client_id
# #     secret = creds.shopify_secret

# #     def __init__(self):
# #         self.access


class ShopifyGraphQLAPI:
    token = creds.shopify_admin_token

    def __init__(self):
        self.headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': ShopifyGraphQLAPI.token}

    def execute_query(self, query, variables=None):
        """
        Executes a GraphQL query or mutation on the Shopify store.
        :param query: The GraphQL query or mutation as a string.
        :param variables: Optional dictionary of variables for the query.
        :return: The JSON response from Shopify.
        """
        url = f'https://{ShopifyGraphQLAPI.shop_url}/admin/api/2023-01/graphql.json'
        payload = {'query': query, 'variables': variables} if variables else {'query': query}
        response = requests.post(url, json=payload, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Query failed to run by returning code of {response.status_code}. {response.text}')


if __name__ == '__main__':
    # # Example: Get all products

    query = """
        {
        shop {
            name
        }
    }
    """
    api = ShopifyGraphQLAPI()
    response = api.execute_query(query)
    print(response['data']['shop']['name'])
