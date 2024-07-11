from setup import creds
import requests


class ShopifyGraphQLAPI:
    token = creds.shopify_admin_token
    shop_url = creds.shopify_shop_url

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

    mutation = """
    mutation productCreate($input: ProductInput!) {
    productCreate(input: $input) {
        product {
        id
        title
        }
        userErrors {
        field
        message
        }
    }
    }
    """
    variables = None
    # variables = {
    # 	'input': {
    # 		'title': 'Example Product Title',
    # 		'descriptionHtml': '<strong>Amazing Product</strong>',
    # 		'productType': 'Widgets',
    # 		'vendor': 'Example Vendor',
    # 		'variants': [
    # 			{
    # 				'price': '19.99',
    # 				'sku': 'unique-sku-123',
    # 				'inventoryQuantity': 100,
    # 				'inventoryManagement': 'SHOPIFY',
    # 				'options': ['Black', 'Medium'],
    # 			}
    # 		],
    # 		'options': [
    # 			{'name': 'Color', 'values': ['Black', 'White']},
    # 			{'name': 'Size', 'values': ['Small', 'Medium', 'Large']},
    # 		],
    # 	}
    # }

    query = """
        mutation {
            productCreate(
                    input: {
                        title: "Sweet new product", 
                        productType: "Snowboard", 
                        vendor: "JadedPixel"
                    }
            ) 
                {
                product {
                    id
                }
            }
        }
    """
    api = ShopifyGraphQLAPI()
    response = api.execute_query(query=query)
    print(response)
