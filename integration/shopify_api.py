from setup import creds
import requests
import json


class Shopify:
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
        return requests.post(url, headers=Shopify.headers, json=payload)

    def update_product(product_id: int, payload: dict):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
        response = requests.put(url, headers=Shopify.headers, json=payload)
        return response.json()

    def delete_product(product_id: int):
        url = f'https://{Shopify.shop_url}/admin/api/2024-07/products/{product_id}.json'
        response = requests.delete(url, headers=Shopify.headers)
        return response.json()


if __name__ == '__main__':
    # # Example: Get all products

    # # Create a Product with Variants
    # payload = {
    #     'product': {
    #         'title': 'Thuja Green Giant',
    #         'body_html': '<p>"Green Giant" is a popular and well-known cultivar of Thuja standishii x plicata, commonly known as the Western Red Cedar. Here are some key features of the Green Giant Arborvitae:</p>  <ol>  <li>  <p><strong>Size:</strong> Green Giant Arborvitae is known for its rapid growth, and it can reach impressive heights. Mature specimens can grow up to 25 to 30 feet tall with a spread of about 8 to 10 feet.</p>  </li>  <li>  <p><strong>Growth Rate:</strong> This cultivar is recognized for its fast growth, making it a popular choice for those looking to establish a screen or hedge relatively quickly.</p>  </li>  <li>  <p><strong>Foliage:</strong> The foliage of the Green Giant Arborvitae is scale-like and maintains a rich green color throughout the year. The dense, layered branches provide excellent coverage.</p>  </li>  <li>  <p><strong>Shape:</strong> The tree has a naturally pyramidal shape, and its branches are held in a somewhat vertical orientation, giving it an elegant appearance.</p>  </li>  <li>  <p><strong>Versatility:</strong> Green Giant Arborvitae is often used as a privacy screen, windbreak, or as a specimen tree in larger landscapes. Its size and growth rate make it a valuable choice for creating a living wall or barrier.</p>  </li>  <li>  <p><strong>Drought Tolerance:</strong> Once established, Green Giant Arborvitae is relatively drought-tolerant. However, regular watering is recommended, especially during dry periods.</p>  </li>  <li>  <p><strong>Sunlight Requirements:</strong> It thrives in full sun to partial shade, although it generally prefers full sun for optimal growth.</p>  </li>  <li>  <p><strong>Hardiness:</strong> This arborvitae cultivar is hardy in USDA zones 5 to 8, making it suitable for a wide range of climates.</p>  </li>  </ol>  <p>When planting Green Giant Arborvitae, it\'s essential to provide adequate spacing considering its mature size and to ensure proper soil drainage. Regular pruning may be necessary to maintain a desired shape, especially if it is used as a hedge or screen. Overall, the Green Giant Arborvitae is valued for its combination of fast growth, attractive foliage, and versatility in landscaping applications.</p>',
    #         'vendor': 'Settlemyre Nursery',
    #         'product_type': '',
    #         'created_at': '2024-07-11T17:04:08-04:00',
    #         'handle': 'thuja-green-giant',
    #         'updated_at': '2024-07-11T17:13:20-04:00',
    #         'published_at': '2024-07-11T17:04:08-04:00',
    #         'template_suffix': '',
    #         'published_scope': 'global',
    #         'tags': '',
    #         'status': 'active',
    #         'admin_graphql_api_id': 'gid://shopify/Product/9472317194534',
    #         'variants': [
    #             {
    #                 'title': '15 Gallon',
    #                 'compare_at_price': None,
    #                 'price': '139.99',
    #                 'cost': '15.00',
    #                 'sku': '200926',
    #                 'position': 1,
    #                 'inventory_policy': 'deny',
    #                 'compare_at_price': None,
    #                 'fulfillment_service': 'manual',
    #                 'inventory_management': 'shopify',
    #                 'option1': '15 Gallon',
    #                 'option2': None,
    #                 'option3': None,
    #                 'created_at': '2024-07-11T17:04:09-04:00',
    #                 'updated_at': '2024-07-11T17:12:42-04:00',
    #                 'taxable': False,
    #                 'barcode': '',
    #                 'grams': 0,
    #                 'weight': 0.0,
    #                 'weight_unit': 'lb',
    #                 'inventory_quantity': 185,
    #                 'old_inventory_quantity': 185,
    #                 'requires_shipping': False,
    #             }
    #         ],
    #         'options': [{'name': 'Size', 'position': 1, 'values': ['15 Gallon', '7 Gallon']}],
    #     }
    # }

    # response = Shopify.create_product(variables=payload)

    # print(json.dumps(response, indent=4))

    response = Shopify.get_product(9474952167718)
    print(json.dumps(response, indent=4))
