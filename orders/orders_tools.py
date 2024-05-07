from setup import creds
import requests


def get_all_orders():
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders?sort=date_created:desc"

    header = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.get(url=url, headers=header)

    print(response.content)
