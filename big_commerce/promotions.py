from setup import creds
import requests

from utilities.handy_tools import pretty_print


def get_promotions():
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions"
    response = requests.get(url, headers=creds.bc_api_headers)
    if response.status_code == 200:
        print(pretty_print(response))
        return response.json()['data']


get_promotions()
