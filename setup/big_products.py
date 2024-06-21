from setup import creds
import requests
import json


def bc_create_product(name, product_type, sku, weight, price):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products"

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    payload = {
        'name': name,
        'type': product_type,
        'sku': sku,
        'weight': weight,
        'price': price
    }

    response = (requests.post(url, headers=headers, json=payload)).content
    response = json.loads(response)
    pretty_print = json.dumps(response, indent=4)
    return pretty_print


def bc_update_product(product_id, payload, pretty=False):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}"

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = (requests.put(url, headers=headers, json=payload))
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    if json_response['status'] != '404':
        return json_response


def bc_get_product(product_id, pretty=False):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}"

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        response = (requests.get(url, headers=headers))
        json_response = response.json()
        if pretty:
            pretty = response.content
            pretty = json.loads(pretty)
            pretty = json.dumps(pretty, indent=4)
            return pretty
        if 'status' in json_response:
            if json_response['status'] == 404:
                return None
        else:
            return json_response


def bc_get_variant(product_id, variant_id, pretty=False):
    url = (f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/"
           f"products/{product_id}/variants/{variant_id}")

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = (requests.get(url, headers=headers))
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    return json_response
