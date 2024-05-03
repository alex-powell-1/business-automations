import json

import requests

from setup import creds


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


def bc_update_product(product_id, payload, log_file, pretty=False):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}"

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        response = (requests.put(url, headers=headers, json=payload))
    except Exception as err:
        print("Error:bc_update_product()", file=log_file)
        print(f"Payload: {payload}", file=log_file)
        print(err, file=log_file)
        print("-----------------------\n", file=log_file)
    else:
        if response.status_code == 200:
            json_response = response.json()
            if pretty:
                pretty = response.content
                pretty = json.loads(pretty)
                pretty = json.dumps(pretty, indent=4)
                return pretty
            return json_response


def bc_create_image(product_id):
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images'

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    payload = {
        "product_id": product_id,
        "is_thumbnail": True,
        "sort_order": -2147483648,
        "description": "Testing out the description field",
        "image_url": 'https://settlemyrenursery.com/product_images/import/sample_images/birthdaycoupon.jpg'
    }

    response = requests.post(url=url, headers=headers, json=payload)
    return response.content


def bc_get_product(product_id, pretty=False):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/variants"

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        response = requests.get(url, headers=headers)
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


def get_modifier_id(product_id):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/modifiers"

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers).json()
        if response['data']:
            return response['data'][0]['id']


def delete_product_modifier(product_id, modifier_id):
    if product_id is not None:
        url = (f" https://api.bigcommerce.com/stores/{creds.big_store_hash}"
               f"/v3/catalog/products/{product_id}/modifiers/{modifier_id}")

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = requests.delete(url, headers=headers)
        return response


def add_container_workshop_to_item(product_id):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/modifiers"

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        payload = {
            "display_name": "Workshop",
            "type": "product_list",
            "required": False,
            "sort_order": 1,
            "config": {
                "product_list_adjusts_inventory": True,
                "product_list_adjusts_pricing": True,
                "product_list_shipping_calc": "none"
            },
            "option_values": [
                {
                    "label": "Container Workshop April 27 2024 (+$30)",
                    "sort_order": 0,
                    "value_data": {
                        "product_id": 5274
                    },
                    "is_default": False,
                    "adjusters": {
                        "price": None,
                        "weight": None,
                        "image_url": "",
                        "purchasing_disabled": {
                            "status": False,
                            "message": ""
                        }
                    }
                },
                {
                    "label": "Container Workshop May 4 2024 (+$30)",
                    "sort_order": 1,
                    "value_data": {
                        "product_id": 5275
                    },
                    "is_default": False,
                    "adjusters": {
                        "price": None,
                        "weight": None,
                        "image_url": "",
                        "purchasing_disabled": {
                            "status": False,
                            "message": ""
                        }
                    }
                },
                {
                    "label": "Container Workshop May 11 2024 (+$30)",
                    "sort_order": 2,
                    "value_data": {
                        "product_id": 5276
                    },
                    "is_default": False,
                    "adjusters": {
                        "price": None,
                        "weight": None,
                        "image_url": "",
                        "purchasing_disabled": {
                            "status": False,
                            "message": ""
                        }
                    }
                },
                {
                    "label": "Container Workshop May 18 2024 (+$30)",
                    "sort_order": 3,
                    "value_data": {
                        "product_id": 5277
                    },
                    "is_default": False,
                    "adjusters": {
                        "price": None,
                        "weight": None,
                        "image_url": "",
                        "purchasing_disabled": {
                            "status": False,
                            "message": ""
                        }
                    }
                },
                {
                    "label": "Container Workshop May 25 2024 (+$30)",
                    "sort_order": 4,
                    "value_data": {
                        "product_id": 5278
                    },
                    "is_default": False,
                    "adjusters": {
                        "price": None,
                        "weight": None,
                        "image_url": "",
                        "purchasing_disabled": {
                            "status": False,
                            "message": ""
                        }
                    }
                }
            ],
        }

        response = requests.post(url, headers=headers, json=payload)
        response = json.loads(response.content)
        pretty_print = json.dumps(response, indent=4)
        return pretty_print


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
