import json
from datetime import datetime

import requests

from product_tools import products
from setup import creds
from utilities.handy_tools import pretty_print



def bc_create_product(name, product_type, sku, weight, price):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products"
    payload = {
        'name': name,
        'type': product_type,
        'sku': sku,
        'weight': weight,
        'price': price
    }
    response = requests.post(url, headers=creds.bc_api_headers, json=payload)
    print(pretty_print(response))
    return response.json()


def bc_get_custom_fields(product_id):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/custom-fields"
    response = requests.get(url, headers=creds.bc_api_headers)
    print(pretty_print(response))
    return response.json()

bc_get_custom_fields(5573)

def bc_update_product(product_id, payload, log_file, pretty=False):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}"
    response = (requests.put(url, headers=creds.bc_api_headers, json=payload))
    if response.status_code == 200:
            print(pretty_print(response))
            return response.json()
    else:
        print(f"Error: {response.content}")
        return response.content


def bc_create_image(product_id):
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images'
    payload = {
        "product_id": product_id,
        "is_thumbnail": True,
        "sort_order": -2147483648,
        "description": "Testing out the description field",
        "image_url": 'https://settlemyrenursery.com/product_images/import/sample_images/birthdaycoupon.jpg'
    }

    response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
    return response.content


def bc_get_product(product_id, pretty=False):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}"

        response = requests.get(url, headers=creds.bc_api_headers)
        if response.status_code == 200:
            json_response = response.json()
            if pretty:
                return pretty_print(response)
            return json_response
        if response.status_code == 404:
            print(f"Product ID: {product_id} not found on BigCommerce!")
            return None


def bc_get_product_images(product_id):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images"

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 404:
            return
        else:
            json_response = response.json()['data']
            return json_response


def bc_update_product_image(product_id, image_id, payload):
    url = (f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/"
           f"{product_id}/images/{image_id}")
    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.put(headers=headers, url=url, json=payload)
    if response == 404:
        return "Error"
    else:
        return response.content


def bc_has_product_thumbnail(product_id) -> bool:
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images"
        response = requests.get(url, headers=creds.bc_api_headers)
        if response.status_code == 404:
            return False
        else:
            json_response = response.json()['data']
            has_thumbnail = False
            for x in json_response:
                if x['is_thumbnail']:
                    has_thumbnail = True
            return has_thumbnail


def get_modifier(product_id, pretty=False):
    if product_id is not None:
        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/modifiers"
        response = requests.get(url, headers=creds.bc_api_headers)
        if response.status_code == 200:
            if pretty:
                print(pretty_print(response))
            return response.json()['data']


def delete_product_modifier(product_id, modifier_id):
    url = (f" https://api.bigcommerce.com/stores/{creds.big_store_hash}"
               f"/v3/catalog/products/{product_id}/modifiers/{modifier_id}")
    response = requests.delete(url, headers=creds.bc_api_headers)
    if response.status_code == 204:
        print(f"Deleted modifier {modifier_id} from product {product_id}")
    else:
        print(f"Error: {response.content}")


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


def get_category_trees():
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees"
    response = requests.get(url, headers=creds.bc_api_headers)
    print(pretty_print(response))
    return response.json()


def fix_missing_thumbnails(log_file):
    """In response to a bug with CPIce Data Integration, this function will correct products with missing
    thumbnail flags on the e-commerce site"""
    print(f"Set Fixing Missing Thumbnails: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    # Step 1: Get a list of all binding ids
    binding_ids = products.get_binding_ids()
    updated = 0
    for key in binding_ids:
        # Step 2: Get the parent product
        parent = products.get_parent_product(key)
        if parent is not None:
            # Step 2: Get product id of each binding key
            product_id = products.get_bc_product_id(parent)
            if product_id is not None:
                # Step 3: Check if product has a thumbnail image
                if not bc_has_product_thumbnail(product_id):
                    print(f"Missing Thumbnail Found for Binding Key: {key} / Product ID: {product_id}!", file=log_file)
                    # Step 4: Assign photo to thumbnail status
                    # # 4a. Get Top Child SKU from revenue data
                    top_child = products.get_top_child_product(key)
                    # 4b. Get a list of the bc unique product image ids for this product
                    product_images = bc_get_product_images(product_id)
                    if product_images is not None:
                        for image in product_images:
                            # print(str(image['image_file']).split("/")[2].split("__")[0])
                            # 4c. Find the image that is associated with binding key (base image, no carrot)
                            if key == str(image['image_file']).split("/")[2].split("__")[0]:
                                # 4d. Set this image to thumbnail flag: True
                                bc_update_product_image(product_id, image['id'], {"is_thumbnail": True})
                                print(f"Assigning thumbnail flag to base image for binding ID:"
                                      f"Image ID: {image['id']} Filename: {image['image_file']}\n\n", file=log_file)
                                updated += 1
                                break
                            # 4d. If this doesn't exist set it to the top child
                            elif top_child == str(image['image_file']).split("/")[2].split("__")[0]:
                                bc_update_product_image(product_id, image['id'], {"is_thumbnail": True})
                                print(f"Binding Key Image Not Found on BigCommerce!\n"
                                      f"Assigning thumbnail flag to base image for top-performing child:"
                                      f"Image ID: {image['id']} Filename: {image['image_file']}\n\n", file=log_file)
                                updated += 1
                                break
                            # 4e. If neither exist, set the first image to the thumbnail
                            else:
                                bc_update_product_image(product_id, image['id'], {"is_thumbnail": True})
                                print(f"Binding Key Image Not Found on BigCommerce!\n"
                                      f"Base Image for Top Performing Child Not Found on BigCommerce!\n"
                                      f"Assigning thumbnail flag to base image for first image:"
                                      f"Image ID: {image['id']} Filename: {image['image_file']}\n\n", file=log_file)
                                updated += 1
                                break

    print(f"Total products updated: {updated}", file=log_file)
    print(f"Set Fixing Missing Thumbnails: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)


def remove_workshop_modifier(product_id):
    modifier_id = get_modifier(product_id)
    if modifier_id is not None:
        delete_product_modifier(product_id, modifier_id)
        return f"Deleted modifier {modifier_id} from product {product_id}"
    else:
        return f"No modifier found for product {product_id}"


def remove_workshop_from_pottery():
    """Run in sandbox.py to remove workshop modifiers from pottery products."""
    for x in products.get_pottery_for_workshop("sku"):
        product_id = products.get_bc_product_id(x)
        modifier_id = get_modifier(product_id)
        print(x, product_id, modifier_id)
        if modifier_id is not None and len(modifier_id) > 0:
            try:
                for y in modifier_id:
                    delete_product_modifier(product_id, y['id'])
            except Exception as err:
                print(err)
                continue