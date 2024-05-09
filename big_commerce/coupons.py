import json
import secrets
import string
from datetime import datetime, timezone
from email import utils
from setup import date_presets

import requests

from setup import creds


class Coupon:
    def __init__(self, coupon_id):
        self.id = coupon_id
        self.name = ""
        self.type = ""
        self.amount = ""
        self.min_purchase = ""
        self.expires = ""
        self.enabled = ""
        self.code = ""
        self.applies_to = ""
        self.num_uses = ""
        self.max_uses = ""
        self.max_uses_per_customer = ""
        self.restricted_to = ""
        self.shipping_methods = ""
        self.date_created = ""
        self.get_coupon_details()

    def get_coupon_details(self):
        url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons/{self.id}"
        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = requests.get(url=url, headers=headers)

        if response.status_code == 200:
            json_response = response.json()
            self.name = json_response['name']
            self.type = json_response['type']
            self.amount = json_response['amount']
            self.min_purchase = json_response['min_purchase']
            self.expires = json_response['expires']
            self.enabled = json_response['enabled']
            self.code = json_response['code']
            self.applies_to = json_response['applies_to']
            self.num_uses = json_response['num_uses']
            self.max_uses = json_response['max_uses']
            self.max_uses_per_customer = json_response['max_uses_per_customer']
            self.restricted_to = json_response['restricted_to']
            self.shipping_methods = json_response['shipping_methods']
            self.date_created = json_response['date_created']


def bc_get_all_coupons(pretty=False):
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons"

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.get(url=url, headers=headers)
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    return json_response


def bc_create_coupon(name, coupon_type, amount, min_purchase, code,
                     max_uses_per_customer, max_uses, expiration, enabled=True):
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons"

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    payload = {
        'name': name,
        'type': coupon_type,
        'amount': str(amount),
        'min_purchase': min_purchase,
        'enabled': enabled,
        'code': code,
        'max_uses_per_customer': max_uses_per_customer,
        'max_uses': max_uses,
        'applies_to': {
            "entity": "categories",
            "ids": [0]
        },
        'expires': expiration
    }
    response = requests.post(url=url, headers=headers, json=payload)
    return response.json()


def bc_delete_coupon(coupon_id):
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons/{coupon_id}"
    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    return requests.delete(url=url, headers=headers)


def generate_random_code(length):
    res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(length))
    return str(res)


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def delete_expired_coupons(log_file):
    print(f"Deleting Expired Coupons: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    total = 0
    coupons = bc_get_all_coupons(pretty=False)
    current_time = datetime.now(timezone.utc)
    for x in coupons:
        coupon = Coupon(x['id'])
        expiration_date = coupon.expires
        if expiration_date != '':
            expiration_date = utils.parsedate_to_datetime(expiration_date)
            expiration_date = utc_to_local(expiration_date)
            if expiration_date < current_time:
                bc_delete_coupon(coupon.id)
                total += 1
                deleted_coupon_log_file = open(creds.deleted_coupon_log, "a")
                for y, z in x.items():
                    print(str(y), ": ", z, file=deleted_coupon_log_file)
                    print(str(y), ": ", z, file=log_file)

    print(f"Deleting Expired Coupons: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print(f"Total Coupons Deleted: {total}", file=log_file)
    print("-----------------------", file=log_file)

