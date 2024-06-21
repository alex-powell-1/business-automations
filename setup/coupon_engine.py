from setup import creds
import requests
import secrets
import string
import json
from email import utils
from datetime import datetime
from dateutil.relativedelta import relativedelta


def bc_create_coupon(
    name,
    type,
    amount,
    min_purchase,
    code,
    max_uses_per_customer,
    max_uses,
    expiration,
    enabled=True,
    pretty=False,
):
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons"

    headers = {
        "X-Auth-Token": creds.big_access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {
        "name": name,
        "type": type,
        "amount": str(amount),
        "min_purchase": min_purchase,
        "enabled": enabled,
        "code": code,
        "max_uses_per_customer": max_uses_per_customer,
        "max_uses": max_uses,
        "applies_to": {"entity": "categories", "ids": [0]},
        "expires": expiration,
    }
    response = requests.post(url=url, headers=headers, json=payload)
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    return json_response


def generate_random_code(length):
    res = "".join(
        secrets.choice(string.ascii_uppercase + string.digits) for i in range(length)
    )
    return str(res)


print(
    bc_create_coupon(
        name="Back in Stock - (email)",
        type="per_total_discount",
        amount=10,
        min_purchase=100,
        code=generate_random_code(8),
        max_uses_per_customer=1,
        max_uses=1,
        expiration=utils.format_datetime(datetime.now() + relativedelta(days=+3)),
    )
)
