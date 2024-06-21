import json

import requests

from setup import creds


def get_webhooks(pretty=True):
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/hooks"
    headers = {
        "X-Auth-Token": creds.big_access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    response = requests.get(url, headers=headers)
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    if "status" in json_response:
        if json_response["status"] == 404:
            return None
    else:
        return json_response


def create_webhook(pretty=True):
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/hooks"
    headers = {
        "X-Auth-Token": creds.big_access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # payload = {
    #     "scope": "store/order/created",
    #     "destination": creds.ngrok_domain + "/bc",
    # }

    payload = {
        "scope": "store/order/refund/created",
        "destination": creds.ngrok_domain + "/bc",
    }

    response = requests.post(url, json=payload, headers=headers)
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    if "status" in json_response:
        if json_response["status"] == 404:
            return None
    else:
        return json_response


if __name__ == "__main__":
    # print(get_webhooks())
    print(create_webhook())
