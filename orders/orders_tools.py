from setup import creds
import requests
import json


def get_all_orders():
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders?sort=date_created:desc"

    header = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.get(url=url, headers=header)

    print(response.content)


def get_document(doc_id):
    url = f'{creds.cp_api_server}/Document/{doc_id}'

    headers = {
        'Authorization': f'Basic {creds.cp_api_user}',
        'APIKey': creds.cp_api_key,
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers, verify=False)
    pretty = response.content
    pretty = json.loads(pretty)
    pretty = json.dumps(pretty, indent=4)
    print(pretty)


# get_document('553737137014686')


def post_document():
    url = f'{creds.cp_api_server}/Document'

    headers = {
        'Authorization': f'Basic {creds.cp_api_user}',
        'APIKey': creds.cp_api_key,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    # needs work...
    payload = {

            "PS_DOC_HDR": {
                "STR_ID": "1",
                "STA_ID": "1",
                "DRW_ID": "1",
                "CUST_NO": "105786",
                "TKT_TYP": "T",
                "DOC_TYP": "T",
                "USR_ID": "AP",
                "TAX_COD": "1",
                "NORM_TAX_COD": "1",
                "PS_DOC_NOTE": [
                    {
                        "NOTE_ID": "NOTE1",
                        "NOTE": "eCommerce order test"
                    }
                ],
                "PS_DOC_LIN": [
                    {
                        "LIN_TYP": "S",
                        "ITEM_NO": "BTSP4MP",
                        "QTY_SOLD": "1"
                    }
                ],


                "PS_DOC_HDR_MISC_CHRG": [
                    {
                        "TOT_TYP": "S",
                        "MISC_CHRG_NO": "1",
                        "MISC_TYP": "A",
                        "MISC_AMT": "0"
                    }
                ],


                "PS_DOC_PMT": [
                    {
                        "AMT": 11.99,
                        "PAY_COD": "BIG",
                        "FINAL_PMT": "N"
                    }
                ],


                "PS_DOC_TAX": [
                    {
                        "AUTH_COD": "NC",
                        "RUL_COD": "COUNTY",
                        "TAX_DOC_PART": "O",
                        "TAX_AMT": "0",
                        "TOT_TXBL_AMT": 11.99
                    }
                ]
            },
            "PS_DOC_HDR_TOT": [
                {
                    "SUB_TOT": 11.99,
                    "TOT_EXT_COST": 11.99,
                    "TOT": 11.99,
                    "AMT_DUE": 11.99,
                    "TOT_HDR_DISCNTBL_AMT": 11.99
                }
            ],
            "PS_TAX": {
                "ORD_NORM_TAX_AMT": 0,
                "ORD_TAX_AMT": 0
            }
        }

    response = requests.post(url, headers=headers, json=payload, verify=False)
    pretty = response.content
    pretty = json.loads(pretty)
    pretty = json.dumps(pretty, indent=4)
    print(pretty)

#post_document()