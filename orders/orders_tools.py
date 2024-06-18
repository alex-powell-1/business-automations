from setup import creds
import requests
import json


def get_all_orders():
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders?sort=date_created:desc"

    header = {
        "X-Auth-Token": creds.big_access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    response = requests.get(url=url, headers=header)

    print(response.content)


def get_document(doc_id):
    url = f"{creds.cp_api_server}/Document/{doc_id}"

    headers = {
        "Authorization": f"Basic {creds.cp_api_user}",
        "APIKey": creds.cp_api_key,
        "Accept": "application/json",
    }

    response = requests.get(url, headers=headers, verify=False)
    pretty = response.content
    pretty = json.loads(pretty)
    pretty = json.dumps(pretty, indent=4)
    print(pretty)


# get_document('553737137014686')


def post_document():
    url = f"{creds.cp_api_server}/Document"

    headers = {
        "Authorization": f"Basic {creds.cp_api_user}",
        "APIKey": creds.cp_api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # needs work...
    payload = {
        "PS_DOC_HDR": {
            "STR_ID": "WEB",
            "STA_ID": "WEB",
            "DRW_ID": "1",
            "CUST_NO": "OL-100778",
            "TKT_TYP": "T",
            "DOC_TYP": "T",
            "USR_ID": "POS",
            "TAX_COD": "EXEMPT",
            "NORM_TAX_COD": "EXEMPT",
            "SHIP_VIA_COD": "C",
            "PS_DOC_NOTE": [{"NOTE_ID": "NOTE1", "NOTE": "eCommerce order test"}],
            "PS_DOC_LIN": [
                {
                    "LIN_TYP": "S",
                    "ITEM_NO": "BTSP4MP",
                    "QTY_SOLD": "1",
                    "PRC": 11.99,
                    "EXT_PRC": 11.99 * 1,
                }
            ],
            # "PS_DOC_HDR_MISC_CHRG": [
            #     {"TOT_TYP": "S", "MISC_CHRG_NO": "1", "MISC_TYP": "A", "MISC_AMT": "50"}
            # ],
            # "PS_DOC_GFC": [
            #     {
            #         "GFC_COD": "GC",
            #         "GFC_NO": "TEST-GFC-LUKE-3",
            #         "AMT": 25,
            #         "LIN_SEQ_NO": 1,
            #     }
            # ],
            "PS_DOC_PMT": [
                # {"AMT": 25, "PAY_COD": "BIG", "FINAL_PMT": "N"},
                {
                    "AMT": 11.99,
                    "PAY_COD": "BIG",
                    "FINAL_PMT": "N",
                }
            ],
            "PS_DOC_TAX": [
                {
                    "AUTH_COD": "EXEMPT",
                    "RUL_COD": "TAX",
                    "TAX_DOC_PART": "S",
                    "TAX_AMT": "0",
                    "TOT_TXBL_AMT": 11.99,  # not shipping
                },
            ],
            # "PS_DOC_DISC": [
            #     {
            #         "DISC_ID": "100000000000106",
            #         "DISC_AMT": 5.00,
            #         "DISC_TYP": "A",
            #         "APPLY_TO": "H",
            #     }
            # ],
            "PS_DOC_HDR_TOT": [
                {
                    "TOT_TYP": "S",
                    "INITIAL_MIN_DUE": 0,
                    "TAX_AMT_SHIPPED": 0,
                    "LINS": 1,
                    "SUB_TOT": 11.99,
                    "TOT_TND": 11.99,
                    "TOT_CHNG": 0,
                    "TOT_WEIGHT": 0,
                    "TOT_CUBE": 0,
                    "TOT": 11.99,
                    "AMT_DUE": 0,
                    "TOT_HDR_DISC": 5,
                    "TOT_LIN_DISC": 0,
                    "TOT_HDR_DISCNTBL_AMT": 11.99,
                }
            ],
        },
    }

    refund_payload = {
        "PS_DOC_HDR": {
            "STR_ID": "WEB",
            "STA_ID": "WEB",
            "DRW_ID": "1",
            "CUST_NO": "105786",
            "LOY_PGM_COD": "BASIC",
            "TKT_TYP": "T",
            "DOC_TYP": "T",
            "USR_ID": "POS",
            "TAX_COD": "EXEMPT",
            "NORM_TAX_COD": "EXEMPT",
            "SHIP_VIA_COD": "CPC_FLAT",
            "TAX_OVRD_REAS": "Y",
            "HAS_ENTD_LINS": "N",
            "PS_DOC_NOTE": [{"NOTE_ID": "NOTE1", "NOTE": "eCommerce order test #2"}],
            "PS_DOC_LIN": [
                {
                    "LIN_TYP": "R",
                    "ITEM_NO": "BTSP4MP",
                    "QTY_SOLD": 2,
                    "PRC": 11.99,
                }
            ],
            "PS_DOC_HDR_MISC_CHRG": [
                {
                    "TOT_TYP": "S",
                    "MISC_CHRG_NO": "1",
                    "MISC_TYP": "A",
                    "MISC_AMT": -50.00,
                }
            ],
            "PS_DOC_PMT": [
                {
                    "AMT": 73.98,
                    "PAY_COD": "BIG",
                    "FINAL_PMT": "N",
                    "PMT_LIN_TYP": "C",
                    "DESCR": "Big Commerce",
                },
            ],
            # "PS_DOC_PMT_APPLY": [
            #     {
            #         "AMT": -73.98,
            #         "APPL_TYP": "S",
            #     },
            # ],
            "PS_DOC_TAX": [
                {
                    "AUTH_COD": "EXEMPT",
                    "RUL_COD": "TAX",
                    "TAX_DOC_PART": "S",
                    "TAX_AMT": "0",
                    "TOT_TXBL_AMT": -23.98,  # not shipping
                }
            ],
        },
    }

    response = requests.post(url, headers=headers, json=payload, verify=False)
    pretty = response.content
    pretty = json.loads(pretty)
    pretty = json.dumps(pretty, indent=4)
    print(pretty)


post_document()


# EXAMPLE GIFT CARD PAYMENT PAYLOAD
# payload = {
#     "PS_DOC_HDR": {
#         "STR_ID": "WEB",
#         "STA_ID": "WEB",
#         "DRW_ID": "1",
#         "CUST_NO": "105786",
#         "LOY_PGM_COD": "BASIC",
#         "TKT_TYP": "T",
#         "DOC_TYP": "T",
#         "USR_ID": "POS",
#         "TAX_COD": "EXEMPT",
#         "NORM_TAX_COD": "EXEMPT",
#         "SHIP_VIA_COD": "C",
#         "PS_DOC_NOTE": [{"NOTE_ID": "NOTE1", "NOTE": "eCommerce order test"}],
#         "PS_DOC_LIN": [
#             {
#                 "LIN_TYP": "S",
#                 "ITEM_NO": "BTSP4MP",
#                 "QTY_SOLD": "1",
#                 "PRC": 11.99,
#                 "EXT_PRC": 11.99 * 1,
#             }
#         ],
#         # "PS_DOC_HDR_MISC_CHRG": [
#         #     {"TOT_TYP": "S", "MISC_CHRG_NO": "1", "MISC_TYP": "A", "MISC_AMT": "50"}
#         # ],
#         # "PS_DOC_GFC": [
#         #     {
#         #         "GFC_COD": "GC",
#         #         "GFC_NO": "TEST-GFC-LUKE-3",
#         #         "AMT": 25,
#         #         "LIN_SEQ_NO": 1,
#         #     }
#         # ],
#         "PS_DOC_PMT": [
#             # {"AMT": 25, "PAY_COD": "BIG", "FINAL_PMT": "N"},
#             {
#                 "AMT": 11.99,
#                 "PAY_COD": "GC",
#                 "FINAL_PMT": "N",
#                 "CARD_NO": "TEST-GFC-LUKE-3",
#             }
#         ],
#         "PS_DOC_TAX": [
#             {
#                 "AUTH_COD": "EXEMPT",
#                 "RUL_COD": "TAX",
#                 "TAX_DOC_PART": "S",
#                 "TAX_AMT": "0",
#                 "TOT_TXBL_AMT": 11.99,  # not shipping
#             }
#         ],
#     }
# }
