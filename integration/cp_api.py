from setup import creds
import requests
import json


class CounterPointAPI:
    def __init__(self, session: requests.Session = requests.Session()):
        self.base_url = creds.cp_api_server
        self.session = session

        self.get_headers = {
            "Authorization": f"Basic {creds.cp_api_user}",
            "APIKey": creds.cp_api_key,
            "Accept": "application/json",
        }

        self.post_headers = {
            "Authorization": f"Basic {creds.cp_api_user}",
            "APIKey": creds.cp_api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get(self, url):
        response = self.session.get(url, headers=self.get_headers, verify=False)

        return response

    def post(self, url, payload: dict):
        response = self.session.post(
            url, headers=self.post_headers, data=payload, verify=False
        )

        return response


class DocumentAPI(CounterPointAPI):
    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)

        self.base_url = f"{self.base_url}/Document"

    def get_document(self, doc_id):
        url = f"{self.base_url}/{doc_id}"

        response = self.get(url)

        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)

        return pretty

    def post_document(self, payload: dict):
        url = self.base_url

        response = self.post(url, payload)

        return response


# {
#     "LIN_TYP": "S",
#     "ITEM_NO": "BTSP4MP",
#     "QTY_SOLD": "1",
#     "PRC": 11.99,
#     "EXT_PRC": 11.99 * 1,
# }


class OrderAPI(DocumentAPI):
    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)

    def get_line_items_from_bc_products(self, products: list):
        line_items = []

        for product in products:
            line_item = {
                "LIN_TYP": "S",
                "ITEM_NO": product["sku"],
                "QTY_SOLD": product["quantity"],
                "PRC": product["price"],
                "EXT_PRC": product["price"] * product["quantity"],
            }

            if product["type"] != "giftcard":
                line_items.append(line_item)

        self.line_item_length = len(line_items)

        return line_items

    def get_gift_cards_from_bc_products(self, products: list):
        gift_cards = []

        for product in products:
            if product["type"] == "giftcard":
                gift_card = {
                    "GFC_COD": "GC",
                    "GFC_NO": product["gift_card_number"],
                    "AMT": product["price"],
                    "LIN_SEQ_NO": self.line_item_length + 1,
                }

                gift_cards.append(gift_card)

        return

    def post_order(
        self,
        cust_no: str,
        is_shipping: bool = False,
        notes: list[dict[str, str]] = [],
        bc_products: list = [],
        shipping_cost: float = 0,
    ):
        payload = {
            "PS_DOC_HDR": {
                "STR_ID": "WEB",
                "STA_ID": "WEB",
                "DRW_ID": "1",
                "CUST_NO": cust_no,
                "TKT_TYP": "T",
                "DOC_TYP": "T",
                "USR_ID": "POS",
                "TAX_COD": "EXEMPT",
                "NORM_TAX_COD": "EXEMPT",
                "SHIP_VIA_COD": "T" if is_shipping else "C",
                "PS_DOC_NOTE": notes,
                "PS_DOC_LIN": self.get_line_items_from_bc_products(bc_products),
                "PS_DOC_GFC": self.get_gift_cards_from_bc_products(bc_products),
                "PS_DOC_PMT": [
                    # {"AMT": 25, "PAY_COD": "BIG", "FINAL_PMT": "N"},
                    {
                        "AMT": 11.99,
                        "PAY_COD": "GC",
                        "FINAL_PMT": "N",
                        "CARD_NO": "TEST-GFC-LUKE-3",
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
                "PS_DOC_DISC": [
                    {"LIN_SEQ_NO": 1, "DISC_ID": "100000000000101", "DISC_AMT": 5}
                ],
            },
            "PS_TAX": {"ORD_NORM_TAX_AMT": 0, "ORD_TAX_AMT": 0},
        }

        if is_shipping:
            payload["PS_DOC_HDR_MISC_CHRG"] = [
                {
                    "TOT_TYP": "S",
                    "MISC_CHRG_NO": "1",
                    "MISC_TYP": "A",
                    "MISC_AMT": shipping_cost,
                }
            ]
