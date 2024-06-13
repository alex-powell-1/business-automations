from setup import creds
import requests
import json
import math

from integration.database import Database


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

    def post(self, url, payload: dict = {}):
        response = self.session.post(
            url, headers=self.post_headers, json=payload, verify=False
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

        response = self.post(url, payload=payload)

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
            if product["type"] == "physical":
                line_item = {
                    "LIN_TYP": "S",
                    "ITEM_NO": product["sku"],
                    "QTY_SOLD": float(product["quantity"]),
                    "PRC": float(product["base_price"]),
                    "EXT_PRC": float(product["base_price"])
                    * float(product["quantity"]),
                }

                line_items.append(line_item)

        self.line_item_length = len(line_items)

        return line_items

    def get_gift_cards_from_bc_products(self, products: list):
        gift_cards = []

        for product in products:
            if product["type"] == "giftcertificate":
                gift_card = {
                    "GFC_COD": "GC",
                    "GFC_NO": product["gift_certificate_id"]["code"],
                    "AMT": product["base_price"],
                    "LIN_SEQ_NO": self.line_item_length + 1,
                }

                gift_cards.append(gift_card)

        return gift_cards

    def get_payment_from_bc_order(self, bc_order: dict):
        payments = [
            {
                "AMT": float(bc_order["total_inc_tax"] or 0),
                "PAY_COD": "BIG",
                "FINAL_PMT": "N",
            }
        ]

        if float(bc_order["store_credit_amount"] or 0) > 0:
            payments.append(
                {
                    "AMT": bc_order["store_credit_amount"],
                    "PAY_COD": "LOYALTY",
                    "FINAL_PMT": "N",
                }
            )

        return payments

    def get_is_shipping(self, bc_order: dict):
        return float(bc_order["base_shipping_cost"] or 0) > 0

    def get_shipping_cost(self, bc_order: dict):
        return float(bc_order["base_shipping_cost"] or 0)

    def get_notes(self, bc_order: dict):
        notes = []

        if bc_order["customer_message"]:
            notes.append(
                {"NOTE_ID": "Customer Message", "NOTE": bc_order["customer_message"]}
            )

        return notes

    def write_one_lin_loy(self, doc_id, line_item: dict, lin_seq_no: int):
        points_earned = (float(line_item["EXT_PRC"] or 0) / 20) or 0

        query = f"""
        INSERT INTO PS_DOC_LIN_LOY 
        (DOC_ID, LIN_SEQ_NO, LIN_LOY_PTS_EARND, LOY_PGM_RDM_ELIG, LOY_PGM_AMT_PD_WITH_PTS, LOY_PT_EARN_RUL_DESC, LOY_PT_EARN_RUL_SEQ_NO) 
        VALUES 
        ('{doc_id}', {lin_seq_no}, {points_earned}, 'Y', 0, 'Basic', 5)
        """

        Database.db.query_db(query, commit=True)

        return points_earned

    def write_lin_loy(self, doc_id, line_items: list[dict]):
        points = 0
        for lin_seq_no, line_item in enumerate(line_items, start=1):
            points += self.write_one_lin_loy(doc_id, line_item, lin_seq_no)

        return points

    def write_ps_doc_hdr_loy_pgm(
        self, doc_id, cust_no, points_earned: float, points_redeemed: float
    ):
        query = f"""
        SELECT LOY_PTS_BAL FROM {creds.ar_cust_table}
        WHERE CUST_NO = '{cust_no}'
        """
        response = Database.db.query_db(query)
        points_balance = float(response[0][0] or 0)

        wquery = f"""
        INSERT INTO PS_DOC_HDR_LOY_PGM
        (DOC_ID, LIN_LOY_PTS_EARND, LOY_PTS_EARND_GROSS, LOY_PTS_ADJ_FOR_RDM, LOY_PTS_ADJ_FOR_INC_RND, LOY_PTS_ADJ_FOR_OVER_MAX, LOY_PTS_EARND_NET, LOY_PTS_RDM, LOY_PTS_BAL)
        VALUES
        ('{doc_id}', 0, 0, 0, 0, 0, {points_earned}, {points_redeemed}, {points_balance})
        """

        Database.db.query_db(wquery, commit=True)

    def get_loyalty_points_used(self, doc_id):
        query = f"""
        SELECT AMT FROM PS_DOC_PMT
        WHERE PAY_COD = 'LOYALTY' AND DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query)
        points_used = math.floor(float(response[0][0] or 0))

    def write_loyalty(self, doc_id, cust_no, line_items: list[dict]):
        points_earned = math.floor(self.write_lin_loy(doc_id, line_items))
        points_redeemed = self.get_loyalty_points_used(doc_id)

        self.write_ps_doc_hdr_loy_pgm(doc_id, cust_no, points_earned, points_redeemed)

    def get_post_order_payload(self, cust_no: str, bc_order: dict = {}):
        bc_products = bc_order["products"]["url"]
        is_shipping = self.get_is_shipping(bc_order)
        shipping_cost = self.get_shipping_cost(bc_order)
        notes = self.get_notes(bc_order)

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
                "PS_DOC_PMT": self.get_payment_from_bc_order(bc_order),
                "PS_DOC_TAX": [
                    {
                        "AUTH_COD": "EXEMPT",
                        "RUL_COD": "TAX",
                        "TAX_DOC_PART": "S",
                        "TAX_AMT": "0",
                        "TOT_TXBL_AMT": float(bc_order["total_inc_tax"] or 0)
                        - float(bc_order["base_shipping_cost"] or 0),  # not shipping
                    },
                ],
            }
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

        return payload

    def post_order(self, cust_no: str, bc_order: dict):
        payload = self.get_post_order_payload(cust_no, bc_order)

        cust_no = payload["PS_DOC_HDR"]["CUST_NO"]

        response = self.post_document(payload)
        try:
            doc_id = response.json()["Documents"][0]["DOC_ID"]

            self.write_loyalty(doc_id, cust_no, payload["PS_DOC_HDR"]["PS_DOC_LIN"])

            print(f"Order {doc_id} created")
        except:
            print(response.content)

        return response
