from setup import creds
import requests
import json
import math

from integration.database import Database

from integration.error_handler import GlobalErrorHandler


class CounterPointAPI:
    def __init__(self, session: requests.Session = requests.Session()):
        self.base_url = creds.cp_api_server
        self.session = session
        self.logger = GlobalErrorHandler.logger
        self.error_handler = GlobalErrorHandler.error_handler

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
        self.discount_seq_no = 1

    def get_line_items_from_bc_products(self, products: list):
        line_items = []

        for product in products:
            if product["type"] == "physical":
                total_discount = 0
                if len(product["applied_discounts"]) > 0:
                    for discount in product["applied_discounts"]:
                        if discount["target"] == "product":
                            total_discount += float(discount["amount"])

                line_item = {
                    "LIN_TYP": "S",
                    "ITEM_NO": product["sku"],
                    "QTY_SOLD": float(product["quantity"]),
                    "PRC": float(product["base_price"]),
                    "EXT_PRC": float(product["base_price"]) * float(product["quantity"])
                    - total_discount,
                    "DSC_AMT": total_discount,
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
        return float(bc_order["base_shipping_cost"]) > 0

    def get_shipping_cost(self, bc_order: dict):
        return float(bc_order["base_shipping_cost"])

    def get_notes(self, bc_order: dict):
        notes = []

        if bc_order["customer_message"]:
            notes.append(
                {"NOTE_ID": "Customer Message", "NOTE": bc_order["customer_message"]}
            )

        return notes

    def write_one_doc_disc(
        self, doc_id, disc_seq_no: int, disc_amt: float, lin_seq_no: int = None
    ):
        apply_to = "L" if lin_seq_no else "H"
        disc_type = "A"
        disc_id = "100000000000331" if lin_seq_no else "100000000000330"
        disc_pct = 0
        disc_amt_shipped = 0

        query = f"""
        INSERT INTO PS_DOC_DISC
        (DOC_ID, DISC_SEQ_NO, LIN_SEQ_NO, DISC_ID, APPLY_TO, DISC_TYP, DISC_AMT, DISC_PCT, DISC_AMT_SHIPPED)
        VALUES
        ('{doc_id}', {disc_seq_no}, {lin_seq_no or "NULL"}, {disc_id}, '{apply_to}', '{disc_type}', {disc_amt}, {disc_pct}, {disc_amt_shipped})
        """

        response = Database.db.query_db(query, commit=True)

        return

    def write_doc_disc(self, doc_id, line_items: list[dict]):
        for i, line_item in enumerate(line_items, start=1):
            amt = float(line_item["DSC_AMT"])

            if amt > 0:
                self.write_one_doc_disc(
                    doc_id, disc_seq_no=self.discount_seq_no, disc_amt=amt, lin_seq_no=i
                )

                self.discount_seq_no += 1

        return

    def write_h_doc_disc(self, doc_id, disc_amt: float):
        if disc_amt > 0:
            self.write_one_doc_disc(
                doc_id, disc_seq_no=self.discount_seq_no, disc_amt=disc_amt
            )

            self.discount_seq_no += 1

    def write_doc_discounts(self, doc_id, bc_order: dict):
        coupons = bc_order["coupons"]["url"]

        total = 0

        for coupon in coupons:
            total += float(coupon["amount"])

        if total > 0:
            self.write_one_doc_disc(
                doc_id, disc_seq_no=self.discount_seq_no, disc_amt=total
            )
            self.discount_seq_no += 1

    def write_one_lin_loy(self, doc_id, line_item: dict, lin_seq_no: int):
        points_earned = (float(line_item["EXT_PRC"] or 0) / 20) or 0

        query = f"""
        INSERT INTO PS_DOC_LIN_LOY 
        (DOC_ID, LIN_SEQ_NO, LIN_LOY_PTS_EARND, LOY_PGM_RDM_ELIG, LOY_PGM_AMT_PD_WITH_PTS, LOY_PT_EARN_RUL_DESCR, LOY_PT_EARN_RUL_SEQ_NO) 
        VALUES 
        ('{doc_id}', {lin_seq_no}, {points_earned}, 'Y', 0, 'Basic', 5)
        """

        response = Database.db.query_db(query, commit=True)

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

        # wquery = f"""
        # INSERT INTO PS_DOC_HDR_LOY_PGM
        # (DOC_ID, LIN_LOY_PTS_EARND, LOY_PTS_EARND_GROSS, LOY_PTS_ADJ_FOR_RDM, LOY_PTS_ADJ_FOR_INC_RND, LOY_PTS_ADJ_FOR_OVER_MAX, LOY_PTS_EARND_NET, LOY_PTS_RDM, LOY_PTS_BAL)
        # VALUES
        # ('107437365396751', 0, 0, 0, 0, 0, 24, 31, 24)
        # """

        response = Database.db.query_db(wquery, commit=True)

    def get_loyalty_points_used(self, doc_id):
        query = f"""
        SELECT AMT FROM PS_DOC_PMT
        WHERE PAY_COD = 'LOYALTY' AND DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query)
        points_used = math.floor(float(response[0][0] or 0))

        return points_used

    def write_loyalty(self, doc_id, cust_no, line_items: list[dict]):
        points_earned = math.floor(self.write_lin_loy(doc_id, line_items))
        points_redeemed = self.get_loyalty_points_used(doc_id)

        self.write_ps_doc_hdr_loy_pgm(doc_id, cust_no, points_earned, points_redeemed)

    def get_total_lin_disc(self, line_items: list[dict]):
        total = 0

        for line_item in line_items:
            total += float(line_item["DSC_AMT"])

        return total

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
            payload["PS_DOC_HDR"]["PS_DOC_HDR_MISC_CHRG"] = [
                {
                    "TOT_TYP": "S",
                    "MISC_CHRG_NO": "1",
                    "MISC_TYP": "A",
                    "MISC_AMT": shipping_cost,
                }
            ]

            payload["PS_DOC_HDR"]["PS_DOC_HDR_TOT"] = [
                {
                    "TOT_LIN_DISC": self.get_total_lin_disc(
                        payload["PS_DOC_HDR"]["PS_DOC_LIN"]
                    )
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
            self.write_doc_discounts(doc_id, bc_order)
            self.write_doc_disc(doc_id, payload["PS_DOC_HDR"]["PS_DOC_LIN"])

            self.logger.success(f"Order {doc_id} created")
        except:
            self.error_handler.add_error_v(
                "Order could not be created", origin="cp_api.py::post_order()"
            )
            if response.content is not None:
                self.error_handler.add_error_v(
                    response.content, origin="cp_api.py::post_order()"
                )

        return response


class JsonTools:
    @staticmethod
    def get_json(url: str):
        response = requests.get(url, headers=creds.bc_api_headers)
        return response.json()

    @staticmethod
    def unpack_list(lst: list):
        for i, item in enumerate(lst):
            if isinstance(item, dict):
                lst[i] = JsonTools.unpack(item)

        return lst

    @staticmethod
    def unpack(obj: dict):
        for key, value in obj.items():
            if isinstance(value, dict):
                obj[key] = JsonTools.unpack(value)
            elif isinstance(value, list):
                obj[key] = JsonTools.unpack_list(value)
            elif isinstance(value, str) and value.startswith("http"):
                try:
                    myjson = JsonTools.get_json(value)
                    if isinstance(myjson, list):
                        JsonTools.unpack_list(myjson)
                    if isinstance(myjson, dict):
                        JsonTools.unpack(myjson)
                    obj[key] = myjson
                except:
                    if value.endswith("coupons"):
                        obj[key] = []
                    pass

        return obj
