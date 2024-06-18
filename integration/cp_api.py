from setup import creds
import requests
import json
import math

from integration.database import Database

from integration.error_handler import GlobalErrorHandler


import uuid


def generate_guid():
    return str(uuid.uuid4())


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


DOC_PRESETS = {
    "PS_DOC_HDR": [
        ("BO_LINS", 0),
        ("SO_LINS", 0),
        ("RET_LINS", 0),
        ("RET_LIN_TOT", 0),
        ("SLS_REP", "POS"),
        ("STK_LOC_ID", 1),
        ("PRC_LOC_ID", 1),
        ("SVC_LINS", 0),
        ("BILL_TO_CONTACT_ID", 1),
        ("SHIP_TO_CONTACT_ID", 2),
        ("REQ_REPRICE", "N"),
        ("RS_UTC_DT", ["GETDATE()"]),
        ("IS_DOC_COMMITTED", "N"),
        ("FOOD_STMP_AMT", 0),
        ("FOOD_STMP_LINS", 0),
        ("FOOD_STMP_TAX_AMT", 0),
        ("TKT_DT", ["GETDATE()"]),
        ("IS_REL_TKT", "N"),
        ("FOOD_STMP_NORM_TAX_AMT", 0),
        ("HAS_ENTD_LINS", "N"),
        ("HAS_PCKD_LINS", "N"),
        ("HAS_PCKVRFD_LINS", "N"),
        ("HAS_INVCD_LINS", "N"),
        ("HAS_RLSD_LINS", "N"),
        ("TO_LEAVE_LINS", 0),
        ("LST_MAINT_DT", ["GETDATE()"]),
        ("LST_MAINT_USR_ID", "POS"),
        ("IS_OFFLINE", 0),
        ("RS_STAT", 1),
        ("DS_LINS", 0),
    ],
}


# {
#     "LIN_TYP": "S",
#     "ITEM_NO": product["sku"],
#     "QTY_SOLD": float(product["quantity"]),
#     "PRC": ext_prc / float(product["quantity"]),
#     "EXT_PRC": ext_prc,
#     "DSC_AMT": total_discount,
# }


class DocumentAPI(CounterPointAPI):
    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)

        self.base_url = f"{self.base_url}Document"

    def get_document(self, doc_id):
        url = f"{self.base_url}/{doc_id}"

        response = self.get(url)

        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)

        return pretty

    def post_ps_doc_lin(self, doc_id, payload: list):
        url = f"{self.base_url}/{doc_id}/Lines"

        print(url)

        response = self.post(url, payload={"PS_DOC_LIN": payload})

        print(response.content)

    def get_drawer_session_id(self, str_id, drw_id):
        query = f"""
        SELECT DRW_SESSION_ID FROM PS_DRW_SESSION
        WHERE STR_ID = '{str_id}' AND DRW_ID = '{drw_id}'
        """

        response = Database.db.query_db(query)

        return response[0][0]

    def create_document(self, name: str, props: list[tuple]):
        extra_props = DOC_PRESETS.get(name, [])
        props += extra_props

        def get_values(prop):
            if isinstance(prop[1], str):
                return f"'{prop[1]}'"
            elif isinstance(prop[1], list):
                return prop[1][0]
            else:
                return f"{prop[1]}"

        query = f"""
        INSERT INTO {name}
        ({', '.join([prop[0] for prop in props])})
        VALUES
        ({', '.join([get_values(prop) for prop in props])})
        """

        print(query)

        response = Database.db.query_db(query, commit=True)

        print(response)

        return

    def create_ps_doc_hdr(self, ps_doc_hdr_props: list[tuple]):
        self.create_document("PS_DOC_HDR", ps_doc_hdr_props)

    def post_document(self, payload: dict):
        url = self.base_url

        response = self.post(url, payload=payload)

        return response

        doc_id = 8600000015099
        tkt_no = 9988

        # PS_DOC_HDR PROPS TO ADD:
        # SAL_LINS
        # SAL_LIN_TOT
        # GFC_LINS
        # DRW_SESSION_ID
        # LOY_PGM_COD
        # TO_REL_LINS

        # DOC_GUID??

        ps_doc_hdr_props = [
            ("DOC_ID", doc_id),
            ("TKT_NO", tkt_no),
            ("DOC_GUID", generate_guid()),
            ("DRW_SESSION_ID", self.get_drawer_session_id("WEB", "1")),
        ]
        other_docs = {}

        for key, value in payload["PS_DOC_HDR"].items():
            if isinstance(value, dict):
                other_docs.append(value)
            elif isinstance(value, list):
                other_docs[key] = value
            else:
                ps_doc_hdr_props.append((key, value))

        self.create_ps_doc_hdr(ps_doc_hdr_props)
        self.post_ps_doc_lin(doc_id, other_docs["PS_DOC_LIN"])


# {
#     "PS_DOC_HDR": {
#         "STR_ID": "WEB",
#         "STA_ID": "WEB",
#         "DRW_ID": "1",
#         "CUST_NO": cust_no,
#         "TKT_TYP": "T",
#         "DOC_TYP": "T",
#         "USR_ID": "POS",
#         "TAX_COD": "EXEMPT",
#         "NORM_TAX_COD": "EXEMPT",
#         "SHIP_VIA_COD": "T" if is_shipping else "C",
#         "PS_DOC_NOTE": notes,
#         "PS_DOC_LIN": self.get_line_items_from_bc_products(bc_products),
#         "PS_DOC_GFC": self.get_gift_cards_from_bc_products(bc_products),
#         "PS_DOC_PMT": self.get_payment_from_bc_order(bc_order),
#         "PS_DOC_TAX": [
#             {
#                 "AUTH_COD": "EXEMPT",
#                 "RUL_COD": "TAX",
#                 "TAX_DOC_PART": "S",
#                 "TAX_AMT": "0",
#                 "TOT_TXBL_AMT": float(bc_order["total_inc_tax"] or 0)
#                 - float(bc_order["base_shipping_cost"] or 0),  # not shipping
#             },
#         ],
#     }
# }


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
        self.total_discount_amount = 0
        self.total_gfc_amount = 0
        self.total_hdr_disc = 0
        self.total_lin_disc = 0

    def get_line_items_from_bc_products(self, products: list):
        line_items = []

        for product in products:
            if product["type"] == "physical":
                total_discount = 0
                if len(product["applied_discounts"]) > 0:
                    for discount in product["applied_discounts"]:
                        if discount["target"] == "product":
                            total_discount += float(discount["amount"])

                ext_prc = (
                    float(product["base_price"]) * float(product["quantity"])
                    - total_discount
                )
                line_item = {
                    "LIN_TYP": "O",
                    "ITEM_NO": product["sku"],
                    "QTY_SOLD": float(product["quantity"]),
                    "PRC": ext_prc / float(product["quantity"]),
                    "EXT_PRC": ext_prc,
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
                self.total_gfc_amount += float(product["base_price"])

        return gift_cards

    def get_gift_card_payments_from_bc_order(self, bc_order: dict):
        gift_cards = []

        for gift_card in bc_order["transactions"]["data"]:
            if gift_card["method"] == "gift_certificate":
                gift_card = {
                    "AMT": gift_card["amount"],
                    "PAY_COD": "GC",
                    "FINAL_PMT": "N",
                    "CARD_NO": gift_card["gift_certificate"]["code"],
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

        payments += self.get_gift_card_payments_from_bc_order(bc_order)

        return payments

    def post_payment(self, doc_id, bc_order: dict):
        url = f"{self.base_url}/{doc_id}/Payments"

        payload = {"PS_DOC_PMT": self.get_payment_from_bc_order(bc_order)}

        print(url)
        print(payload)

        response = self.post(url, payload=payload)

        return response

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

        if apply_to == "H":
            self.total_hdr_disc += disc_amt
        else:
            self.total_lin_disc += disc_amt

        query = f"""
        INSERT INTO PS_DOC_DISC
        (DOC_ID, DISC_SEQ_NO, LIN_SEQ_NO, DISC_ID, APPLY_TO, DISC_TYP, DISC_AMT, DISC_PCT, DISC_AMT_SHIPPED)
        VALUES
        ('{doc_id}', {disc_seq_no}, {lin_seq_no or "NULL"}, {disc_id}, '{apply_to}', '{disc_type}', {disc_amt}, {disc_pct}, {disc_amt_shipped})
        """

        self.total_discount_amount += disc_amt

        response = Database.db.query_db(query, commit=True)

        print(response)

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

        print(response)

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

        print(response)

    def get_loyalty_points_used(self, doc_id):
        query = f"""
        SELECT AMT FROM PS_DOC_PMT
        WHERE PAY_COD = 'LOYALTY' AND DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query)
        points_used = 0
        try:
            points_used = math.floor(float(response[0][0] or 0))
        except:
            pass

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
                "DOC_TYP": "O",
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

        self.sub_tot = sum(
            [
                float(line_item["EXT_PRC"])
                for line_item in payload["PS_DOC_HDR"]["PS_DOC_LIN"]
            ]
        )

        return payload

    def post_order(self, cust_no: str, bc_order: dict):
        payload = self.get_post_order_payload(cust_no, bc_order)

        cust_no = payload["PS_DOC_HDR"]["CUST_NO"]

        response = self.post_document(payload)
        # try:
        doc_id = response.json()["Documents"][0]["DOC_ID"]

        self.write_loyalty(doc_id, cust_no, payload["PS_DOC_HDR"]["PS_DOC_LIN"])
        self.write_doc_discounts(doc_id, bc_order)
        self.write_doc_disc(doc_id, payload["PS_DOC_HDR"]["PS_DOC_LIN"])

        self.more_writes(doc_id, payload, bc_order)

        self.logger.success(f"Order {doc_id} created")
        # except Exception as e:
        # self.error_handler.add_error_v(
        #     "Order could not be created", origin="cp_api.py::post_order()"
        # )
        # if response.content is not None:
        #     self.error_handler.add_error_v(
        #         response.content, origin="cp_api.py::post_order()"
        #     )

        # print(e)

        return response

    def more_writes(self, doc_id, payload, bc_order):
        tot_tndr = float(bc_order["total_inc_tax"] or 0)

        query = f"""
        DELETE FROM PS_DOC_HDR_TOT
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        print(response)

        sub_tot = float(bc_order["subtotal_ex_tax"] or 0)
        document_discount = float(self.total_discount_amount or 0)
        gfc_amount = float(self.total_gfc_amount or 0)
        shipping_amt = float(bc_order["base_shipping_cost"] or 0)

        tot = sub_tot - document_discount - gfc_amount + shipping_amt

        query = f"""
        INSERT INTO PS_DOC_HDR_TOT
        (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
        VALUES
        ('{doc_id}', 'S', 0, '!', 0, {len(payload["PS_DOC_HDR"]["PS_DOC_LIN"])}, {gfc_amount}, 0, {sub_tot}, 0, 0, {shipping_amt}, 0, 0, {tot_tndr}, 0, 0, 0, {tot}, 0, {self.total_hdr_disc}, {self.total_lin_disc}, {tot_tndr}, 0)
        """

        response = Database.db.query_db(query, commit=True)

        query = f"""
        UPDATE PS_DOC_LIN
        SET LIN_TYP = 'S'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        query = f"""
        UPDATE PS_DOC_PMT_APPLY
        SET APPL_TYP = 'S'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        print(query)
        print(response)


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
            elif key == "gift_certificate_id":
                try:
                    myjson = JsonTools.get_json(
                        f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/gift_certificates/{value}"
                    )
                    obj[key] = myjson
                except:
                    pass

        return obj
