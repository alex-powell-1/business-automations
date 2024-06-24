from setup import creds
import requests
import json
import math
from datetime import datetime, timezone
from integration.database import Database
from integration.error_handler import GlobalErrorHandler, Logger, ErrorHandler
import uuid
from customer_tools import customers


ORDER_PREFIX = ""
REFUND_SUFFIX = "R"
PARTIAL_REFUND_SUFFIX = "PR"

# This class is primarily used to interact with the NCR Counterpoint API
# If you need documentation on the API, good luck.
# https://github.com/NCRCounterpointAPI/APIGuide/blob/master/Endpoints/POST_Document.md
class CounterPointAPI:
    logger = Logger(
        f"//MAINSERVER/Share/logs/integration/orders/orders_{datetime.now().strftime("%m_%d_%y")}.log"
    )
    error_handler = ErrorHandler(logger)

    def __init__(self, session: requests.Session = requests.Session()):
        self.base_url = creds.cp_api_server
        self.session = session
        self.logger = CounterPointAPI.logger
        self.error_handler = CounterPointAPI.error_handler

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


# This class is used to interact with the NCR Counterpoint API's Document endpoint
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

    def post_document(self, payload: dict):
        url = self.base_url

        response = self.post(url, payload=payload)

        return response


# This is the primary class used in this file.
# It is used to create/refund/partially refund orders.
class OrderAPI(DocumentAPI):
    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)
        self.discount_seq_no = 1
        self.total_discount_amount = 0
        self.total_gfc_amount = 0
        self.total_hdr_disc = 0
        self.total_lin_disc = 0
        self.refund = None
        self.pr = None

    # Returns true if the provided BigCommerce order is a refund
    def is_refund(self, bc_order: dict = None):
        if self.refund is not None:
            return self.refund
        elif bc_order is not None:
            print(bc_order)
            self.refund = (bc_order["status"] == "Refunded")
            # self.refund = False
            return self.refund
        else:
            return False

    # Return self.pr if it is not None, set self.pr to set if set is not None, or return False
    # This is used to determine if the order is a partial refund
    # I'm not using the setter here because I am manually setting the self.pr value later.
    def is_pr(self, set: bool = None):
        if self.pr is not None:
            return self.pr
        elif set is not None:
            self.pr = set
            return self.pr
        else:
            return False

    # Returns a list of line items from a BigCommerce order.
    # products is a list of products from a BigCommerce order.
    def get_line_items_from_bc_products(self, products: list):
        line_items = []

        for product in products:
            if product["type"] == "physical":
                total_discount = 0
                if len(product["applied_discounts"]) > 0:
                    for discount in product["applied_discounts"]:
                        if discount["target"] == "product":
                            total_discount += abs(float(discount["amount"]))

                ext_cost = 0

                query = f"""
                SELECT LST_COST FROM IM_ITEM
                WHERE ITEM_NO = '{product["sku"]}'
                """

                response = Database.db.query_db(query)
                if response is not None:
                    try:
                        ext_cost = float(response[0][0])
                    except:
                        pass
                else:
                    pass

                try:
                    qty = (
                        float(product["quantity_refunded"])
                        if self.is_pr()
                        else float(product["quantity"])
                    )
                except:
                    qty = float(product["quantity"])

                ext_prc = float(product["base_price"]) * qty - total_discount

                line_item = {
                    "LIN_TYP": "O",
                    "ITEM_NO": product["sku"],
                    "USR_ENTD_PRC": "N",
                    "QTY_SOLD": qty,
                    "PRC": ext_prc / qty,
                    "EXT_PRC": -ext_prc if self.is_refund() else ext_prc,
                    "EXT_COST": (
                        -ext_cost * qty if self.is_refund() else ext_cost * qty
                    ),
                    "DSC_AMT": total_discount,
                }

                line_items.append(line_item)
                self.total_lin_items += 1

        self.line_item_length = len(line_items)

        return line_items

    # Returns a list of gift cards from a list of products from a BigCommerce order.
    # products is a list of products from a BigCommerce order.
    def get_gift_cards_from_bc_products(self, products: list):
        gift_cards = []

        for i, product in enumerate(products, start=1):
            if self.is_pr() and float(product["quantity_refunded"]) == 0:
                continue

            if product["type"] == "giftcertificate":
                gift_card = {
                    "GFC_COD": "GC",
                    "GFC_NO": product["gift_certificate_id"]["code"],
                    "AMT": float(product["base_price"]),
                    "LIN_SEQ_NO": self.line_item_length + 1,
                    "DESCR": "Gift Certificate",
                    "CREATE_AS_STC": "N",
                    "GFC_SEQ_NO": i,
                }
                self.line_item_length += 1

                gift_cards.append(gift_card)
                self.total_gfc_amount += float(product["base_price"])

        return gift_cards

    # Return a list of gift cards used as payment in a BigCommerce order.
    def get_gift_card_payments_from_bc_order(self, bc_order: dict):
        gift_cards = []

        for gift_card in bc_order["transactions"]["data"]:
            if gift_card["method"] == "gift_certificate":
                _gift_card = {
                    "AMT": (
                        (float(gift_card["amount"]))
                        if self.is_refund()
                        else float(gift_card["amount"])
                    ),
                    "PAY_COD": "GC",
                    "FINAL_PMT": "N",
                    "CARD_NO": gift_card["gift_certificate"]["code"],
                    "PMT_LIN_TYP": "C" if self.is_refund() else "T",
                    "REMAINING_BAL": float(
                        gift_card["gift_certificate"]["remaining_balance"]
                    ),
                }

                gift_cards.append(_gift_card)

        return gift_cards

    # Returns a list of payments from a BigCommerce order.
    def get_payment_from_bc_order(self, bc_order: dict):
        def negative(num):
            return num if num == 0 else -num

        payments = [
            {
                "AMT": float(bc_order["total_inc_tax"] or 0),
                "PAY_COD": "BIG",
                "FINAL_PMT": "N",
                "PMT_LIN_TYP": "C" if self.is_refund() else "T",
            }
        ]

        if float(bc_order["store_credit_amount"] or 0) > 0:
            payments.append(
                {
                    "AMT": (
                        (float(bc_order["store_credit_amount"] or 0))
                        if self.is_refund()
                        else float(bc_order["store_credit_amount"] or 0)
                    ),
                    "PAY_COD": "LOYALTY",
                    "FINAL_PMT": "N",
                    "PMT_LIN_TYP": "C" if self.is_refund() else "T",
                }
            )

        payments += self.get_gift_card_payments_from_bc_order(bc_order)

        return payments

    # Returns true if the BigCommerce order requires shipping.
    def get_is_shipping(self, bc_order: dict):
        return float(bc_order["base_shipping_cost"]) > 0

    # Returns the shipping cost of a BigCommerce order.
    def get_shipping_cost(self, bc_order: dict):
        return float(bc_order["base_shipping_cost"])

    # Get a list of order notes from a BigCommerce order.
    def get_notes(self, bc_order: dict):
        notes = []

        if bc_order["customer_message"]:
            notes.append(
                {"NOTE_ID": "Customer Message", "NOTE": bc_order["customer_message"]}
            )

        return notes

    # Write entry into PS_DOC_DISC
    def write_one_doc_disc(
        self, doc_id, disc_seq_no: int, disc_amt: float, lin_seq_no: int = None
    ):
        apply_to = "L" if lin_seq_no else "H"
        disc_type = "A"
        disc_id = "100000000000331" if lin_seq_no else "100000000000330"
        disc_pct = 0
        disc_amt_shipped = 0

        if self.is_refund() and apply_to == "L":
            disc_amt = -disc_amt

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

        self.total_discount_amount += abs(disc_amt)

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success(f"Discount {disc_seq_no} created")
        else:
            self.error_handler.add_error_v(
                f"Discount {disc_seq_no} could not be created"
            )

        return

    # Provide a list of line items and write discounts for each line.
    def write_doc_disc(self, doc_id, line_items: list[dict]):
        for i, line_item in enumerate(line_items, start=1):
            amt = float(line_item["DSC_AMT"])

            if amt > 0:
                self.write_one_doc_disc(
                    doc_id, disc_seq_no=self.discount_seq_no, disc_amt=amt, lin_seq_no=i
                )

                self.discount_seq_no += 1

        return

    # Write full document discount
    def write_h_doc_disc(self, doc_id, disc_amt: float):
        if disc_amt > 0:
            self.write_one_doc_disc(
                doc_id, disc_seq_no=self.discount_seq_no, disc_amt=disc_amt
            )

            self.discount_seq_no += 1

    # Write all discounts from a BigCommerce order.
    def write_doc_discounts(self, doc_id, bc_order: dict):
        self.logger.info("Writing discounts")
        coupons = bc_order["coupons"]["url"]

        total = 0

        for coupon in coupons:
            total += float(coupon["amount"])

        if total > 0:
            self.write_one_doc_disc(
                doc_id, disc_seq_no=self.discount_seq_no, disc_amt=total
            )
            self.discount_seq_no += 1

    # Write loyalty line
    def write_one_lin_loy(self, doc_id, line_item: dict, lin_seq_no: int):
        points_earned = (float(line_item["EXT_PRC"] or 0) / 20) or 0

        query = f"""
        INSERT INTO PS_DOC_LIN_LOY 
        (DOC_ID, LIN_SEQ_NO, LIN_LOY_PTS_EARND, LOY_PGM_RDM_ELIG, LOY_PGM_AMT_PD_WITH_PTS, LOY_PT_EARN_RUL_DESCR, LOY_PT_EARN_RUL_SEQ_NO) 
        VALUES 
        ('{doc_id}', {lin_seq_no}, {points_earned}, 'Y', 0, 'Basic', 5)
        """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success(f"Line loyalty points ({points_earned})")
        else:
            self.error_handler.add_error_v(
                f"Line #{lin_seq_no} could not receive loyalty points"
            )

        return points_earned

    # Write all loyalty lines from a list of line items from a BC order.
    def write_lin_loy(self, doc_id, line_items: list[dict]):
        points = 0
        for lin_seq_no, line_item in enumerate(line_items, start=1):
            points += self.write_one_lin_loy(doc_id, line_item, lin_seq_no)

        return points

    # Write entry into PS_DOC_HDR_LOY_PGM
    def write_ps_doc_hdr_loy_pgm(
        self, doc_id, cust_no, points_earned: float, points_redeemed: float
    ):
        query = f"""
        SELECT LOY_PTS_BAL FROM {creds.ar_cust_table}
        WHERE CUST_NO = '{cust_no}'
        """
        response = Database.db.query_db(query)
        points_balance = 0
        try:
            points_balance = float(response[0][0]) if response else 0
        except:
            pass

        wquery = f"""
        INSERT INTO PS_DOC_HDR_LOY_PGM
        (DOC_ID, LIN_LOY_PTS_EARND, LOY_PTS_EARND_GROSS, LOY_PTS_ADJ_FOR_RDM, LOY_PTS_ADJ_FOR_INC_RND, LOY_PTS_ADJ_FOR_OVER_MAX, LOY_PTS_EARND_NET, LOY_PTS_RDM, LOY_PTS_BAL)
        VALUES
        ('{doc_id}', 0, 0, 0, 0, 0, {points_earned}, {points_redeemed}, {points_balance})
        """

        response = Database.db.query_db(wquery, commit=True)

        if response["code"] == 200:
            self.logger.success(f"Loyalty points written")
        else:
            self.error_handler.add_error_v("Loyalty points could not be written")

    # Returns total number of loyalty points used.
    def get_loyalty_points_used(self, doc_id):
        query = f"""
        SELECT AMT FROM PS_DOC_PMT
        WHERE PAY_COD = 'LOYALTY' AND DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query)

        points_used = 0

        try:
            points_used = math.floor(float(response[0][0])) if response else 0
        except:
            pass

        return points_used

    # Write loyalty points.
    def write_loyalty(self, doc_id, cust_no, line_items: list[dict]):
        self.logger.info("Writing loyalty")
        points_earned = math.floor(self.write_lin_loy(doc_id, line_items))
        points_redeemed = self.get_loyalty_points_used(doc_id)

        self.write_ps_doc_hdr_loy_pgm(doc_id, cust_no, points_earned, points_redeemed)

    # Returns the total line discount amount summed together.
    def get_total_lin_disc(self, line_items: list[dict]):
        total = 0

        for line_item in line_items:
            total += float(line_item["DSC_AMT"])

        return total

    # Get the NCR Counterpoint API POST payload for a BigCommerce order.
    # Assigns order to cust_no
    def get_post_order_payload(self, cust_no: str, bc_order: dict = {}):
        self.discount_seq_no = 1
        self.total_discount_amount = 0
        self.total_gfc_amount = 0
        self.total_hdr_disc = 0
        self.total_lin_disc = 0
        self.total_lin_items = 0

        is_refund = self.is_refund(bc_order)

        bc_products = bc_order["products"]["url"]
        is_shipping = self.get_is_shipping(bc_order)
        shipping_cost = self.get_shipping_cost(bc_order)
        notes = self.get_notes(bc_order)

        payload = {
            "PS_DOC_HDR": {
                "STR_ID": "WEB",
                "STA_ID": "WEB",
                "DRW_ID": "1",
                "TKT_NUM": f"{ORDER_PREFIX}{bc_order["id"]}",
                "CUST_NO": cust_no,
                "TKT_TYP": "T",
                "DOC_TYP": "O",
                "USR_ID": "POS",
                "TAX_COD": "EXEMPT",
                "NORM_TAX_COD": "EXEMPT",
                "SHIP_VIA_COD": (
                    "CPC_FLAT" if is_refund else ("T" if is_shipping else "C")
                ),
                "PS_DOC_NOTE": notes,
                "PS_DOC_LIN": self.get_line_items_from_bc_products(bc_products),
                # "PS_DOC_GFC": self.get_gift_cards_from_bc_products(bc_products),
                "__PS_DOC_GFC__": self.get_gift_cards_from_bc_products(bc_products),
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
                    "TOT_TYP": "O",
                    "MISC_CHRG_NO": "1",
                    "MISC_TYP": "A",
                    "MISC_AMT": shipping_cost,
                }
            ]

        if is_refund:
            payload["PS_DOC_HDR"]["TAX_OVRD_REAS"] = "Y"

        self.sub_tot = sum(
            [
                float(line_item["EXT_PRC"])
                for line_item in payload["PS_DOC_HDR"]["PS_DOC_LIN"]
            ]
        )

        return payload

    # Check if the AR_CUST table has a customer with the provided cust_no
    def has_cust(self, cust_no):
        return customers.is_current_customer(cust_no)

    # Check if the AR_CUST table has a customer with the provided email and phone
    def has_cust_info(self, bc_order: dict):
        email = self.billing_or_shipping(bc_order, "email")
        phone = self.billing_or_shipping(bc_order, "phone")

        return OrderAPI.get_customer_from_info({"email": email, "phone": phone})

    # Returns the key from the billing_address
    def billing(self, bc_order: dict, key: str):
        try:
            return bc_order["billing_address"][key]
        except:
            return None

    # Return the key from the shipping_addresses
    def shipping(self, bc_order: dict, key: str):
        try:
            return bc_order["shipping_addresses"]["url"][0][key]
        except:
            return None

    # Return the key from the billing_address or shipping_addresses
    def billing_or_shipping(self, bc_order: dict, key: str):
        try:
            return self.billing(bc_order, key) or self.shipping(bc_order, key)
        except:
            return None

    # Create a new customer from a BigCommerce order.
    def create_new_customer(self, bc_order: dict):
        def bos(key: str):
            return self.billing_or_shipping(bc_order, key)

        def b(key: str):
            return self.billing(bc_order, key)

        def s(key: str):
            return self.shipping(bc_order, key)

        first_name = bos("first_name")
        last_name = bos("last_name")
        phone_number = bos("phone")
        email_address = bos("email")
        street_address = b("street_1")
        city = b("city")
        state = b("state")
        zip_code = b("zip")

        cust_no = customers.add_new_customer(
            first_name=first_name,
            last_name=last_name,
            phone_number=phone_number,
            email_address=email_address,
            street_address=street_address,
            city=city,
            state=state,
            zip_code=zip_code,
        )

        def write_shipping_adr():
            first_name = s("first_name")
            last_name = s("last_name")
            phone_number = s("phone")
            email_address = s("email")
            street_address = s("street_1")
            city = s("city")
            state = s("state")
            zip_code = s("zip")

            response = customers.update_customer_shipping(
                cust_no=cust_no,
                first_name=first_name,
                last_name=last_name,
                phone_number=phone_number,
                email_address=email_address,
                street_address=street_address,
                city=city,
                state=state,
                zip_code=zip_code,
            )

            if response["code"] == 200:
                self.logger.success("Shipping address updated")
            else:
                self.error_handler.add_error_v("Shipping address could not be updated")
                self.error_handler.add_error_v(response["message"])


        if (bc_order["shipping_address_count"] or 0) > 0:
            write_shipping_adr()

    # Update an existing customer from a BigCommerce order.
    def update_cust(self, bc_order: dict, cust_no: str | int):
        if not self.has_cust(cust_no):
            self.error_handler.add_error_v("Valid customer number is required")
            return

        def b(key: str):
            return self.billing(bc_order, key)

        def s(key: str):
            return self.shipping(bc_order, key)

        def write_cust():
            first_name = b("first_name")
            last_name = b("last_name")
            phone_number = b("phone")
            email_address = b("email")
            street_address = b("street_1")
            city = b("city")
            state = b("state")
            zip_code = b("zip")

            response = customers.update_customer(
                cust_no=cust_no,
                first_name=first_name,
                last_name=last_name,
                phone_number=phone_number,
                email_address=email_address,
                street_address=street_address,
                city=city,
                state=state,
                zip_code=zip_code,
            )

            if response["code"] == 200:
                self.logger.success("Customer updated")
            else:
                self.error_handler.add_error_v("Customer could not be updated")
                self.error_handler.add_error_v(response["message"])

        def write_shipping_adr():
            first_name = s("first_name")
            last_name = s("last_name")
            phone_number = s("phone")
            email_address = s("email")
            street_address = s("street_1")
            city = s("city")
            state = s("state")
            zip_code = s("zip")

            response = customers.update_customer_shipping(
                cust_no=cust_no,
                first_name=first_name,
                last_name=last_name,
                phone_number=phone_number,
                email_address=email_address,
                street_address=street_address,
                city=city,
                state=state,
                zip_code=zip_code,
            )

            if response["code"] == 200:
                self.logger.success("Shipping address updated")
            else:
                self.error_handler.add_error_v("Shipping address could not be updated")
                self.error_handler.add_error_v(response["message"])

        write_cust()
        if (bc_order["shipping_address_count"] or 0) > 0:
            write_shipping_adr()

    # This function will run the whole ordeal using the provided BigCommerce order_id.
    # cust_no_override is used to override the customer number for the order when posted to Counterpoint.
    # Session can be provided to use the same http session for all requests.
    @staticmethod
    def post_order(
        order_id: str | int,
        cust_no_override: str = None,
        session: requests.Session = requests.Session(),
    ):
        oapi = OrderAPI(session=session)

        bc_order = OrderAPI.get_order(order_id)

        if str(bc_order["payment_status"]).lower() in ["declined", ""]:
            oapi.error_handler.add_error_v("Order payment declined")
            oapi.error_handler.add_error_v(
                f"Payment status: '{bc_order["payment_status"]}'"
            )
            raise Exception("Order payment declined")

        cust_no = ""

        try:
            if not oapi.has_cust_info(bc_order):
                CounterPointAPI.logger.info("Creating new customer")
                oapi.create_new_customer(bc_order)
                cust_no = OrderAPI.get_cust_no(bc_order)
            else:
                CounterPointAPI.logger.info("Updating existing customer")
                cust_no = OrderAPI.get_cust_no(bc_order)
                oapi.update_cust(bc_order, cust_no)
        except:
            raise Exception("Customer could not be created/updated")

        if cust_no_override is None:
            if cust_no is None or cust_no == "" or not oapi.has_cust(cust_no):
                oapi.error_handler.add_error_v("Valid customer number is required")
                raise Exception("Valid customer number is required")
        else:
            cust_no = cust_no_override

        try:
            if bc_order["status"] == "Partially Refunded":
                oapi.post_partial_refund(cust_no=cust_no, bc_order=bc_order)
            else:
                oapi.post_bc_order(cust_no=cust_no, bc_order=bc_order)
        except Exception as e:
            oapi.error_handler.add_error_v("Order could not be posted")
            oapi.error_handler.add_error_v(e)

            query = f"""
            SELECT TOP 2 DOC_ID FROM PS_DOC_HDR
            WHERE CUST_NO = '{cust_no}'
            AND TKT_DT > '{datetime.now().strftime("%Y-%m-%d")}'
            ORDER BY LST_MAINT_DT DESC
            """

            oapi.logger.info("Attempting to cleanup order")

            response = Database.db.query_db(query)

            if response is not None and len(response) > 0 and len(response) < 3:
                for result in response:
                    doc_id = result[0]

                    query = f"""
                    DELETE FROM PS_DOC_HDR WHERE DOC_ID = '{doc_id}'
                    """

                    response = Database.db.query_db(query, commit=True)

                    if response["code"] == 200:
                        oapi.logger.success(f"Order {doc_id} deleted")
                    else:
                        oapi.error_handler.add_error_v(
                            f"Order {doc_id} could not be deleted"
                        )
                        oapi.error_handler.add_error_v(response["message"])
            else:
                oapi.error_handler.add_error_v("Could not cleanup order")

            raise e

    # Returns true if the provided ticket number exists in the PS_DOC_HDR table.
    def tkt_num_exists(self, tkt_num: str, suffix: str = "", index: int = 1):
        query = f"""
        SELECT TKT_NO FROM PS_DOC_HDR
        WHERE TKT_NO like '{tkt_num}{suffix}{index}'
        """

        response = Database.db.query_db(query)

        try:
            ticket_amt = len(response)

            if ticket_amt == 0:
                return False
            else:
                return True
        except:
            return False

    # Returns the refund index.
    # Refund index is the number at the end of the ticket number on a refund or partial refund.
    # Ex. 1151R1, 1151R2, 1150PR1, 1150PR2, etc.
    def get_refund_index(self, tkt_num: str, suffix: str = ""):
        index = 1
        found = False
        while not found:
            if self.tkt_num_exists(tkt_num=tkt_num, suffix=suffix, index=index):
                index += 1
            else:
                found = True

        return index

    # Post a partial refund to Counterpoint.
    def post_partial_refund(self, cust_no: str, bc_order: dict):
        self.logger.info("Posting order as partial refund")

        self.pr = True
        self.refund = True

        if cust_no is None or cust_no == "" or not self.has_cust(cust_no):
            self.error_handler.add_error_v("Valid customer number is required")
            return

        payload = self.get_post_order_payload(cust_no, bc_order)

        response = self.post_document(payload)

        if response.json()["ErrorCode"] == "SUCCESS":
            self.logger.success(
                f"Order {response.json()['Documents'][0]['DOC_ID']} created"
            )
        else:
            self.error_handler.add_error_v("Order could not be created")
            self.error_handler.add_error_v(response.content)

        try:
            doc_id = response.json()["Documents"][0]["DOC_ID"]
        except:
            self.error_handler.add_error_v("Document ID could not be retrieved")
            return

        try:
            if (
                payload["PS_DOC_HDR"]["TKT_NUM"]
                and payload["PS_DOC_HDR"]["TKT_NUM"] != ""
            ):
                refund_index = self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=PARTIAL_REFUND_SUFFIX)
                self.write_ticket_no(
                    doc_id,
                    f"{payload["PS_DOC_HDR"]["TKT_NUM"]}{PARTIAL_REFUND_SUFFIX}{refund_index}",
                )
        except:
            pass

        self.write_loyalty(doc_id, cust_no, payload["PS_DOC_HDR"]["PS_DOC_LIN"])
        self.write_doc_discounts(doc_id, bc_order)
        self.write_doc_disc(doc_id, payload["PS_DOC_HDR"]["PS_DOC_LIN"])

        if self.is_refund(bc_order):
            self.refund_writes(doc_id, payload, bc_order)

        self.more_writes(doc_id, payload, bc_order)

        self.logger.success(f"Order {doc_id} created")

        return response

    # Post an order/refund to Counterpoint.
    def post_bc_order(self, cust_no: str, bc_order: dict):
        self.logger.info("Posting order")

        if cust_no is None or cust_no == "" or not self.has_cust(cust_no):
            self.error_handler.add_error_v("Valid customer number is required")
            return

        payload = self.get_post_order_payload(cust_no, bc_order)

        response = self.post_document(payload)

        if response.json()["ErrorCode"] == "SUCCESS":
            self.logger.success(
                f"Order {response.json()['Documents'][0]['DOC_ID']} created"
            )
        else:
            self.error_handler.add_error_v("Order could not be created")
            raise Exception(response.content)

        try:
            doc_id = response.json()["Documents"][0]["DOC_ID"]
        except:
            self.error_handler.add_error_v("Document ID could not be retrieved")
            raise Exception("Document ID could not be retrieved")

        # WRITE TICKET NUMBER

        try:
            if (
                payload["PS_DOC_HDR"]["TKT_NUM"]
                and payload["PS_DOC_HDR"]["TKT_NUM"] != ""
            ):
                if self.is_refund(bc_order):
                    refund_index = self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=REFUND_SUFFIX)
                    self.write_ticket_no(
                        doc_id,
                        f"{payload["PS_DOC_HDR"]["TKT_NUM"]}{REFUND_SUFFIX}{refund_index}",
                    )
                else:
                    self.write_ticket_no(
                        doc_id,
                        f"{payload["PS_DOC_HDR"]["TKT_NUM"]}",
                    )
        except:
            pass

        # WRITE PS_DOC_GFC

        if len(payload["PS_DOC_HDR"]["__PS_DOC_GFC__"]) > 0 and not self.is_refund(bc_order):
            for gift_card in payload["PS_DOC_HDR"]["__PS_DOC_GFC__"]:
                query = f"""
                INSERT INTO PS_DOC_GFC
                (DOC_ID, GFC_COD, GFC_NO, AMT, LIN_SEQ_NO, DESCR, CREATE_AS_STC, GFC_SEQ_NO)
                VALUES
                ('{doc_id}', '{gift_card["GFC_COD"]}', '{gift_card["GFC_NO"]}', {gift_card["AMT"]}, {gift_card["LIN_SEQ_NO"]}, '{gift_card["DESCR"]}', '{gift_card["CREATE_AS_STC"]}', {gift_card["GFC_SEQ_NO"]})
                """

                response = Database.db.query_db(query, commit=True)

                if response["code"] == 200:
                    self.logger.success("Gift card written")
                else:
                    self.error_handler.add_error_v("Gift card could not be written")
                    self.error_handler.add_error_v(response["message"])

                def commit_query(query):
                    response = Database.db.query_db(query, commit=True)
                    return response

                def get_next_seq_no():
                    query = f"""
                    SELECT MAX(SEQ_NO) FROM SY_GFC_ACTIV
                    WHERE GFC_NO = '{gift_card["GFC_NO"]}'
                    """

                    response = Database.db.query_db(query)

                    try:
                        return int(response[0][0]) + 1
                    except:
                        return 1

                def add_gfc_bal(amt: float | int):
                    current_date = datetime.now().strftime("%Y-%m-%d")

                    tkt_no = payload['PS_DOC_HDR']['TKT_NUM']

                    # if self.is_refund():
                    #     refund_index = self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=REFUND_SUFFIX)
                    #     tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{REFUND_SUFFIX}{refund_index}"
                    # else:
                    #     tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}"

                    r = commit_query(
                        f"""
                        INSERT INTO SY_GFC
                        (GFC_NO, DESCR, DESCR_UPR, ORIG_DAT, ORIG_STR_ID, ORIG_STA_ID, ORIG_DOC_NO, ORIG_CUST_NO, GFC_COD, NO_EXP_DAT, ORIG_AMT, CURR_AMT, CREATE_METH, LIAB_ACCT_NO, RDM_ACCT_NO, RDM_METH, FORF_ACCT_NO, IS_VOID, LST_ACTIV_DAT, LST_MAINT_DT, LST_MAINT_USR_ID, ORIG_DOC_ID, ORIG_BUS_DAT, RS_STAT)
                        VALUES
                        ('{gift_card["GFC_NO"]}', 'Gift Certificate', 'GIFT CERTIFICATE', '{current_date}', 'WEB', 'WEB', '{tkt_no}', '{cust_no}', 'GC', 'Y', {amt}, {amt}, 'G', 2090, 2090, '!', 8510, 'N', '{current_date}', GETDATE(), 'POS', '{doc_id}', '{current_date}', 0)
                        """
                    )

                    if r["code"] == 200:
                        self.logger.success(f"Gift card balance updated")
                    else:
                        self.error_handler.add_error_v(
                            "Gift card balance could not be updated"
                        )
                        self.error_handler.add_error_v(r["message"])

                    r = commit_query(
                        f"""
                        INSERT INTO SY_GFC_ACTIV
                        (GFC_NO, SEQ_NO, DAT, STR_ID, STA_ID, DOC_NO, ACTIV_TYP, AMT, LST_MAINT_DT, LST_MAINT_USR_ID, DOC_ID)
                        VALUES
                        ('{gift_card["GFC_NO"]}', {get_next_seq_no()}, '{current_date}', 'WEB', 'WEB', '{tkt_no}', 'I', {amt}, GETDATE(), 'POS', '{doc_id}')
                        """
                    )

                    if r["code"] == 200:
                        self.logger.success(f"Gift card balance updated")
                    else:
                        self.error_handler.add_error_v(
                            "Gift card balance could not be updated"
                        )
                        self.error_handler.add_error_v(r["message"])
        
                add_gfc_bal(gift_card["AMT"])

        self.write_loyalty(doc_id, cust_no, payload["PS_DOC_HDR"]["PS_DOC_LIN"])
        self.write_doc_discounts(doc_id, bc_order)
        self.write_doc_disc(doc_id, payload["PS_DOC_HDR"]["PS_DOC_LIN"])

        if self.is_refund(bc_order):
            self.refund_writes(doc_id, payload, bc_order)

        self.more_writes(doc_id, payload, bc_order)

        self.logger.success(f"Order {doc_id} created")

        return response

    # Writes the correct ticket number to the document.
    def write_ticket_no(self, doc_id, tkt_no):
        tables = ["PS_DOC_HDR", "PS_DOC_LIN", "PS_DOC_PMT"]

        for table in tables:
            query = f"""
            UPDATE {table}
            SET TKT_NO = '{tkt_no}'
            WHERE DOC_ID = '{doc_id}'
            """

            response = Database.db.query_db(query, commit=True)

            if response["code"] == 200:
                self.logger.success(f"Ticket number updated.")
            else:
                self.error_handler.add_error_v("Ticket number could not be updated")
                self.error_handler.add_error_v(response["message"])

    # Writes to several tables in Counterpoint.
    # Necessary to process refunds correctly.
    # This function is called before more_writes on refunds and partial refunds.
    def refund_writes(self, doc_id, payload, bc_order):
        self.logger.info("Writing refund data")

        if self.is_pr():
            sub_tot = 0

            for line_item in payload["PS_DOC_HDR"]["PS_DOC_LIN"]:
                sub_tot += float(line_item["EXT_PRC"])

            bc_order["subtotal_ex_tax"] = float(
                bc_order["refunded_amount"] or 0
            ) + float(self.total_discount_amount or 0) / float(
                bc_order["items_total"] or 1
            )
            bc_order["subtotal_inc_tax"] = float(
                bc_order["refunded_amount"] or 0
            ) + float(self.total_discount_amount or 0) / float(
                bc_order["items_total"] or 1
            )

            bc_order["total_ex_tax"] = float(bc_order["refunded_amount"] or 0)
            bc_order["total_inc_tax"] = float(bc_order["refunded_amount"] or 0)

        def commit_query(query):
            response = Database.db.query_db(query, commit=True)

            return response



        # REMOVE GIFT CARD BALANCE
        if len(payload["PS_DOC_HDR"]["__PS_DOC_GFC__"]) > 0:
            for gift_card in payload["PS_DOC_HDR"]["__PS_DOC_GFC__"]:
                card_no = gift_card["GFC_NO"]
                def get_gfc_bal():
                    query = f"""
                    SELECT CURR_AMT FROM SY_GFC
                    WHERE GFC_NO = '{card_no}'
                    """

                    response = Database.db.query_db(query)
                    try:
                        return float(response[0][0])
                    except:
                        return 0

                def get_bal_diff(num: float | int):
                    return num - get_gfc_bal()

                def get_next_seq_no():
                    query = f"""
                    SELECT MAX(SEQ_NO) FROM SY_GFC_ACTIV
                    WHERE GFC_NO = '{gift_card["GFC_NO"]}'
                    """

                    response = Database.db.query_db(query)

                    try:
                        return int(response[0][0]) + 1
                    except:
                        return 1

                def add_gfc_bal(amt: float | int):
                    current_date = datetime.now().strftime("%Y-%m-%d")

                    tkt_no = payload['PS_DOC_HDR']['TKT_NUM']

                    if self.is_pr():
                        refund_index = int(self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=PARTIAL_REFUND_SUFFIX)) - 1
                        tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{PARTIAL_REFUND_SUFFIX}{refund_index}"
                    else:
                        refund_index = int(self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=REFUND_SUFFIX)) - 1
                        tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{REFUND_SUFFIX}{refund_index}"

                    r = commit_query(
                        f"""
                        INSERT INTO SY_GFC_ACTIV
                        (GFC_NO, SEQ_NO, DAT, STR_ID, STA_ID, DOC_NO, ACTIV_TYP, AMT, LST_MAINT_DT, LST_MAINT_USR_ID, DOC_ID)
                        VALUES
                        ('{card_no}', {get_next_seq_no()}, '{current_date}', 'WEB', 'WEB', '{tkt_no}', 'R', {amt}, GETDATE(), 'POS', '{doc_id}')
                        """
                    )

                    if r["code"] == 200:
                        self.logger.success(f"Gift card balance updated")
                    else:
                        self.error_handler.add_error_v(
                            "Gift card balance could not be updated"
                        )
                        self.error_handler.add_error_v(r["message"])

                    r = commit_query(
                        f"""
                        UPDATE SY_GFC
                        SET CURR_AMT = {0}
                        WHERE GFC_NO = '{card_no}'
                        """
                    )

                    if r["code"] == 200:
                        self.logger.success(f"Gift card balance updated")
                    else:
                        self.error_handler.add_error_v(
                            "Gift card balance could not be updated"
                        )
                        self.error_handler.add_error_v(r["message"])

                add_gfc_bal(get_bal_diff(0))


        r = commit_query(
            f"""
            UPDATE PS_DOC_PMT
            SET AMT = {-(float(bc_order["total_inc_tax"] or 0))},
            HOME_CURNCY_AMT = {-(float(bc_order["total_inc_tax"] or 0))} 
            WHERE DOC_ID = '{doc_id}'
            """
        )

        if r["code"] == 200:
            self.logger.success("Updated payment")
        else:
            self.error_handler.add_error_v("Payment could not be updated")
            self.error_handler.add_error_v(r["message"])

        for payment in payload["PS_DOC_HDR"]["PS_DOC_PMT"]:
            if payment["PAY_COD"] == "GC":
                remaining_bal = float(payment["REMAINING_BAL"])
                card_no = payment["CARD_NO"]

                def get_gfc_bal():
                    query = f"""
                    SELECT CURR_AMT FROM SY_GFC
                    WHERE GFC_NO = '{card_no}'
                    """

                    response = Database.db.query_db(query)
                    try:
                        return float(response[0][0])
                    except:
                        return 0

                def get_bal_diff():
                    return remaining_bal - get_gfc_bal()

                def get_next_seq_no():
                    query = f"""
                    SELECT MAX(SEQ_NO) FROM SY_GFC_ACTIV
                    WHERE GFC_NO = '{card_no}'
                    """

                    response = Database.db.query_db(query)

                    try:
                        return int(response[0][0]) + 1
                    except:
                        return 1

                def add_gfc_bal(amt: float | int):
                    current_date = datetime.now().strftime("%Y-%m-%d")

                    tkt_no = ""

                    if self.is_pr():
                        refund_index = self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=PARTIAL_REFUND_SUFFIX)
                        tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{PARTIAL_REFUND_SUFFIX}{refund_index}"
                    else:
                        refund_index = self.get_refund_index(tkt_num=payload["PS_DOC_HDR"]["TKT_NUM"], suffix=REFUND_SUFFIX)
                        tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{REFUND_SUFFIX}{refund_index}"

                    r = commit_query(
                        f"""
                        INSERT INTO SY_GFC_ACTIV
                        (GFC_NO, SEQ_NO, DAT, STR_ID, STA_ID, DOC_NO, ACTIV_TYP, AMT, LST_MAINT_DT, LST_MAINT_USR_ID, DOC_ID)
                        VALUES
                        ('{card_no}', {get_next_seq_no()}, '{current_date}', 'WEB', 'WEB', '{tkt_no}', 'R', {amt}, GETDATE(), 'POS', '{doc_id}')
                        """
                    )

                    if r["code"] == 200:
                        self.logger.success(f"Gift card balance updated")
                    else:
                        self.error_handler.add_error_v(
                            "Gift card balance could not be updated"
                        )
                        self.error_handler.add_error_v(r["message"])

                    r = commit_query(
                        f"""
                        UPDATE SY_GFC
                        SET CURR_AMT = {remaining_bal}
                        WHERE GFC_NO = '{card_no}'
                        """
                    )

                    if r["code"] == 200:
                        self.logger.success(f"Gift card balance updated")
                    else:
                        self.error_handler.add_error_v(
                            "Gift card balance could not be updated"
                        )
                        self.error_handler.add_error_v(r["message"])

                add_gfc_bal(get_bal_diff())

            if payment["PAY_COD"] == "LOYALTY":
                query = f"""
                UPDATE {creds.ar_cust_table}
                SET LOY_PTS_BAL = LOY_PTS_BAL + {abs(math.floor(float(payment["AMT"])))}
                WHERE CUST_NO = '{payload["PS_DOC_HDR"]["CUST_NO"]}'
                """

                response = Database.db.query_db(query, commit=True)

                if response["code"] == 200:
                    self.logger.success("Loyalty points added")
                else:
                    self.error_handler.add_error_v("Loyalty points could not be added")
                    self.error_handler.add_error_v(response["message"])

        r = commit_query(
            f"""
            UPDATE PS_DOC_PMT_APPLY
            SET AMT = {-(float(bc_order["total_inc_tax"] or 0))},
            HOME_CURNCY_AMT = {-(float(bc_order["total_inc_tax"] or 0))}
            WHERE DOC_ID = '{doc_id}' AND PMT_SEQ_NO in (
                SELECT PMT_SEQ_NO FROM PS_DOC_PMT WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'BIG'
            )
            """
        )

        if r["code"] == 200:
            self.logger.success("Updated payment application")
        else:
            self.error_handler.add_error_v("Payment application could not be updated")
            self.error_handler.add_error_v(r["message"])

        def invert_line_qty(line_item: dict, index: int):
            qty = -line_item["QTY_SOLD"]

            r = commit_query(
                f"""
                UPDATE PS_DOC_LIN
                SET QTY_SOLD = {qty},
                EXT_PRC = {qty * line_item["PRC"]},
                EXT_COST = {qty * line_item["EXT_COST"]}
                WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                """
            )

            if r["code"] == 200:
                self.logger.success(f"Line {index} inverted")
            else:
                self.error_handler.add_error_v(f"Line {index} could not be inverted")
                self.error_handler.add_error_v(r["message"])

        for i, line_item in enumerate(payload["PS_DOC_HDR"]["PS_DOC_LIN"], start=1):
            invert_line_qty(line_item, i)

        def get_value(table, column, index):
            query = f"""
            SELECT {column} FROM {table}
            WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
            """

            response = Database.db.query_db(query)

            try:
                return float(response[0][0]) if response else None
            except Exception as e:
                self.error_handler.add_error_v(
                    f"[{table}] Line {index} {column} could not be retrieved"
                )
                raise e

        def set_value(table, column, value, index):
            r = commit_query(
                f"""
                UPDATE {table}
                SET {column} = {value}
                WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                """
            )

            if r["code"] == 200:
                self.logger.success(f"[{table}] Line {index} {column} set to {value}")
            else:
                self.error_handler.add_error_v(
                    f"[{table}] Line {index} {column} could not be set to {value}"
                )
                self.error_handler.add_error_v(r["message"])

        def negative_column(table: str, column: str, index: int):
            set_value(table, column, -get_value(table, column, index), index)

        for i, line_item in enumerate(payload["PS_DOC_HDR"]["PS_DOC_LIN"], start=1):
            for column in [
                "EXT_COST",
                "ORIG_QTY",
                "GROSS_EXT_PRC",
                "GROSS_DISP_EXT_PRC",
                "CALC_EXT_PRC",
            ]:
                negative_column("PS_DOC_LIN", column, i)

            def set_value_lin(column, value):
                set_value("PS_DOC_LIN", column, value, i)

            def get_value_lin(column):
                return get_value("PS_DOC_LIN", column, i)

            set_value_lin("QTY_ENTD", 0)
            set_value_lin("QTY_TO_REL", get_value_lin("QTY_SOLD"))
            set_value_lin("QTY_TO_LEAVE", 0)

            def set_value_prc(column, value):
                set_value("PS_DOC_LIN_PRICE", column, value, i)

            def get_value_prc(column):
                return get_value("PS_DOC_LIN_PRICE", column, i)

            def invert_prc(column):
                set_value_prc(column, -get_value_prc(column))

            set_value_prc("PRC_RUL_SEQ_NO", -1)
            set_value_prc("PRC_BRK_DESCR", "'I'")
            invert_prc("QTY_PRCD")

    # Updates the users loyalty points
    def redeem_loyalty_pmts(self, doc_id, payload, bc_order):
        for payment in payload["PS_DOC_HDR"]["PS_DOC_PMT"]:
            if payment["PAY_COD"] == "LOYALTY":
                query = f"""
                UPDATE {creds.ar_cust_table}
                SET LOY_PTS_BAL = LOY_PTS_BAL - {abs(math.floor(float(payment["AMT"])))}
                WHERE CUST_NO = '{payload["PS_DOC_HDR"]["CUST_NO"]}'
                """

                response = Database.db.query_db(query, commit=True)

                if response["code"] == 200:
                    self.logger.success("Loyalty points added")
                else:
                    self.error_handler.add_error_v("Loyalty points could not be added")
                    self.error_handler.add_error_v(response["message"])

    # Writes to several tables in Counterpoint.
    def more_writes(self, doc_id, payload, bc_order):
        if not self.is_refund():
            self.redeem_loyalty_pmts(doc_id, payload, bc_order)

        self.logger.info("Writing tables")

        def convert_date_string_to_datetime(date_string):
            date = datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S %z")

            date = date.replace(tzinfo=timezone.utc).astimezone(tz=None)

            return date

        def convert_datetime_to_date_string(date):
            date_string = date.strftime("%Y-%m-%d %H:%M:%S.%f")
            return date_string[:-3]

        def double_convert_date(date_str):
            date = convert_date_string_to_datetime(date_str)
            return convert_datetime_to_date_string(date)

        date = double_convert_date(bc_order["date_created"])

        query = f"""
        UPDATE PS_DOC_HDR
        SET TKT_DT = '{date}'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Date updated")
        else:
            self.error_handler.add_error_v("Date could not be updated")
            self.error_handler.add_error_v(response["message"])

        def get_tndr():
            total = 0

            for payment in payload["PS_DOC_HDR"]["PS_DOC_PMT"]:
                total += abs(float(payment["AMT"]))

            return total

        tot_tndr = 0 if self.is_refund() else get_tndr()

        query = f"""
        DELETE FROM PS_DOC_HDR_TOT
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Total removed")
        else:
            self.error_handler.add_error_v("Total could not be removed")
            self.error_handler.add_error_v(response["message"])

        sub_tot = float(bc_order["subtotal_ex_tax"] or 0)
        document_discount = float(self.total_discount_amount or 0)
        gfc_amount = float(self.total_gfc_amount)
        shipping_amt = float(bc_order["base_shipping_cost"] or 0)

        tot_ext_cost = 0

        for line_item in payload["PS_DOC_HDR"]["PS_DOC_LIN"]:
            try:
                self.logger.info(f"Getting cost for {line_item['ITEM_NO']}")

                query = f"""
                SELECT LST_COST FROM IM_ITEM
                WHERE ITEM_NO = '{line_item["ITEM_NO"]}'
                """

                response = Database.db.query_db(query)

                tot_ext_cost += float(response[0][0])
            except Exception as e:
                self.error_handler.add_error_v("Could not get cost")
                self.error_handler.add_error_v(e)

        if self.is_refund(bc_order):
            self.total_lin_disc = abs(self.total_lin_disc)
            query = f"""
            INSERT INTO PS_DOC_HDR_TOT
            (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
            VALUES
            ('{doc_id}', 'S', 0, '!', 0, {len(payload["PS_DOC_HDR"]["PS_DOC_LIN"])}, 0, 0, {-(sub_tot - self.total_discount_amount / float(bc_order["items_total"])) if self.is_pr() else -(sub_tot - document_discount)}, 0, {-tot_ext_cost}, {shipping_amt}, 0, 0, {tot_tndr}, {(sub_tot - self.total_discount_amount / float(bc_order["items_total"])) if self.is_pr() else (sub_tot - self.total_discount_amount)}, 0, 0, {(-(sub_tot - (self.total_discount_amount / float(bc_order["items_total"])))) if self.is_pr() else -(sub_tot - self.total_discount_amount)}, 0, {0 if self.is_pr() else -self.total_hdr_disc}, {self.total_lin_disc + self.total_hdr_disc if self.is_pr() else self.total_lin_disc}, 0, 0)
            """
        else:
            query = f"""
            INSERT INTO PS_DOC_HDR_TOT
            (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
            VALUES
            ('{doc_id}', 'S', 0, '!', 0, {len(payload["PS_DOC_HDR"]["PS_DOC_LIN"])}, {gfc_amount}, 0, {sub_tot - document_discount}, 0, {tot_ext_cost}, {shipping_amt}, 0, 0, {tot_tndr}, 0, 0, 0, {tot_tndr}, 0, {self.total_hdr_disc}, {self.total_lin_disc}, {sub_tot}, 0)
            """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Total written")
        else:
            self.error_handler.add_error_v("Total could not be written")
            self.error_handler.add_error_v(response["message"])

        if self.is_refund(bc_order):
            query = f"""
            UPDATE PS_DOC_LIN
            SET LIN_TYP = 'R'
            WHERE DOC_ID = '{doc_id}'
            """
        else:
            query = f"""
            UPDATE PS_DOC_LIN
            SET LIN_TYP = 'S'
            WHERE DOC_ID = '{doc_id}'
            """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Updated line types")
        else:
            self.error_handler.add_error_v("Line types could not be updated")
            self.error_handler.add_error_v(response["message"])

        query = f"""
        UPDATE PS_DOC_PMT_APPLY
        SET APPL_TYP = 'S'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Updated payment application types")
        else:
            self.error_handler.add_error_v(
                "Payment application types could not be updated"
            )
            self.error_handler.add_error_v(response["message"])

        if self.is_refund(bc_order):
            query = f"""
            UPDATE PS_DOC_HDR
            SET RET_LINS = {self.total_lin_items}
            WHERE DOC_ID = '{doc_id}'
            """
        else:
            query = f"""
            UPDATE PS_DOC_HDR
            SET SAL_LINS = {self.total_lin_items}
            WHERE DOC_ID = '{doc_id}'
            """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Updated line items")
        else:
            self.error_handler.add_error_v("Line items could not be updated")
            self.error_handler.add_error_v(response["message"])

        if self.is_refund(bc_order):
            pass
        else:
            query = f"""
            UPDATE PS_DOC_HDR
            SET TO_REL_LINS = {self.total_lin_items}
            WHERE DOC_ID = '{doc_id}'
            """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Updated line items to release")
        else:
            self.error_handler.add_error_v("Line items to release could not be updated")
            self.error_handler.add_error_v(response["message"])

        if self.is_refund(bc_order):
            query = f"""
            UPDATE PS_DOC_HDR
            SET RET_LIN_TOT = {-(float(bc_order["subtotal_ex_tax"] or 0))}
            WHERE DOC_ID = '{doc_id}'
            """
        else:
            query = f"""
            UPDATE PS_DOC_HDR
            SET SAL_LIN_TOT = {float(bc_order["subtotal_ex_tax"] or 0)}
            WHERE DOC_ID = '{doc_id}'
            """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Updated line total")
        else:
            self.error_handler.add_error_v("Line total could not be updated")
            self.error_handler.add_error_v(response["message"])

        def get_orig_doc_id():
            query = f"""
            SELECT ORIG_DOC_ID FROM PS_DOC_HDR_ORIG_DOC WHERE DOC_ID = '{doc_id}'
            """

            response = Database.db.query_db(query)

            try:
                return response[0][0]
            except:
                return None

        if not self.is_refund(bc_order):
            query = f"""
            UPDATE PS_DOC_HDR_MISC_CHRG
            SET DOC_ID = '{doc_id}',
            TOT_TYP = 'S'
            WHERE DOC_ID = '{get_orig_doc_id()}'
            """
        else:
            query = f"""
            UPDATE PS_DOC_HDR_MISC_CHRG
            SET DOC_ID = '{doc_id}',
            TOT_TYP = 'S',
            MISC_AMT = {-(float(bc_order["base_shipping_cost"] or 0))}
            WHERE DOC_ID = '{get_orig_doc_id()}'
            """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Updated shipping charge")
        else:
            self.error_handler.add_error_v("Shipping charge could not be updated")
            self.error_handler.add_error_v(response["message"])

        self.cleanup(doc_id)

    # Remove the original document and the reference to the original document
    # The original document is the ORDER document that was created at the NCR Counterpoint API POST.
    # The original document is different than the final TICKET document that we see and is not needed.
    def cleanup(self, doc_id):
        self.logger.info("Cleaning up")

        query = f"""
        DELETE FROM PS_DOC_HDR
        WHERE DOC_ID in (
            SELECT ORIG_DOC_ID FROM PS_DOC_HDR_ORIG_DOC
            WHERE DOC_ID = '{doc_id}'
        )
        """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Removed original document")
        else:
            self.error_handler.add_error_v("Original document could not be removed")
            self.error_handler.add_error_v(response["message"])

        query = f"""
        DELETE FROM PS_DOC_HDR_ORIG_DOC
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.db.query_db(query, commit=True)

        if response["code"] == 200:
            self.logger.success("Removed original document reference")
        else:
            self.error_handler.add_error_v(
                "Original document reference could not be removed"
            )
            self.error_handler.add_error_v(response["message"])

    # Get customer from email and phone number
    @staticmethod
    def get_customer_from_info(user_info):
        return customers.lookup_customer(
            email_address=user_info["email"], phone_number=user_info["phone"]
        )

    # Get the customer's phone number from the BigCommerce order
    @staticmethod
    def get_cust_phone(bc_order: dict):
        try:
            phone = bc_order["billing_address"]["phone"]

            if phone is None or phone == "":
                phone = bc_order["shipping_addresses"]["url"][0]["phone"]

            return phone
        except:
            return ""

    # Get the customer's email address from the BigCommerce order
    @staticmethod
    def get_cust_email(bc_order: dict):
        try:
            email = bc_order["billing_address"]["email"]

            if email is None or email == "":
                email = bc_order["shipping_addresses"]["url"][0]["email"]

            return email
        except:
            return ""

    # Get the customer's number from the BigCommerce order
    @staticmethod
    def get_cust_no(bc_order: dict):
        user_info = {
            "email": OrderAPI.get_cust_email(bc_order),
            "phone": OrderAPI.get_cust_phone(bc_order),
        }

        cust_no = OrderAPI.get_customer_from_info(user_info)

        return cust_no

    # Get the BigCommerce order object for a given order ID.
    @staticmethod
    def get_order(order_id: str | int):
        url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/orders/{order_id}"
        order = JsonTools.get_json(url)

        order["transactions"] = JsonTools.get_json(
            f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/orders/{order_id}/transactions"
        )
        order = JsonTools.unpack(order)

        return order


# This class is used to parse the BigCommerce order response.
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
                if key in ["products", "coupons", "shipping_addresses"]:
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
            elif key == "gift_certificate_id" and value is not None:
                try:
                    myjson = JsonTools.get_json(
                        f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/gift_certificates/{value}"
                    )
                    obj[key] = myjson
                except:
                    pass

        return obj
