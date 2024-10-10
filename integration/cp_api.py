from setup import creds
import requests
import json
import math
from datetime import datetime, timezone
from database import Database
from setup.error_handler import ProcessInErrorHandler
from customer_tools import customers
from integration.shopify_api import Shopify
from integration.models.shopify_orders import ShopifyOrder, LineItem
from integration.models.cp_orders import CPLineItem, CPNote, CPGiftCard
from integration.models.payments import GCPayment
from traceback import format_exc as tb


ORDER_PREFIX = 'S'
REFUND_SUFFIX = 'R'
PARTIAL_REFUND_SUFFIX = 'PR'
LOYALTY_EXCLUSIONS = ['SERVICE', 'DELIVERY']


# This class is primarily used to interact with the NCR Counterpoint API
# If you need documentation on the API, good luck.
# https://github.com/NCRCounterpointAPI/APIGuide/blob/master/Endpoints/POST_Document.md
class CounterPointAPI:
    logger = ProcessInErrorHandler.logger
    error_handler = ProcessInErrorHandler.error_handler

    def __init__(self, session: requests.Session = requests.Session()):
        self.base_url = creds.Counterpoint.API.server
        self.session = session
        self.logger = CounterPointAPI.logger
        self.error_handler = CounterPointAPI.error_handler
        self.payload: dict = None

        self.get_headers = {
            'Authorization': f'Basic {creds.Counterpoint.API.user}',
            'APIKey': creds.Counterpoint.API.key,
            'Accept': 'application/json',
        }

        self.post_headers = {
            'Authorization': f'Basic {creds.Counterpoint.API.user}',
            'APIKey': creds.Counterpoint.API.key,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    def get(self, url):
        response = self.session.get(url, headers=self.get_headers, verify=False)

        return response

    def post(self, url):
        response = self.session.post(url, headers=self.post_headers, json=self.payload, verify=False)

        return response


# This class is used to interact with the NCR Counterpoint API's Document endpoint
class DocumentAPI(CounterPointAPI):
    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)
        self.base_url = f'{self.base_url}Document'

    def get_document(self, doc_id):
        url = f'{self.base_url}/{doc_id}'

        response = self.get(url)

        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)

        return pretty

    def post_document(self, payload) -> dict:
        url = self.base_url

        response = self.post(url, payload=self.payload)

        return response.json()


class OrderAPI(DocumentAPI):
    """This class is used to interact with the NCR Counterpoint API's Document endpoint
    and create orders and refunds."""

    def __init__(self, order_id: int, session: requests.Session = requests.Session(), verbose: bool = False):
        self.verbose: bool = verbose
        super().__init__(session=session)
        self.order: ShopifyOrder = Shopify.Order.get(order_id=order_id)
        self.doc_id: str = None
        self.cp_tkt_no: str = None  # 1133
        self.cp_tkt_no_full: str = None  # 1133R1 or S1133
        self.store_id: str = self.get_store_id()
        self.station_id: str = self.get_station_id()
        self.drawer_id: str = self.get_drawer_id()
        self.discount_seq_no: int = 1
        self.total_discount_amount: float = 0
        self.total_gfc_amount: float = 0
        self.total_hdr_disc: float = 0
        self.total_lin_disc: float = 0
        self.is_refund: bool = self.order.status in ['Refunded', 'Partially Refunded']
        self.is_partial_refund: bool = self.order.status == 'Partially Refunded'
        self.refund_index: int = None
        self.total_tender: float = 0
        self.has_loyalty_payment: bool = False
        self.has_gift_card_payment: bool = False
        self.total_lin_items: int = 0
        self.line_item_length: int = 0

    def get_line_items(self) -> list[dict]:
        """Returns a list of line items dicts from a order."""
        line_items = []
        for item in self.order.items:
            if item.type == 'physical':
                line_items.append(CPLineItem(item).payload)
                self.total_lin_items += 1
        return line_items

    def get_gift_card_purchases(self) -> list[dict]:
        """Returns a list of gift cards from a list of products from a BigCommerce order."""

        gift_cards = []

        for i, product in enumerate(self.order.items, start=1):
            if self.is_partial_refund and float(product.quantity_refunded) == 0:
                continue

            if product.type == 'giftcertificate':
                gift_cards.append(CPGiftCard(product, self.line_item_length + 1, i).payload)
                self.line_item_length += 1
                self.total_gfc_amount += float(product.base_price)

        return gift_cards

    def get_payments(self) -> list[dict]:
        """Returns a list of payments from a BigCommerce order."""
        order = self.order
        payments = [
            {
                'AMT': order.total_inc_tax - order.store_credit_amount,
                'PAY_COD': 'SHOP',
                'FINAL_PMT': 'N',
                'PMT_LIN_TYP': 'C' if self.is_refund else 'T',
            }
        ]

        if order.store_credit_amount > 0:
            payments.append(
                {
                    'AMT': order.store_credit_amount,
                    'PAY_COD': 'LOYALTY',
                    'FINAL_PMT': 'N',
                    'PMT_LIN_TYP': 'C' if self.is_refund else 'T',
                }
            )

        def get_gift_card_payments() -> list[dict]:
            """Returns a list of gift card payments from a order.
            NOTE: Gift Card Payments are not supported in our current setup."""
            gfc_payments = []

            if not self.order.transactions:
                return gfc_payments

            for pay_method in self.order.transactions['data']:
                if pay_method['method'] == 'gift_certificate':
                    gfc_payments.append(GCPayment(pay_method))

            return gfc_payments

        payments += get_gift_card_payments()

        return payments

    def get_notes(self) -> list[dict]:
        """Returns a list of notes from a order."""
        notes = []
        if self.order.customer_message:
            notes.append(CPNote(self.order).payload)
        return notes

    def write_discounts(self, doc_id, line_items: list[CPLineItem]):
        """Write discounts to the PS_DOC_DISC table."""

        def write_h(doc_id, disc_seq_no: int, disc_amt: float, lin_seq_no: int = None):
            """Helper function to write discounts to the PS_DOC_DISC table."""
            if self.is_refund:
                disc_amt = -disc_amt

            if not lin_seq_no:
                self.total_hdr_disc += disc_amt
            else:
                self.total_lin_disc += disc_amt

            self.total_discount_amount += abs(disc_amt)

            Database.CP.Discount.write_discount(
                doc_id=doc_id,
                disc_seq_no=disc_seq_no,
                disc_amt=disc_amt,
                disc_id='100000000000331' if lin_seq_no else '100000000000330',
                apply_to='L' if lin_seq_no else 'H',  # L for line, H for header
                disc_type='A',  # Amount,
                disc_pct=0,
                disc_amt_shipped=0,
                lin_seq_no=lin_seq_no,
            )

        self.logger.info('Writing discounts')

        # Processing order discounts
        if self.order.header_discount:
            self.logger.info(f'Writing header discount: ${self.order.header_discount}')
            write_h(doc_id, disc_seq_no=self.discount_seq_no, disc_amt=self.order.header_discount)
            self.discount_seq_no += 1

        # Processing line discounts
        for i, item in enumerate(line_items, start=1):
            if item.discount_amount:
                self.logger.info(f'Writing line discount: ${item.discount_amount}')
                write_h(doc_id, disc_seq_no=self.discount_seq_no, disc_amt=item.discount_amount, lin_seq_no=i)
                self.discount_seq_no += 1

        self.total_lin_disc = abs(self.total_lin_disc) if self.is_refund else self.total_lin_disc

    def write_loyalty(self, line_items: list[dict]):
        self.logger.info('Writing loyalty')

        cust_no = self.order.customer.cp_id

        def write_lin_loy(line_items: list[dict]) -> int:
            points = 0

            def write_loyalty_line(line_item: dict, lin_seq_no: int):
                if line_item['sku'] in LOYALTY_EXCLUSIONS:
                    return 0

                points_earned = (float(line_item['EXT_PRC'] or 0) / 20) or 0
                Database.CP.Loyalty.write_line(self.doc_id, lin_seq_no, points_earned)
                return points_earned

            for lin_seq_no, line_item in enumerate(line_items, start=1):
                points += write_loyalty_line(line_item, lin_seq_no)

            return math.floor(points)

        points_earned = write_lin_loy(self.doc_id, line_items)
        points_redeemed = Database.CP.Loyalty.get_points_used(self.doc_id)
        point_balance = Database.CP.Customer.get_loyalty_balance(cust_no)
        Database.CP.Loyalty.write_ps_doc_hdr_loy_pgm(self.doc_id, points_earned, points_redeemed, point_balance)

        new_bal = point_balance + points_earned
        if new_bal < 0:
            new_bal = 0

        Database.CP.Customer.set_loyalty_balance(cust_no, new_bal)

    def get_store_id(self):
        return 1 if self.order.channel.lower() == 'pos' else 'WEB'

    def get_station_id(self):
        return 'POS' if self.order.channel.lower() == 'pos' else 'WEB'

    def get_drawer_id(self):
        return 'POS' if self.order.channel.lower() == 'pos' else 1

    def get_post_payload(self) -> dict:
        """Returns the POST payload for a Counterpoint order."""
        order = self.order

        if self.verbose:
            self.logger.info(f'Getting payload for order: \n{order}\n')

        payload = {
            'PS_DOC_HDR': {
                'STR_ID': self.store_id,
                'STA_ID': self.station_id,
                'DRW_ID': self.drawer_id,
                'TKT_NUM': f'{ORDER_PREFIX}{self.order.id}',
                'CUST_NO': self.order.customer.cp_id,
                'TKT_TYP': 'T',
                'DOC_TYP': 'O',
                'USR_ID': 'POS',
                'HAS_ENTD_LINS': 'N',
                'TAX_COD': 'EXEMPT',
                'NORM_TAX_COD': 'EXEMPT',
                'SHIP_VIA_COD': ('CPC_FLAT' if self.is_refund else ('T' if order.is_shipping else 'C')),
                'PS_DOC_NOTE': self.get_notes(),
                'PS_DOC_LIN': self.get_line_items(),
                # "PS_DOC_GFC": self.get_gift_cards_from_bc_products(bc_products),
                '__PS_DOC_GFC__': self.get_gift_card_purchases(),
                'PS_DOC_PMT': self.get_payments(),
                'PS_DOC_TAX': [
                    {
                        'AUTH_COD': 'EXEMPT',
                        'RUL_COD': 'TAX',
                        'TAX_DOC_PART': 'S',
                        'TAX_AMT': '0',
                        'TOT_TXBL_AMT': order.total_inc_tax - order.base_shipping_cost,
                    }
                ],
            }
        }

        if order.is_shipping:
            payload['PS_DOC_HDR']['PS_DOC_HDR_MISC_CHRG'] = [
                {'TOT_TYP': 'O', 'MISC_CHRG_NO': '1', 'MISC_TYP': 'A', 'MISC_AMT': order.shipping_cost}
            ]

        if self.is_refund:
            payload['PS_DOC_HDR']['TAX_OVRD_REAS'] = 'Y'

        self.sub_tot = sum([float(line_item['EXT_PRC']) for line_item in payload['PS_DOC_HDR']['PS_DOC_LIN']])

        if self.verbose:
            self.logger.info(f'Payload: \n{payload}\n')

        return payload

    def create_customer(self):
        """Create a new customer in Counterpoint from an order."""
        CounterPointAPI.logger.info('Creating new customer')

        order = self.order

        first_name = order.billing_address.first_name
        if not first_name and order.shipping_address:
            first_name = order.shipping_address.first_name

        last_name = order.billing_address.last_name
        if not last_name and order.shipping_address:
            last_name = order.shipping_address.last_name

        phone_number = order.billing_address.phone
        if not phone_number and order.shipping_address:
            phone_number = order.shipping_address.phone

        email_address = order.billing_address.email
        if not email_address and order.shipping_address:
            email_address = order.shipping_address.email

        street_address = order.billing_address.address_1
        city = order.billing_address.city
        state = order.billing_address.province
        zip_code = order.billing_address.zip

        customer_number = customers.add_new_customer(
            first_name=first_name,
            last_name=last_name,
            phone_number=phone_number,
            email_address=email_address,
            street_address=street_address,
            city=city,
            state=state,
            zip_code=zip_code,
        )

        self.order.customer.cp_id = customer_number

        # Add to the middleware database
        shopify_cust_no = order.customer.id
        Database.Shopify.Customer.insert(cp_cust_no=customer_number, shopify_cust_no=shopify_cust_no)

        def write_shipping_adr():
            first_name = order.shipping_address.first_name
            last_name = order.shipping_address.last_name
            phone_number = order.shipping_address.phone
            email_address = order.shipping_address.email
            street_address = order.shipping_address.address_1
            city = order.shipping_address.city
            state = order.shipping_address.province
            zip_code = order.shipping_address.zip

            if (
                first_name is None
                or last_name is None
                or phone_number is None
                or email_address is None
                or street_address is None
                or city is None
                or state is None
                or zip_code is None
            ):
                return

            response = customers.update_customer_shipping(
                cust_no=self.order.customer.cp_id,
                first_name=first_name,
                last_name=last_name,
                phone_number=phone_number,
                email_address=email_address,
                street_address=street_address,
                city=city,
                state=state,
                zip_code=zip_code,
            )

            if response['code'] == 200:
                self.logger.success('Shipping address updated')
            else:
                self.error_handler.add_error_v('Shipping address could not be updated')
                self.error_handler.add_error_v(response['message'])

        if order.shipping_address:
            write_shipping_adr()

    def update_customer(self):
        CounterPointAPI.logger.info('Updating existing customer')

        if not self.order.customer.cp_id:
            self.error_handler.add_error_v('Valid customer number is required')
            return

        def write_cust():
            customer = self.order.customer
            billing_address = self.order.billing_address

            response = customers.update_customer(
                cust_no=customer.cp_id,
                first_name=customer.first_name,
                last_name=customer.last_name,
                phone_number=customer.phone,
                email_address=customer.email,
                street_address=billing_address.address_1,
                city=billing_address.city,
                state=billing_address.province,
                zip_code=billing_address.zip,
            )

            Database.Shopify.Customer.update(cp_cust_no=customer.cp_id, shopify_cust_no=customer.id)

            if response['code'] == 200:
                self.logger.success('Customer updated')
            else:
                self.error_handler.add_error_v('Customer could not be updated')
                self.error_handler.add_error_v(response['message'])

        def write_shipping_adr():
            s = self.order.shipping_address
            first_name = s.first_name
            last_name = s.last_name
            phone_number = s.phone
            email_address = s.email
            street_address = s.address_1
            city = s.city
            state = s.province
            zip_code = s.zip

            if (
                first_name is None
                or last_name is None
                or phone_number is None
                or email_address is None
                or street_address is None
                or city is None
                or state is None
                or zip_code is None
            ):
                return

            response = customers.update_customer_shipping(
                cust_no=self.order.customer.cp_id,
                first_name=first_name,
                last_name=last_name,
                phone_number=phone_number,
                email_address=email_address,
                street_address=street_address,
                city=city,
                state=state,
                zip_code=zip_code,
            )

            if response['code'] == 200:
                self.logger.success('Shipping address updated')
            else:
                self.error_handler.add_error_v('Shipping address could not be updated')
                self.error_handler.add_error_v(response['message'])

        write_cust()
        if self.order.shipping_address:
            write_shipping_adr()

    @staticmethod
    def process_order(order_id: int, session: requests.Session = requests.Session()):
        oapi = OrderAPI(order_id, session)
        order = oapi.order
        cust_no = order.customer.cp_id

        if order.payment_status.lower() in ['declined', '']:
            oapi.error_handler.add_error_v('Order payment declined')
            raise Exception('Order payment declined')

        try:
            if not order.customer.cp_id:
                oapi.create_customer()
            else:
                oapi.update_customer()
        except:
            raise Exception('Customer could not be created/updated')

        try:
            oapi.post_order()

        except Exception as e:
            oapi.error_handler.add_error_v('Order could not be posted', traceback=tb())
            oapi.error_handler.add_error_v(str(e))

            query = f"""
            SELECT TOP 2 DOC_ID FROM PS_DOC_HDR
            WHERE CUST_NO = '{cust_no}'
            AND TKT_DT > '{datetime.now().strftime("%Y-%m-%d")}'
            ORDER BY LST_MAINT_DT DESC
            """

            oapi.logger.info('Attempting to cleanup order')

            response = Database.query(query)

            if response is not None and len(response) > 0 and len(response) < 3:
                for result in response:
                    doc_id = result[0]

                    query = f"""
                    DELETE FROM PS_DOC_HDR WHERE DOC_ID = '{doc_id}'
                    """

                    response = Database.query(query)

                    if response['code'] == 200:
                        oapi.logger.success(f'Order {doc_id} deleted')
                    else:
                        oapi.error_handler.add_error_v(f'Order {doc_id} could not be deleted')
                        oapi.error_handler.add_error_v(response['message'])
            else:
                oapi.error_handler.add_error_v('Could not cleanup order')

            raise e

    def get_refund_index(self, suffix: str) -> int:
        """Returns the refund index for a given refund or partial refund. Ex. 1151R1, 1150PR1, 1150PR2"""
        if self.refund_index is not None:
            return self.refund_index

        index: int = 1
        found = False

        while not found:
            if Database.CP.OpenOrder.tkt_num_exists(tkt_num=self.cp_tkt_no, suffix=suffix, index=index):
                index += 1
            else:
                found = True

        self.refund_index = index
        return index

    def write_gift_cards(self):
        for gift_card in self.payload['PS_DOC_HDR']['__PS_DOC_GFC__']:
            Database.CP.GiftCard.insert(
                doc_id=self.doc_id,
                paycode=gift_card['GFC_COD'],
                number=gift_card['GFC_NO'],
                amount=gift_card['AMT'],
                line_seq_no=gift_card['LIN_SEQ_NO'],
                description=gift_card['DESCR'],
                gfc_seq_no=gift_card['GFC_SEQ_NO'],
            )

            def commit_query(query):
                response = Database.query(query)
                return response

            def get_next_seq_no():
                query = f"""
                SELECT MAX(SEQ_NO) FROM SY_GFC_ACTIV
                WHERE GFC_NO = '{gift_card["GFC_NO"]}'
                """

                response = Database.query(query)

                try:
                    return int(response[0][0]) + 1
                except:
                    return 1

            def add_gfc_bal(amt: float | int):
                current_date = datetime.now().strftime('%Y-%m-%d')

                tkt_no = self.payload['PS_DOC_HDR']['TKT_NUM']

                # if self.is_refund:
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

                if r['code'] == 200:
                    self.logger.success('Gift card balance updated')
                else:
                    self.error_handler.add_error_v('Gift card balance could not be updated')
                    self.error_handler.add_error_v(r['message'])

                r = commit_query(
                    f"""
                    INSERT INTO SY_GFC_ACTIV
                    (GFC_NO, SEQ_NO, DAT, STR_ID, STA_ID, DOC_NO, ACTIV_TYP, AMT, LST_MAINT_DT, LST_MAINT_USR_ID, DOC_ID)
                    VALUES
                    ('{gift_card["GFC_NO"]}', {get_next_seq_no()}, '{current_date}', 'WEB', 'WEB', '{tkt_no}', 'I', {amt}, GETDATE(), 'POS', '{doc_id}')
                    """
                )

                if r['code'] == 200:
                    self.logger.success('Gift card balance updated')
                else:
                    self.error_handler.add_error_v('Gift card balance could not be updated')
                    self.error_handler.add_error_v(r['message'])

            add_gfc_bal(gift_card['AMT'])

    def post_order(self):
        """Posts an order/refund/partial refund to Counterpoint."""
        if self.is_partial_refund:
            self.logger.info('Posting order as partial refund')
        else:
            self.logger.info(f'Posting order: {self.order.id}')

        cust_no = self.order.customer.cp_id
        if not cust_no:
            self.error_handler.add_error_v('Valid customer number is required')
            return

        self.payload = self.get_post_payload(cust_no, self.order)
        response = self.post_document(self.payload)

        if response['ErrorCode'] == 'SUCCESS':
            try:
                self.doc_id = response['Documents'][0]['DOC_ID']
            except:
                self.error_handler.add_error_v('Document ID could not be retrieved')
                return
        else:
            self.error_handler.add_error_v('Order could not be created')
            raise Exception(response)

        self.logger.success(f'Order Created! DOC_ID: {self.doc_id}')

        # Set Order Properties
        self.cp_tkt_no = self.payload['PS_DOC_HDR']['TKT_NUM']
        item_lines = self.payload['PS_DOC_HDR']['PS_DOC_LIN']
        gift_lines = self.payload['PS_DOC_HDR']['__PS_DOC_GFC__']
        self.has_loyalty_payment = Database.CP.OpenOrder.has_loyalty_payment(self.doc_id)
        self.has_gift_card_payment = Database.CP.OpenOrder.has_gc_payment(self.doc_id)
        self.total_tender = self.order.refund_total if self.is_refund else self.get_total_tender()

        # Write Tables
        self.write_ticket_no()
        self.write_loyalty(self.doc_id, cust_no, item_lines)
        self.write_discounts(self.doc_id, item_lines)

        if self.is_refund:
            self.write_refund_data()
        else:
            # Flow for Standard Orders
            self.redeem_loyalty_pmts()
            if gift_lines:
                self.write_gift_cards()

        Database.CP.OpenOrder.set_ticket_date(self.doc_id, self.order.date_created)
        Database.CP.OpenOrder.delete_hdr_total_entry(self.doc_id)
        Database.CP.OpenOrder.set_loyalty_program(self.doc_id)
        self.write_hdr_total_entry()
        Database.CP.OpenOrder.set_line_type(self.doc_id, line_type='R' if self.is_refund else 'S')
        Database.CP.OpenOrder.set_apply_type(self.doc_id, apply_type='S')
        Database.CP.OpenOrder.set_line_totals(self.doc_id, self.total_lin_items, is_refund=self.is_refund)
        self.set_line_quantities()
        self.cleanup(self.doc_id)
        self.logger.success(f'Order {self.doc_id} created')
        return response

    def write_ticket_no(self) -> None:
        """Updates tables with the ticket number for the order."""
        if self.cp_tkt_no:
            if self.is_refund:
                suffix = PARTIAL_REFUND_SUFFIX if self.is_partial_refund else REFUND_SUFFIX
                refund_index = self.get_refund_index(suffix=suffix)
                tkt_no = f'{self.cp_tkt_no}{suffix}{refund_index}'
            else:
                tkt_no = self.cp_tkt_no

        self.cp_tkt_no_full = tkt_no

        Database.CP.OpenOrder.write_ticket_number(doc_id=self.doc_id, tkt_no=tkt_no)

    def write_refund_data(self):
        self.logger.info(f'Writing refund data for order: {self.doc_id}')

        gift_cards = self.payload['PS_DOC_HDR']['__PS_DOC_GFC__']
        payments = self.payload['PS_DOC_HDR']['PS_DOC_PMT']

        def remove_gift_card_balance():
            for gift_card in gift_cards:
                card_no = gift_card['GFC_NO']
                amt = 0 - Database.CP.GiftCard.get_balance(card_no)
                doc_id = self.doc_id
                tkt_no = self.cp_tkt_no_full
                Database.CP.GiftCard.add_balance(tkt_no=tkt_no, card_no=card_no, amount=amt, doc_id=doc_id)
                Database.CP.GiftCard.delete_balance(card_no)

        remove_gift_card_balance()

        total_paid = -self.order.total_inc_tax
        Database.CP.OpenOrder.update_payment_amount(doc_id=self.doc_id, amount=total_paid)

        # PARTIAL REFUND PAYMENT WRITES
        if self.is_partial_refund:

            def get_ps_doc_pmt_index(pay_cod: str):
                index = 0

                for i, pmt in enumerate(self.payload['PS_DOC_HDR']['PS_DOC_PMT']):
                    if pmt['PAY_COD'] == pay_cod:
                        index = i

                return index

            shop_payment = Database.CP.OpenOrder.get_payment_by_code(self.doc_id, 'SHOP')
            remaining: float = 0

            if self.order.total_inc_tax > shop_payment:
                remaining = self.order.total_inc_tax - shop_payment

            shop = self.order.total_inc_tax if self.order.total_inc_tax < shop_payment else shop_payment
            gc = (remaining / 2 if self.has_loyalty_payment else remaining) if self.has_gift_card_payment else 0
            loy = (remaining / 2 if self.has_gift_card_payment else remaining) if self.has_loyalty_payment else 0

            if shop_payment > 0:
                shop_refund = -shop
                Database.CP.OpenOrder.update_payment_amount(self.doc_id, shop_refund, 'SHOP')
                Database.CP.OpenOrder.update_payment_apply(self.doc_id, shop_refund, 'SHOP')

            if self.has_gift_card_payment():
                gc_refund = -gc
                self.payload['PS_DOC_HDR']['PS_DOC_PMT'][get_ps_doc_pmt_index('GC')]['AMT'] = gc_refund
                Database.CP.OpenOrder.update_payment_amount(self.doc_id, gc_refund, 'GC')
                Database.CP.OpenOrder.update_payment_apply(self.doc_id, gc_refund, 'GC')

            if self.has_loyalty_payment:
                loy_refund = -loy
                payment_index = get_ps_doc_pmt_index('LOYALTY')
                self.payload['PS_DOC_HDR']['PS_DOC_PMT'][payment_index]['AMT'] = loy_refund
                Database.CP.OpenOrder.update_payment_amount(self.doc_id, loy_refund, 'LOYALTY')
                Database.CP.OpenOrder.update_payment_apply(self.doc_id, loy_refund, 'LOYALTY')

            total_paid = shop + gc + loy

        # PAYMENT REFUND WRITES
        for payment in payments:
            if payment['PAY_COD'] == 'GC':
                amt_spent = float(payment['AMT'])
                card_no = payment['CARD_NO']

                query = f"""
                UPDATE SY_GFC_ACTIV
                SET AMT = {abs(amt_spent)},
                DOC_NO = '{self.cp_tkt_no_full}'
                WHERE GFC_NO = '{card_no}' AND SEQ_NO = {Database.CP.GiftCard.get_last_activity_index()}
                """

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Gift card activity updated')
                else:
                    self.error_handler.add_error_v('Gift card activity could not be updated')
                    self.error_handler.add_error_v(response['message'])

                query = f"""
                UPDATE SY_GFC
                SET CURR_AMT = CURR_AMT + {abs(amt_spent) * 2}
                WHERE GFC_NO = '{card_no}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Gift card balance updated')
                else:
                    self.error_handler.add_error_v('Gift card balance could not be updated')
                    self.error_handler.add_error_v(response['message'])

            if payment['PAY_COD'] == 'LOYALTY':
                points = abs(math.floor(float(payment['AMT'])))
                Database.CP.Loyalty.add_points(cust_no=self.order.customer.cp_id, points=points)

        # PAYMENT APPLY REFUND
        if not self.is_partial_refund():
            Database.CP.OpenOrder.update_payment_apply(self.doc_id, total_paid, 'SHOP')

        def invert_line_qty(line_item: dict, index: int):
            qty = -line_item['QTY_SOLD']

            r = commit_query(
                f"""
                UPDATE PS_DOC_LIN
                SET QTY_SOLD = {qty},
                EXT_PRC = {qty * line_item["PRC"]},
                EXT_COST = {qty * line_item["EXT_COST"]}
                WHERE DOC_ID = '{self.doc_id}' AND LIN_SEQ_NO = {index}
                """
            )

            if r['code'] == 200:
                self.logger.success(f'Line {index} inverted')
            else:
                self.error_handler.add_error_v(f'Line {index} could not be inverted')
                self.error_handler.add_error_v(r['message'])

        for i, line_item in enumerate(payload['PS_DOC_HDR']['PS_DOC_LIN'], start=1):
            invert_line_qty(line_item, i)

        order_db = Database.CP.OpenOrder

        def negative_column(table: str, column: str, index: int):
            order_db.set_value(table, column, -order_db.get_value(table, column, index), index, self.doc_id)

        for i, line_item in enumerate(payload['PS_DOC_HDR']['PS_DOC_LIN'], start=1):
            for column in ['EXT_COST', 'ORIG_QTY', 'GROSS_EXT_PRC', 'GROSS_DISP_EXT_PRC', 'CALC_EXT_PRC']:
                negative_column('PS_DOC_LIN', column, i)

            def set_value_lin(column, value):
                order_db.set_value('PS_DOC_LIN', column, value, i)

            def get_value_lin(column):
                return order_db.get_value('PS_DOC_LIN', column, i)

            set_value_lin('QTY_ENTD', 0)
            set_value_lin('QTY_TO_REL', get_value_lin('QTY_SOLD'))
            set_value_lin('QTY_TO_LEAVE', 0)

            def set_value_prc(column, value):
                order_db.set_value('PS_DOC_LIN_PRICE', column, value, i)

            def get_value_prc(column):
                return order_db.get_value('PS_DOC_LIN_PRICE', column, i)

            def invert_prc(column):
                set_value_prc(column, -get_value_prc(column))

            set_value_prc('PRC_RUL_SEQ_NO', -1)
            set_value_prc('PRC_BRK_DESCR', "'I'")
            invert_prc('QTY_PRCD')

    def redeem_loyalty_pmts(self):
        for payment in self.payload['PS_DOC_HDR']['PS_DOC_PMT']:
            if payment['PAY_COD'] == 'LOYALTY':
                amount = abs(math.floor(float(payment['AMT'])))
                Database.CP.Loyalty.redeem(amount, self.order.customer.cp_id)

    def get_total_tender(self) -> float:
        """Returns the total tender amount for an order."""
        total = 0

        for payment in self.payload['PS_DOC_HDR']['PS_DOC_PMT']:
            total += abs(float(payment['AMT']))

        return total

    def write_hdr_total_entry(self):
        sub_tot = (
            self.order.subtotal_ex_tax - self.total_discount_amount
            if not self.is_refund
            else self.order.total_inc_tax
        )
        tot = self.order.total_inc_tax
        gfc_amount = 0 if self.is_refund else self.total_gfc_amount
        tot_ext_cost = 0

        for line_item in self.payload['PS_DOC_HDR']['PS_DOC_LIN']:
            tot_ext_cost += Database.CP.Product.get_cost(line_item['ITEM_NO'])

        Database.CP.OpenOrder.insert_hdr_total_entry(
            doc_id=self.doc_id,
            lines=len(self.payload['PS_DOC_HDR']['PS_DOC_LIN']),
            gfc_amt=gfc_amount,
            sub_tot=-sub_tot if self.is_refund else sub_tot,
            tot_ext_cost=-tot_ext_cost if self.is_refund else tot_ext_cost,
            tot_tender=self.total_tender,
            tot=tot,
            total_hdr_disc=self.total_hdr_disc,
            total_lin_disc=self.total_lin_disc,
            eh=ProcessInErrorHandler,
        )

    def set_line_quantities(self):
        if not self.is_refund:
            order_db = Database.CP.OpenOrder
            for i, line_item in enumerate(self.payload['PS_DOC_HDR']['PS_DOC_LIN'], start=1):

                def set_value_lin(column, value):
                    order_db.set_value('PS_DOC_LIN', column, value, i, self.doc_id)

                def get_value_lin(column):
                    return order_db.get_value('PS_DOC_LIN', column, i, self.doc_id)

                set_value_lin('QTY_ENTD', 0)
                set_value_lin('QTY_TO_REL', get_value_lin('QTY_SOLD'))
                set_value_lin('QTY_TO_LEAVE', 0)

    # Writes to several tables in Counterpoint.
    def more_writes(self):
        self.logger.info('Writing tables')

        # if float(bc_order['base_shipping_cost']) > 0:
        #     if not self.is_refund(bc_order):
        #         # query = f"""
        #         # UPDATE PS_DOC_HDR_MISC_CHRG
        #         # SET DOC_ID = '{doc_id}',
        #         # TOT_TYP = 'S'
        #         # WHERE DOC_ID = '{get_orig_doc_id()}'
        #         # """

        #         query = f"""
        #         INSERT INTO PS_DOC_HDR_MISC_CHRG
        #         (DOC_ID, TOT_TYP, MISC_CHRG_NO, MISC_TYP, MISC_AMT, MISC_PCT, MISC_TAX_AMT_ALLOC, MISC_NORM_TAX_AMT_ALLOC)
        #         VALUES
        #         ('{doc_id}', 'S', '1', 'A', {(float(bc_order["base_shipping_cost"] or 0))}, 0, 0, 0)
        #         """
        #     else:
        #         # query = f"""
        #         # UPDATE PS_DOC_HDR_MISC_CHRG
        #         # SET DOC_ID = '{doc_id}',
        #         # TOT_TYP = 'S',
        #         # MISC_AMT = {-(float(bc_order["base_shipping_cost"] or 0))}
        #         # WHERE DOC_ID = '{get_orig_doc_id()}'
        #         # """

        #         query = f"""
        #         INSERT INTO PS_DOC_HDR_MISC_CHRG
        #         (DOC_ID, TOT_TYP, MISC_CHRG_NO, MISC_TYP, MISC_AMT, MISC_PCT, MISC_TAX_AMT_ALLOC, MISC_NORM_TAX_AMT_ALLOC)
        #         VALUES
        #         ('{doc_id}', 'S', '1', 'A', {-(float(bc_order["base_shipping_cost"] or 0))}, 0, 0, 0)
        #         """

        #     response = Database.query(query)

        #     if response['code'] == 200:
        #         self.logger.success('Applied shipping charge')
        #     else:
        #         self.error_handler.add_error_v('Shipping charge could not be applied')
        #         self.error_handler.add_error_v(response)

    # Remove the original document and the reference to the original document
    # The original document is the ORDER document that was created at the NCR Counterpoint API POST.
    # The original document is different than the final TICKET document that we see and is not needed.
    def cleanup(self):
        """Cleans up the original document and the reference to the original document.
        The original document is the ORDER document that was created at the NCR Counterpoint API POST.
        The original document is different than the final TICKET document that we see and is not needed."""

        orig_doc_id = Database.CP.OpenOrder.get_orig_doc_id(self.doc_id)

        self.logger.info(f'Cleaning up document {orig_doc_id}')

        Database.CP.Customer.decrement_orders(self.order.customer.cp_id)

        # Remove the original document
        query = f"""
        DELETE FROM PS_DOC_HDR
        WHERE DOC_ID in (
        SELECT ORIG_DOC_ID FROM PS_DOC_HDR_ORIG_DOC
        WHERE DOC_ID = '{doc_id}'
        )
        """
        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Removed original document')
        else:
            self.error_handler.add_error_v('Original document could not be removed')
            self.error_handler.add_error_v(response['message'])

        query = f"""
        DELETE FROM PS_DOC_HDR_ORIG_DOC
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Removed original document reference')
        else:
            self.error_handler.add_error_v('Original document reference could not be removed')
            self.error_handler.add_error_v(response['message'])

    # Get customer from email and phone number
    @staticmethod
    def get_customer_from_info(user_info):
        return Database.CP.Customer.lookup_customer(
            email_address=user_info['email'], phone_number=user_info['phone']
        )

    @staticmethod
    def get_cust_phone(order: ShopifyOrder):
        """Get the customer's phone number from the Shopify order"""
        try:
            phone = order.billing_address.phone

            if phone is None or phone == '':
                if order.shipping_address:
                    phone = order.shipping_address.phone

            return phone
        except:
            return ''

    @staticmethod
    def get_cust_email(order: ShopifyOrder):
        """Get the customer's email from the Shopify order"""
        try:
            email = order.billing_address.email

            if email is None or email == '':
                email = order.shipping_address.email or order.email
            return email
        except:
            return ''

    # Get the customer's number from the BigCommerce order
    @staticmethod
    def get_cust_no(order: ShopifyOrder):
        user_info = {'email': OrderAPI.get_cust_email(order), 'phone': OrderAPI.get_cust_phone(order)}

        cust_no = OrderAPI.get_customer_from_info(user_info)

        return cust_no


# This class is used to parse the BigCommerce order response.
class JsonTools:
    @staticmethod
    def get_json(url: str):
        response = requests.get(url, headers=creds.test_bc_api_headers)
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
                if key in ['products', 'coupons', 'shipping_addresses']:
                    obj[key] = JsonTools.unpack(value)
            elif isinstance(value, list):
                obj[key] = JsonTools.unpack_list(value)
            elif isinstance(value, str) and value.startswith('http'):
                try:
                    myjson = JsonTools.get_json(value)
                    if isinstance(myjson, list):
                        myjson = JsonTools.unpack_list(myjson)
                    if isinstance(myjson, dict):
                        myjson = JsonTools.unpack(myjson)
                    obj[key] = myjson
                except:
                    if value.endswith('coupons'):
                        obj[key] = []
                    if value.endswith('shipping_addresses'):
                        obj[key] = []
                    pass

        return obj


class HoldOrder(DocumentAPI):
    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)

    class ItemPayload:
        def __init__(self, name, item_no, qty, price):
            self.name = name
            self.item_no = item_no
            self.qty = qty
            self.price = price

        def get(self):
            sku = self.item_no

            if self.name.lower() == 'service':
                sku = 'SERVICE'

            if self.name.lower() == 'delivery':
                sku = 'DELIVERY'

            return {
                'LIN_TYP': 'S',
                'ITEM_NO': sku,
                'QTY_SOLD': float(self.qty),
                'PRC': float(self.price),
                'EXT_PRC': float(self.price) * float(self.qty),
            }

    class LinesPayload:
        def __init__(self):
            self.line_items = []

        def get(self):
            return self.line_items

        def add(self, item: dict):
            itemPayload = HoldOrder.ItemPayload(
                name=item['name'], item_no=item['item_no'], qty=item['qty'], price=item['price']
            )
            data = itemPayload.get()
            if data is not None:
                self.line_items.append(data)

    class DocumentPayload:
        def __init__(self, cust_no: str | int):
            self.storeId = 1
            self.stationId = 'POS'
            self.drawerId = 'POS'
            self.ticketType = 'T'
            self.docType = 'H'
            self.userId = 'POS'
            self.notes = []
            self.cust_no = cust_no
            self.line_items = HoldOrder.LinesPayload()
            self.shipping = 0

        def get(self):
            pl = {
                'PS_DOC_HDR': {
                    'STR_ID': self.storeId,
                    'STA_ID': self.stationId,
                    'DRW_ID': self.drawerId,
                    'TKT_TYP': self.ticketType,
                    'DOC_TYP': self.docType,
                    'USR_ID': self.userId,
                    'CUST_NO': self.cust_no,
                    'PS_DOC_LIN': self.line_items.get(),
                    'PS_DOC_NOTE': self.notes,
                }
            }

            return pl

        def add_note(self, note, note_id='ADMIN NOTE'):
            self.notes.append({'NOTE_ID': note_id, 'NOTE': note})

        def add_item(self, name: str, item_no: str, qty: int, price: float):
            self.line_items.add({'name': name, 'item_no': item_no, 'qty': qty, 'price': price})

        def add_lines(self, lines: list[dict]):
            for line in lines:
                self.line_items.add(line)

    @staticmethod
    def apply_total(doc_id: str, sub_tot: float, total_discount: float):
        tot = float(sub_tot) - float(total_discount)

        query = f"""
        INSERT INTO PS_DOC_HDR_TOT
        (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
        VALUES
        ('{doc_id}', 'S', 0, '!', 0, 0, 0, 0, {sub_tot}, 0, 0, 0, 0, 0, 0, 0, 0, 0, {tot}, 0, {total_discount}, 0, {sub_tot}, 0)
        """

        return Database.query(query)

    @staticmethod
    def apply_discount(doc_id: str, amount: float):
        query = f"""
        INSERT INTO PS_DOC_DISC
        (DOC_ID, DISC_SEQ_NO, DISC_ID, APPLY_TO, DISC_TYP, DISC_AMT, DISC_PCT, DISC_AMT_SHIPPED)
        VALUES
        ('{doc_id}', 1, '100000000000330', 'H', 'A', '{amount}', 0, 0)
        """

        return Database.query(query)

    @staticmethod
    def post_pl(payload: dict, discount: float = 0, sub_tot: float = 0):
        ho = HoldOrder()
        data = ho.post_document(payload=payload)

        if (
            data is None
            or data['ErrorCode'] != 'SUCCESS'
            or data['Documents'] is None
            or len(data['Documents']) == 0
        ):
            return data

        doc_id = data['Documents'][0]['DOC_ID']

        HoldOrder.apply_discount(doc_id=doc_id, amount=discount)

        HoldOrder.apply_total(doc_id=doc_id, sub_tot=sub_tot, total_discount=discount)

        return data

    @staticmethod
    def create(lines: list[dict], cust_no: str | int = 'CASH'):
        doc = HoldOrder.DocumentPayload(cust_no=cust_no)
        doc.add_lines(lines)
        return doc

    @staticmethod
    def get_lines_from_draft_order(draft_order_id: str | int):
        """Get all lines from a draft order."""
        shop_order = Shopify.Order.Draft.get(draft_order_id)

        lines = []
        snode = shop_order['node']
        line_items = snode['lineItems']['edges']

        for _item in line_items:
            item = _item['node']

            lines.append(
                {
                    'name': item['name'] or '',
                    'item_no': item['sku'],
                    'qty': item['quantity'],
                    'price': float(item['originalUnitPriceSet']['shopMoney']['amount']),
                }
            )

        return lines
