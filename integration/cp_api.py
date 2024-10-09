from setup import creds
import requests
import json
import math
from datetime import datetime, timezone
from database import Database
from setup.error_handler import ProcessInErrorHandler
from customer_tools import customers
from integration.shopify_api import Shopify
from integration.models.orders import ShopifyOrder, CPOrder
from traceback import format_exc as tb


ORDER_PREFIX = 'S'
REFUND_SUFFIX = 'R'
PARTIAL_REFUND_SUFFIX = 'PR'


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

    def post(self, url, payload: dict = {}):
        response = self.session.post(url, headers=self.post_headers, json=payload, verify=False)

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

    def post_document(self, payload: dict):
        url = self.base_url

        response = self.post(url, payload=payload)

        return response.json()


class OrderAPI(DocumentAPI):
    """This class is used to interact with the NCR Counterpoint API's Document endpoint
    and create orders and refunds."""

    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)
        self.discount_seq_no: int = 1
        self.total_discount_amount: float = 0
        self.total_gfc_amount: float = 0
        self.total_hdr_disc: float = 0
        self.total_lin_disc: float = 0
        self.refund: bool = False
        self.is_partial_refund: bool = False
        self.refund_index: int = None
        self.total_lin_items: int = 0
        self.line_item_length: int = 0

    def is_refund(self, order: ShopifyOrder = None) -> bool:
        """Returns True if the order is a refund, False if it is not."""
        if order:
            self.refund = order.status in ['Refunded', 'Partially Refunded']

        return self.refund

    def get_line_items(self, lines: list[ShopifyOrder.LineItem]) -> list[dict]:
        """Returns a list of line items from a order."""
        line_items = []

        for item in lines:
            if item.type == 'physical':
                line = CPOrder.LineItem(item)
                line_item = {
                    'LIN_TYP': line.type,
                    'ITEM_NO': line.sku,
                    'USR_ENTD_PRC': line.user_entered_price,
                    'QTY_SOLD': line.quantity,
                    'PRC': line.price,
                    'EXT_PRC': line.extended_price,
                    'EXT_COST': line.extended_cost,
                    'DSC_AMT': line.discount_amount,
                    'sku': line.sku,
                }

                line_items.append(line_item)
                self.total_lin_items += 1

        return line_items

    def get_gift_card_purchases(self, lines: list[ShopifyOrder.LineItem]) -> list[dict]:
        """Returns a list of gift cards from a list of products from a BigCommerce order."""

        gift_cards = []

        for i, product in enumerate(lines, start=1):
            if self.is_partial_refund and float(product.quantity_refunded) == 0:
                continue

            if product.type == 'giftcertificate':
                gift_card = {
                    'GFC_COD': 'GC',
                    'GFC_NO': product.gift_certificate_id['code'],
                    'AMT': float(product.base_price),
                    'LIN_SEQ_NO': self.line_item_length + 1,
                    'DESCR': 'Gift Certificate',
                    'CREATE_AS_STC': 'N',
                    'GFC_SEQ_NO': i,
                }
                self.line_item_length += 1

                gift_cards.append(gift_card)
                self.total_gfc_amount += float(product.base_price)

        return gift_cards

    def get_payments(self, order: ShopifyOrder) -> list[dict]:
        """Returns a list of payments from a BigCommerce order."""

        payments = [
            {
                'AMT': order.total_inc_tax - order.store_credit_amount,
                'PAY_COD': 'SHOP',
                'FINAL_PMT': 'N',
                'PMT_LIN_TYP': 'C' if self.is_refund() else 'T',
            }
        ]

        if order.store_credit_amount > 0:
            payments.append(
                {
                    'AMT': order.store_credit_amount,
                    'PAY_COD': 'LOYALTY',
                    'FINAL_PMT': 'N',
                    'PMT_LIN_TYP': 'C' if self.is_refund() else 'T',
                }
            )

        def get_gift_card_payments(order: ShopifyOrder) -> list[dict]:
            """Returns a list of gift card payments from a order."""
            gfc_payments = []

            if not order.transactions:
                return gfc_payments

            for pay_method in order.transactions['data']:
                if pay_method['method'] == 'gift_certificate':
                    gfc_payment = {
                        'AMT': float(pay_method['amount']),
                        'PAY_COD': 'GC',
                        'FINAL_PMT': 'N',
                        'CARD_NO': pay_method['gift_certificate']['code'],
                        'PMT_LIN_TYP': 'C' if self.is_refund() else 'T',
                        'REMAINING_BAL': float(pay_method['gift_certificate']['remaining_balance']),
                    }

                    gfc_payments.append(gfc_payment)

            return gfc_payments

        payments += get_gift_card_payments(order)

        return payments

    def get_notes(self, order: ShopifyOrder) -> list[dict]:
        """Returns a list of notes from a order."""
        notes = []

        if order.customer_message:
            notes.append({'NOTE_ID': 'Customer Message', 'NOTE': order.customer_message})

        return notes

    def write_discounts(self, doc_id, bc_order, line_items: list[dict]):
        """Write discounts to the PS_DOC_DISC table."""

        def write_disc_line(doc_id, disc_seq_no: int, disc_amt: float, lin_seq_no: int = None):
            """Write a discount to the PS_DOC_DISC table."""
            apply_to = 'L' if lin_seq_no else 'H'  # L for line, H for header
            disc_type = 'A'  # Amount
            disc_id = '100000000000331' if lin_seq_no else '100000000000330'
            disc_pct = 0
            disc_amt_shipped = 0

            if self.is_refund():
                disc_amt = -disc_amt

            if apply_to == 'H':
                self.total_hdr_disc += disc_amt
            else:
                self.total_lin_disc += disc_amt

            query = f"""
            INSERT INTO PS_DOC_DISC
            (DOC_ID, DISC_SEQ_NO, LIN_SEQ_NO, DISC_ID, APPLY_TO, DISC_TYP, 
            DISC_AMT, DISC_PCT, DISC_AMT_SHIPPED)
            
            VALUES
            
            ('{doc_id}', {disc_seq_no}, {lin_seq_no or "NULL"}, {disc_id}, '{apply_to}', '{disc_type}', 
            {disc_amt}, {disc_pct}, {disc_amt_shipped})
            """

            self.total_discount_amount += abs(disc_amt)

            response = Database.query(query)

            if response['code'] == 200:
                self.logger.success(f'Discount {disc_seq_no} created')
            else:
                self.error_handler.add_error_v(f'Discount {disc_seq_no} could not be created')

        self.logger.info('Writing discounts')
        coupons = bc_order['coupons']['url']
        if coupons:
            coupon_total: float = 0
            for coupon in coupons:
                coupon_total += float(coupon['amount'])

            if coupon_total > 0:
                self.logger.info(f'Writing coupon discount: ${coupon_total}')
                write_disc_line(doc_id, disc_seq_no=self.discount_seq_no, disc_amt=coupon_total)
                self.discount_seq_no += 1

        for i, line_item in enumerate(line_items, start=1):
            amt = float(line_item['DSC_AMT'])
            if amt > 0:
                self.logger.info(f'Writing line discount: ${amt}')
                write_disc_line(doc_id, disc_seq_no=self.discount_seq_no, disc_amt=amt, lin_seq_no=i)
                self.discount_seq_no += 1

    # Write loyalty line
    def write_one_lin_loy(self, doc_id, line_item: dict, lin_seq_no: int):
        if line_item['sku'] == 'SERVICE':
            return 0
        if line_item['sku'] == 'DELIVERY':
            return 0

        points_earned = (float(line_item['EXT_PRC'] or 0) / 20) or 0

        query = f"""
        INSERT INTO PS_DOC_LIN_LOY 
        (DOC_ID, LIN_SEQ_NO, LIN_LOY_PTS_EARND, LOY_PGM_RDM_ELIG, LOY_PGM_AMT_PD_WITH_PTS, LOY_PT_EARN_RUL_DESCR, LOY_PT_EARN_RUL_SEQ_NO) 
        VALUES 
        ('{doc_id}', {lin_seq_no}, {points_earned}, 'Y', 0, 'Basic', 5)
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success(f'Line loyalty points ({points_earned})')
        else:
            self.error_handler.add_error_v(f'Line #{lin_seq_no} could not receive loyalty points')

        return points_earned

    # Write all loyalty lines from a list of line items from a BC order.
    def write_lin_loy(self, doc_id, line_items: list[dict]):
        points = 0
        for lin_seq_no, line_item in enumerate(line_items, start=1):
            points += self.write_one_lin_loy(doc_id, line_item, lin_seq_no)

        return points

    # Write entry into PS_DOC_HDR_LOY_PGM
    def write_ps_doc_hdr_loy_pgm(self, doc_id, cust_no, points_earned: float, points_redeemed: float):
        query = f"""
        SELECT LOY_PTS_BAL 
        FROM {creds.Table.CP.Customers.table}
        WHERE CUST_NO = '{cust_no}'
        """
        response = Database.query(query)
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

        response = Database.query(wquery)

        if response['code'] == 200:
            self.logger.success('Loyalty points written')
        else:
            self.error_handler.add_error_v('Loyalty points could not be written')

        if points_balance + points_earned < 0:
            new_bal = 0
        else:
            new_bal = points_balance + points_earned

        query = f"""
        UPDATE AR_CUST
        SET LOY_PTS_BAL = {new_bal}
        WHERE CUST_NO = '{cust_no}'
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Cust Loyalty points written')
        else:
            self.error_handler.add_error_v('Cust Loyalty points could not be written')

    # Returns total number of loyalty points used.
    def get_loyalty_points_used(self, doc_id):
        query = f"""
        SELECT AMT FROM PS_DOC_PMT
        WHERE PAY_COD = 'LOYALTY' AND DOC_ID = '{doc_id}'
        """

        response = Database.query(query)

        points_used = 0

        try:
            points_used = math.floor(float(response[0][0])) if response else 0
        except:
            pass

        return points_used

    # Write loyalty points.
    def write_loyalty(self, doc_id, cust_no, line_items: list[dict]):
        self.logger.info('Writing loyalty')

        # count = 1
        # for item in line_items:
        #     self.logger.info(f'Line item: {count}')
        #     for k, v in item.items():
        #         self.logger.info(f'{k}: {v}')
        #     count += 1

        points_earned = math.floor(self.write_lin_loy(doc_id, line_items))
        points_redeemed = self.get_loyalty_points_used(doc_id)

        self.write_ps_doc_hdr_loy_pgm(doc_id, cust_no, points_earned, points_redeemed)

    # Returns the total line discount amount summed together.
    def get_total_lin_disc(self, line_items: list[dict]):
        total = 0

        for line_item in line_items:
            total += float(line_item['DSC_AMT'])

        return total

    def get_store_id(self, bc_order: dict):
        return 1 if bc_order['channel'].lower() == 'pos' else 'WEB'

    def get_station_id(self, bc_order: dict):
        return 'POS' if bc_order['channel'].lower() == 'pos' else 'WEB'

    def get_drawer_id(self, bc_order: dict):
        return 'POS' if bc_order['channel'].lower() == 'pos' else 1

    def get_post_order_payload(self, cust_no: str, order: ShopifyOrder):
        """Returns the POST payload for a Counterpoint order."""
        self.discount_seq_no = 1
        self.total_discount_amount = 0
        self.total_gfc_amount = 0
        self.total_hdr_disc = 0
        self.total_lin_disc = 0
        self.refund_index = None
        self.total_lin_items = 0
        self.line_item_length = 0

        self.refund = self.is_refund(order)
        products = order.items

        payload = {
            'PS_DOC_HDR': {
                'STR_ID': self.get_store_id(order),
                'STA_ID': self.get_station_id(order),
                'DRW_ID': self.get_drawer_id(order),
                'TKT_NUM': f'{ORDER_PREFIX}{order.id}',
                'CUST_NO': cust_no,
                'TKT_TYP': 'T',
                'DOC_TYP': 'O',
                'USR_ID': 'POS',
                'HAS_ENTD_LINS': 'N',
                'TAX_COD': 'EXEMPT',
                'NORM_TAX_COD': 'EXEMPT',
                'SHIP_VIA_COD': ('CPC_FLAT' if self.refund else ('T' if order.is_shipping else 'C')),
                'PS_DOC_NOTE': self.get_notes(order),
                'PS_DOC_LIN': self.get_line_items(products),
                # "PS_DOC_GFC": self.get_gift_cards_from_bc_products(bc_products),
                '__PS_DOC_GFC__': self.get_gift_card_purchases(products),
                'PS_DOC_PMT': self.get_payments(order),
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

        if self.refund:
            payload['PS_DOC_HDR']['TAX_OVRD_REAS'] = 'Y'

        self.sub_tot = sum([float(line_item['EXT_PRC']) for line_item in payload['PS_DOC_HDR']['PS_DOC_LIN']])

        return payload

    # Check if the AR_CUST table has a customer with the provided cust_no
    def has_cust(self, cust_no):
        return customers.is_current_customer(cust_no)

    # Check if the AR_CUST table has a customer with the provided email and phone
    def has_cust_info(self, bc_order: dict):
        email = self.billing_or_shipping(bc_order, 'email')
        phone = self.billing_or_shipping(bc_order, 'phone')

        return OrderAPI.get_customer_from_info({'email': email, 'phone': phone})

    # Returns the key from the billing_address
    def billing(self, bc_order: dict, key: str):
        try:
            return bc_order['billing_address'][key]
        except:
            return None

    # Return the key from the shipping_addresses
    def shipping(self, bc_order: dict, key: str):
        try:
            return bc_order['shipping_addresses']['url'][0][key]
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
        def billing_or_shipping(key: str):
            return self.billing_or_shipping(bc_order, key)

        def b(key: str):
            return self.billing(bc_order, key)

        def s(key: str):
            return self.shipping(bc_order, key)

        first_name = billing_or_shipping('first_name')
        last_name = billing_or_shipping('last_name')
        phone_number = billing_or_shipping('phone')
        email_address = billing_or_shipping('email')
        street_address = b('street_1')
        city = b('city')
        state = b('state')
        zip_code = b('zip')

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
        # Add to the middleware database
        shopify_cust_no = bc_order['customer_id'].split('/')[-1]
        Database.Shopify.Customer.insert(cp_cust_no=cust_no, shopify_cust_no=shopify_cust_no)

        def write_shipping_adr():
            first_name = s('first_name')
            last_name = s('last_name')
            phone_number = s('phone')
            email_address = s('email')
            street_address = s('street_1')
            city = s('city')
            state = s('state')
            zip_code = s('zip')

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

            if response['code'] == 200:
                self.logger.success('Shipping address updated')
            else:
                self.error_handler.add_error_v('Shipping address could not be updated')
                self.error_handler.add_error_v(response['message'])

        if (len(bc_order['shipping_addresses']['url']) or 0) > 0:
            write_shipping_adr()

    # Update an existing customer from a BigCommerce order.
    def update_cust(self, bc_order: dict, cust_no: str | int):
        if not self.has_cust(cust_no):
            self.error_handler.add_error_v('Valid customer number is required')
            return

        def b(key: str):
            return self.billing(bc_order, key)

        def s(key: str):
            return self.shipping(bc_order, key)

        def write_cust():
            first_name = b('first_name')
            last_name = b('last_name')
            phone_number = b('phone')
            email_address = b('email')
            street_address = b('street_1')
            city = b('city')
            state = b('state')
            zip_code = b('zip')

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

            Database.Shopify.Customer.update(
                cp_cust_no=cust_no, shopify_cust_no=bc_order['customer_id'].split('/')[-1]
            )

            if response['code'] == 200:
                self.logger.success('Customer updated')
            else:
                self.error_handler.add_error_v('Customer could not be updated')
                self.error_handler.add_error_v(response['message'])

        def write_shipping_adr():
            first_name = s('first_name')
            last_name = s('last_name')
            phone_number = s('phone')
            email_address = s('email')
            street_address = s('street_1')
            city = s('city')
            state = s('state')
            zip_code = s('zip')

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

            if response['code'] == 200:
                self.logger.success('Shipping address updated')
            else:
                self.error_handler.add_error_v('Shipping address could not be updated')
                self.error_handler.add_error_v(response['message'])

        write_cust()
        if (len(bc_order['shipping_addresses']['url']) or 0) > 0:
            write_shipping_adr()

    @staticmethod
    def post_shopify_order(shopify_order_id: str | int, cust_no_override: str = None):
        """Convert Shopify order format to BigCommerce order format"""
        bc_order = Shopify.Order.as_bc_order(order_id=shopify_order_id, send=True)

        OrderAPI.post_order(
            order_id=shopify_order_id, bc_order_override=bc_order, cust_no_override=cust_no_override
        )

    # This function will run the whole ordeal using the provided BigCommerce order_id.
    # cust_no_override is used to override the customer number for the order when posted to Counterpoint.
    # Session can be provided to use the same http session for all requests.
    @staticmethod
    def post_order(
        order_id: str | int,
        cust_no_override: str = None,
        session: requests.Session = requests.Session(),
        bc_order_override=None,
    ):
        oapi = OrderAPI(session=session)

        bc_order = {}

        if bc_order_override is None:
            bc_order = OrderAPI.get_order(order_id)
        else:
            bc_order = bc_order_override

        if str(bc_order['payment_status']).lower() in ['declined', '']:
            oapi.error_handler.add_error_v('Order payment declined')
            oapi.error_handler.add_error_v(f"Payment status: '{bc_order["payment_status"]}'")
            raise Exception('Order payment declined')

        cust_no = ''

        if cust_no_override is None:
            try:
                if not oapi.has_cust_info(bc_order):
                    CounterPointAPI.logger.info('Creating new customer')
                    oapi.create_new_customer(bc_order)
                    cust_no = OrderAPI.get_cust_no(bc_order)

                else:
                    CounterPointAPI.logger.info('Updating existing customer')
                    cust_no = OrderAPI.get_cust_no(bc_order)
                    oapi.update_cust(bc_order, cust_no)
            except:
                raise Exception('Customer could not be created/updated')

        if cust_no_override is None:
            if cust_no is None or cust_no == '' or not oapi.has_cust(cust_no):
                oapi.error_handler.add_error_v('Valid customer number is required')
                raise Exception('Valid customer number is required')
        else:
            cust_no = cust_no_override
            if not oapi.has_cust(cust_no):
                oapi.error_handler.add_error_v('Valid customer number is required')
                raise Exception('Valid customer number is required')

        try:
            if bc_order['status'] == 'Partially Refunded':
                oapi.post_partial_refund(cust_no=cust_no, bc_order=bc_order)
            else:
                # This is the main function called
                oapi.post_bc_order(cust_no=cust_no, bc_order=bc_order)
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

    # Returns true if the provided ticket number exists in the PS_DOC_HDR table.
    def tkt_num_exists(self, tkt_num: str, suffix: str = '', index: int = 1):
        query = f"""
        SELECT TKT_NO FROM PS_DOC_HDR
        WHERE TKT_NO like '{tkt_num}{suffix}{index}'
        """

        response = Database.query(query)

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
    def get_refund_index(self, tkt_num: str, suffix: str = ''):
        if self.refund_index is not None:
            return self.refund_index

        index = 1
        found = False
        while not found:
            if self.tkt_num_exists(tkt_num=tkt_num, suffix=suffix, index=index):
                index += 1
            else:
                found = True

        self.refund_index = index
        return index

    # Post a partial refund to Counterpoint.
    def post_partial_refund(self, cust_no: str, bc_order: dict):
        self.logger.info('Posting order as partial refund')

        self.is_partial_refund = True
        self.refund = True

        if cust_no is None or cust_no == '' or not self.has_cust(cust_no):
            self.error_handler.add_error_v('Valid customer number is required')
            return

        payload = self.get_post_order_payload(cust_no, bc_order)

        response = self.post_document(payload)

        if response['ErrorCode'] == 'SUCCESS':
            self.logger.success(f"Order {response['Documents'][0]['DOC_ID']} created")
        else:
            self.error_handler.add_error_v('Order could not be created')
            self.error_handler.add_error_v(response.content)

        try:
            doc_id = response['Documents'][0]['DOC_ID']
        except:
            self.error_handler.add_error_v('Document ID could not be retrieved')
            return

        try:
            if payload['PS_DOC_HDR']['TKT_NUM'] and payload['PS_DOC_HDR']['TKT_NUM'] != '':
                refund_index = self.get_refund_index(
                    tkt_num=payload['PS_DOC_HDR']['TKT_NUM'], suffix=PARTIAL_REFUND_SUFFIX
                )
                self.write_ticket_no(
                    doc_id, f"{payload["PS_DOC_HDR"]["TKT_NUM"]}{PARTIAL_REFUND_SUFFIX}{refund_index}"
                )
        except:
            pass

        self.write_loyalty(doc_id, cust_no, payload['PS_DOC_HDR']['PS_DOC_LIN'])
        self.write_discounts(doc_id, bc_order, payload['PS_DOC_HDR']['PS_DOC_LIN'])

        if self.is_refund(bc_order):
            self.refund_writes(doc_id, payload, bc_order)

        self.more_writes(doc_id, payload, bc_order)

        self.logger.success(f'Order {doc_id} created')

        return response

    # Post an order/refund to Counterpoint.
    def post_bc_order(self, cust_no: str, bc_order: dict):
        self.logger.info('Posting order')

        if cust_no is None or cust_no == '' or not self.has_cust(cust_no):
            self.error_handler.add_error_v('Valid customer number is required')
            return

        print(f'Getting payload for customer: {cust_no}, order: \n{bc_order}\n')

        payload = self.get_post_order_payload(cust_no, bc_order)

        print(f'Payload: \n{payload}\n')

        response = self.post_document(payload)

        if response['ErrorCode'] == 'SUCCESS':
            self.logger.success(f"Order {response['Documents'][0]['DOC_ID']} created")
        else:
            self.error_handler.add_error_v('Order could not be created')
            raise Exception(response)

        try:
            doc_id = response['Documents'][0]['DOC_ID']
        except:
            self.error_handler.add_error_v('Document ID could not be retrieved')
            raise Exception('Document ID could not be retrieved')

        # WRITE TICKET NUMBER

        try:
            if payload['PS_DOC_HDR']['TKT_NUM'] and payload['PS_DOC_HDR']['TKT_NUM'] != '':
                if self.is_refund(bc_order):
                    refund_index = self.get_refund_index(
                        tkt_num=payload['PS_DOC_HDR']['TKT_NUM'], suffix=REFUND_SUFFIX
                    )
                    self.write_ticket_no(doc_id, f"{payload["PS_DOC_HDR"]["TKT_NUM"]}{REFUND_SUFFIX}{refund_index}")
                else:
                    self.write_ticket_no(doc_id, f"{payload["PS_DOC_HDR"]["TKT_NUM"]}")
        except:
            pass

        # WRITE PS_DOC_GFC

        if len(payload['PS_DOC_HDR']['__PS_DOC_GFC__']) > 0 and not self.is_refund(bc_order):
            for gift_card in payload['PS_DOC_HDR']['__PS_DOC_GFC__']:
                query = f"""
                INSERT INTO PS_DOC_GFC
                (DOC_ID, GFC_COD, GFC_NO, AMT, LIN_SEQ_NO, DESCR, CREATE_AS_STC, GFC_SEQ_NO)
                VALUES
                ('{doc_id}', '{gift_card["GFC_COD"]}', '{gift_card["GFC_NO"]}', {gift_card["AMT"]}, {gift_card["LIN_SEQ_NO"]}, '{gift_card["DESCR"]}', '{gift_card["CREATE_AS_STC"]}', {gift_card["GFC_SEQ_NO"]})
                """

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Gift card written')
                else:
                    self.error_handler.add_error_v('Gift card could not be written')
                    self.error_handler.add_error_v(response['message'])

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

        self.write_loyalty(doc_id, cust_no, payload['PS_DOC_HDR']['PS_DOC_LIN'])
        self.write_discounts(doc_id, bc_order, payload['PS_DOC_HDR']['PS_DOC_LIN'])

        if self.is_refund(bc_order):
            self.refund_writes(doc_id, payload, bc_order)

        self.more_writes(doc_id, payload, bc_order)

        self.logger.success(f'Order {doc_id} created')

        return response

    # Writes the correct ticket number to the document.
    def write_ticket_no(self, doc_id, tkt_no):
        tables = ['PS_DOC_HDR', 'PS_DOC_LIN', 'PS_DOC_PMT']

        for table in tables:
            query = f"""
            UPDATE {table}
            SET TKT_NO = '{tkt_no}'
            WHERE DOC_ID = '{doc_id}'
            """

            response = Database.query(query)

            if response['code'] == 200:
                self.logger.success('Ticket number updated.')
            elif response['code'] == 201:
                self.logger.warn(f'DOC ID not found in {table}.')
            else:
                self.error_handler.add_error_v(f'Ticket number could not be updated in {table}.')
                self.error_handler.add_error_v(response['message'])

    # Writes to several tables in Counterpoint.
    # Necessary to process refunds correctly.
    # This function is called before more_writes on refunds and partial refunds.
    def refund_writes(self, doc_id, payload, bc_order):
        self.logger.info('Writing refund data')

        if self.is_partial_refund:
            sub_tot = 0

            for line_item in payload['PS_DOC_HDR']['PS_DOC_LIN']:
                sub_tot += float(line_item['EXT_PRC'])

            # bc_order['subtotal_ex_tax'] = float(bc_order['refunded_amount'] or 0) + float(
            #     self.total_discount_amount or 0
            # ) / float(bc_order['items_total'] or 1)
            # bc_order['subtotal_inc_tax'] = float(bc_order['refunded_amount'] or 0) + float(
            #     self.total_discount_amount or 0
            # ) / float(bc_order['items_total'] or 1)

        def commit_query(query):
            response = Database.query(query)

            return response

        # REMOVE GIFT CARD BALANCE
        if len(payload['PS_DOC_HDR']['__PS_DOC_GFC__']) > 0:
            for gift_card in payload['PS_DOC_HDR']['__PS_DOC_GFC__']:
                card_no = gift_card['GFC_NO']

                def get_gfc_bal():
                    query = f"""
                    SELECT CURR_AMT FROM SY_GFC
                    WHERE GFC_NO = '{card_no}'
                    """

                    response = Database.query(query)
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

                    response = Database.query(query)

                    try:
                        return int(response[0][0]) + 1
                    except:
                        return 1

                def add_gfc_bal(amt: float | int):
                    current_date = datetime.now().strftime('%Y-%m-%d')

                    tkt_no = payload['PS_DOC_HDR']['TKT_NUM']

                    if self.is_partial_refund:
                        refund_index = int(
                            self.get_refund_index(
                                tkt_num=payload['PS_DOC_HDR']['TKT_NUM'], suffix=PARTIAL_REFUND_SUFFIX
                            )
                        )
                        tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{PARTIAL_REFUND_SUFFIX}{refund_index}"
                    else:
                        refund_index = int(
                            self.get_refund_index(tkt_num=payload['PS_DOC_HDR']['TKT_NUM'], suffix=REFUND_SUFFIX)
                        )
                        tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{REFUND_SUFFIX}{refund_index}"

                    r = commit_query(
                        f"""
                        INSERT INTO SY_GFC_ACTIV
                        (GFC_NO, SEQ_NO, DAT, STR_ID, STA_ID, DOC_NO, ACTIV_TYP, AMT, LST_MAINT_DT, LST_MAINT_USR_ID, DOC_ID)
                        VALUES
                        ('{card_no}', {get_next_seq_no()}, '{current_date}', 'WEB', 'WEB', '{tkt_no}', 'R', {amt}, GETDATE(), 'POS', '{doc_id}')
                        """
                    )

                    if r['code'] == 200:
                        self.logger.success('Gift card balance updated')
                    else:
                        self.error_handler.add_error_v('Gift card balance could not be updated')
                        self.error_handler.add_error_v(r['message'])

                    r = commit_query(
                        f"""
                        UPDATE SY_GFC
                        SET CURR_AMT = {0}
                        WHERE GFC_NO = '{card_no}'
                        """
                    )

                    if r['code'] == 200:
                        self.logger.success('Gift card balance updated')
                    else:
                        self.error_handler.add_error_v('Gift card balance could not be updated')
                        self.error_handler.add_error_v(r['message'])

                add_gfc_bal(get_bal_diff(0))

        r = commit_query(
            f"""
            UPDATE PS_DOC_PMT
            SET AMT = {-(float(bc_order["total_inc_tax"] or 0))},
            HOME_CURNCY_AMT = {-(float(bc_order["total_inc_tax"] or 0))} 
            WHERE DOC_ID = '{doc_id}'
            """
        )

        if r['code'] == 200:
            self.logger.success('Updated payment')
        else:
            self.error_handler.add_error_v('Payment could not be updated')
            self.error_handler.add_error_v(r['message'])

        total_paid = -(float(bc_order['total_inc_tax'] or 0))

        # PARTIAL REFUND PAYMENT WRITES
        if self.is_partial_refund:

            def get_ps_doc_pmt_index(pay_cod: str):
                index = 0

                for i, pmt in enumerate(payload['PS_DOC_HDR']['PS_DOC_PMT']):
                    if pmt['PAY_COD'] == pay_cod:
                        index = i

                return index

            def get_total():
                query = f"""
                SELECT SUM(EXT_PRC) FROM PS_DOC_LIN
                WHERE DOC_ID = '{doc_id}'
                """

                response = Database.query(query)

                try:
                    return abs(float(response[0][0]))
                except:
                    return 0

            def get_big_payment():
                query = f"""
                SELECT AMT FROM PS_DOC_PMT
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'SHOP'
                """

                response = Database.query(query)

                try:
                    return abs(float(response[0][0]))
                except:
                    return 0

            def has_loy():
                query = f"""
                SELECT COUNT(*) FROM PS_DOC_PMT
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'LOYALTY'
                """

                response = Database.query(query)

                try:
                    return int(response[0][0]) > 0
                except:
                    return False

            def has_gc():
                query = f"""
                SELECT COUNT(*) FROM PS_DOC_PMT
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'GC'
                """

                response = Database.query(query)

                try:
                    return int(response[0][0]) > 0
                except:
                    return False

            def write_big_payment(amt: float | int):
                query = f"""
                UPDATE PS_DOC_PMT
                SET AMT = {-amt},
                HOME_CURNCY_AMT = {-amt}
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'SHOP'
                """

                payload['PS_DOC_HDR']['PS_DOC_PMT'][get_ps_doc_pmt_index('SHOP')]['AMT'] = -amt

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Big payment updated')
                else:
                    self.error_handler.add_error_v('Big payment could not be updated')
                    self.error_handler.add_error_v(response['message'])

                r = commit_query(
                    f"""
                    UPDATE PS_DOC_PMT_APPLY
                    SET AMT = {-amt},
                    HOME_CURNCY_AMT = {-amt}
                    WHERE DOC_ID = '{doc_id}' AND PMT_SEQ_NO in (
                        SELECT PMT_SEQ_NO FROM PS_DOC_PMT WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'SHOP'
                    )
                    """
                )

                if r['code'] == 200:
                    self.logger.success('Payment applied')
                else:
                    self.error_handler.add_error_v('Payment could not be applied')
                    self.error_handler.add_error_v(r['message'])

            def write_loy_payment(amt: float | int):
                query = f"""
                UPDATE PS_DOC_PMT
                SET AMT = {-amt},
                HOME_CURNCY_AMT = {-amt}
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'LOYALTY'
                """

                payload['PS_DOC_HDR']['PS_DOC_PMT'][get_ps_doc_pmt_index('LOYALTY')]['AMT'] = -amt

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Loyalty payment updated')
                else:
                    self.error_handler.add_error_v('Loyalty payment could not be updated')
                    self.error_handler.add_error_v(response['message'])

                r = commit_query(
                    f"""
                    UPDATE PS_DOC_PMT_APPLY
                    SET AMT = {-amt},
                    HOME_CURNCY_AMT = {-amt}
                    WHERE DOC_ID = '{doc_id}' AND PMT_SEQ_NO in (
                        SELECT PMT_SEQ_NO FROM PS_DOC_PMT WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'LOYALTY'
                    )
                    """
                )

                if r['code'] == 200:
                    self.logger.success('Payment applied')
                else:
                    self.error_handler.add_error_v('Payment could not be applied')
                    self.error_handler.add_error_v(r['message'])

            def write_gc_payment(amt: float | int):
                query = f"""
                UPDATE PS_DOC_PMT
                SET AMT = {-amt},
                HOME_CURNCY_AMT = {-amt}
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'GC'
                """

                payload['PS_DOC_HDR']['PS_DOC_PMT'][get_ps_doc_pmt_index('GC')]['AMT'] = -amt

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Gift card payment updated')
                else:
                    self.error_handler.add_error_v('Gift card payment could not be updated')
                    self.error_handler.add_error_v(response['message'])

                r = commit_query(
                    f"""
                    UPDATE PS_DOC_PMT_APPLY
                    SET AMT = {-amt},
                    HOME_CURNCY_AMT = {-amt}
                    WHERE DOC_ID = '{doc_id}' AND PMT_SEQ_NO in (
                        SELECT PMT_SEQ_NO FROM PS_DOC_PMT WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'GC'
                    )
                    """
                )

                if r['code'] == 200:
                    self.logger.success('Payment applied')
                else:
                    self.error_handler.add_error_v('Payment could not be applied')
                    self.error_handler.add_error_v(r['message'])

            # total = get_total()
            total = bc_order['total_inc_tax'] or 0

            big_payment = get_big_payment()

            remaining = 0

            if total > big_payment:
                remaining = total - big_payment

            big = total if total < big_payment else big_payment
            gc = (remaining / 2 if has_loy() else remaining) if has_gc() else 0
            loy = (remaining / 2 if has_gc() else remaining) if has_loy() else 0

            if big_payment > 0:
                write_big_payment(big)
            if has_gc():
                write_gc_payment(gc)
            if has_loy():
                write_loy_payment(loy)

            total_paid = big + gc + loy

        # PAYMENT REFUND WRITES
        for payment in payload['PS_DOC_HDR']['PS_DOC_PMT']:
            if payment['PAY_COD'] == 'GC':
                amt_spent = float(payment['AMT'])
                card_no = payment['CARD_NO']

                tkt_no = payload['PS_DOC_HDR']['TKT_NUM']

                if self.is_partial_refund():
                    refund_index = int(
                        self.get_refund_index(
                            tkt_num=payload['PS_DOC_HDR']['TKT_NUM'], suffix=PARTIAL_REFUND_SUFFIX
                        )
                    )
                    tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{PARTIAL_REFUND_SUFFIX}{refund_index}"
                else:
                    refund_index = int(
                        self.get_refund_index(tkt_num=payload['PS_DOC_HDR']['TKT_NUM'], suffix=REFUND_SUFFIX)
                    )
                    tkt_no = f"{payload['PS_DOC_HDR']['TKT_NUM']}{REFUND_SUFFIX}{refund_index}"

                def get_last_gfc_activity_index():
                    query = f"""
                    SELECT MAX(SEQ_NO) FROM SY_GFC_ACTIV
                    WHERE GFC_NO = '{card_no}'
                    """

                    response = Database.query(query)

                    try:
                        return int(response[0][0])
                    except:
                        return 1

                query = f"""
                UPDATE SY_GFC_ACTIV
                SET AMT = {abs(amt_spent)},
                DOC_NO = '{tkt_no}'
                WHERE GFC_NO = '{card_no}' AND SEQ_NO = {get_last_gfc_activity_index()}
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
                query = f"""
                UPDATE {creds.Table.CP.Customers.table}
                SET LOY_PTS_BAL = LOY_PTS_BAL + {abs(math.floor(float(payment["AMT"])))}
                WHERE CUST_NO = '{payload["PS_DOC_HDR"]["CUST_NO"]}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Loyalty points added')
                else:
                    self.error_handler.add_error_v('Loyalty points could not be added')
                    self.error_handler.add_error_v(response['message'])

        # PAYMENT APPLY REFUND
        if not self.is_partial_refund():
            r = commit_query(
                f"""
                UPDATE PS_DOC_PMT_APPLY
                SET AMT = {total_paid},
                HOME_CURNCY_AMT = {total_paid}
                WHERE DOC_ID = '{doc_id}' AND PMT_SEQ_NO in (
                    SELECT PMT_SEQ_NO FROM PS_DOC_PMT WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'SHOP'
                )
                """
            )

            if r['code'] == 200:
                self.logger.success('Updated payment application')
            else:
                self.error_handler.add_error_v('Payment application could not be updated')
                self.error_handler.add_error_v(r['message'])

        def invert_line_qty(line_item: dict, index: int):
            qty = -line_item['QTY_SOLD']

            r = commit_query(
                f"""
                UPDATE PS_DOC_LIN
                SET QTY_SOLD = {qty},
                EXT_PRC = {qty * line_item["PRC"]},
                EXT_COST = {qty * line_item["EXT_COST"]}
                WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                """
            )

            if r['code'] == 200:
                self.logger.success(f'Line {index} inverted')
            else:
                self.error_handler.add_error_v(f'Line {index} could not be inverted')
                self.error_handler.add_error_v(r['message'])

        for i, line_item in enumerate(payload['PS_DOC_HDR']['PS_DOC_LIN'], start=1):
            invert_line_qty(line_item, i)

        def get_value(table, column, index):
            query = f"""
            SELECT {column} FROM {table}
            WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
            """

            response = Database.query(query)

            try:
                return float(response[0][0]) if response else None
            except Exception as e:
                self.error_handler.add_error_v(f'[{table}] Line {index} {column} could not be retrieved')
                raise e

        def set_value(table, column, value, index):
            r = commit_query(
                f"""
                UPDATE {table}
                SET {column} = {value}
                WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                """
            )

            if r['code'] == 200:
                self.logger.success(f'[{table}] Line {index} {column} set to {value}')
            else:
                self.error_handler.add_error_v(f'[{table}] Line {index} {column} could not be set to {value}')
                self.error_handler.add_error_v(r['message'])

        def negative_column(table: str, column: str, index: int):
            set_value(table, column, -get_value(table, column, index), index)

        for i, line_item in enumerate(payload['PS_DOC_HDR']['PS_DOC_LIN'], start=1):
            for column in ['EXT_COST', 'ORIG_QTY', 'GROSS_EXT_PRC', 'GROSS_DISP_EXT_PRC', 'CALC_EXT_PRC']:
                negative_column('PS_DOC_LIN', column, i)

            def set_value_lin(column, value):
                set_value('PS_DOC_LIN', column, value, i)

            def get_value_lin(column):
                return get_value('PS_DOC_LIN', column, i)

            set_value_lin('QTY_ENTD', 0)
            set_value_lin('QTY_TO_REL', get_value_lin('QTY_SOLD'))
            set_value_lin('QTY_TO_LEAVE', 0)

            def set_value_prc(column, value):
                set_value('PS_DOC_LIN_PRICE', column, value, i)

            def get_value_prc(column):
                return get_value('PS_DOC_LIN_PRICE', column, i)

            def invert_prc(column):
                set_value_prc(column, -get_value_prc(column))

            set_value_prc('PRC_RUL_SEQ_NO', -1)
            set_value_prc('PRC_BRK_DESCR', "'I'")
            invert_prc('QTY_PRCD')

    # Updates the users loyalty points
    def redeem_loyalty_pmts(self, doc_id, payload, bc_order):
        for payment in payload['PS_DOC_HDR']['PS_DOC_PMT']:
            if payment['PAY_COD'] == 'LOYALTY':
                query = f"""
                UPDATE {creds.Table.CP.Customers.table}
                SET LOY_PTS_BAL = LOY_PTS_BAL - {abs(math.floor(float(payment["AMT"])))}
                WHERE CUST_NO = '{payload["PS_DOC_HDR"]["CUST_NO"]}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    self.logger.success('Loyalty points added')
                else:
                    self.error_handler.add_error_v('Loyalty points could not be added')
                    self.error_handler.add_error_v(response['message'])

    # def earn_loyalty_points(self, payload, )

    # Writes to several tables in Counterpoint.
    def more_writes(self, doc_id, payload, bc_order):
        if not self.is_refund():
            self.redeem_loyalty_pmts(doc_id, payload, bc_order)

        self.logger.info('Writing tables')

        def convert_date_string_to_datetime(date_string):
            date = None

            try:
                date = datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')
            except:
                try:
                    # 2024-07-27T17:20:30Z
                    date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
                except:
                    pass

            date = date.replace(tzinfo=timezone.utc).astimezone(tz=None)

            return date

        def convert_datetime_to_date_string(date):
            date_string = date.strftime('%Y-%m-%d %H:%M:%S.%f')
            return date_string[:-3]

        def double_convert_date(date_str):
            date = convert_date_string_to_datetime(date_str)
            return convert_datetime_to_date_string(date)

        date = double_convert_date(bc_order['date_created'])

        query = f"""
        UPDATE PS_DOC_HDR
        SET TKT_DT = '{date}'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Date updated')
        else:
            self.error_handler.add_error_v('Date could not be updated')
            self.error_handler.add_error_v(response['message'])

        def get_tndr():
            total = 0

            for payment in payload['PS_DOC_HDR']['PS_DOC_PMT']:
                total += abs(float(payment['AMT']))

            return total

        tot_tndr = float(bc_order['refund_total'] or 0) if self.is_refund() else get_tndr()

        query = f"""
        DELETE FROM PS_DOC_HDR_TOT
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Total removed')
        else:
            self.error_handler.add_error_v('Total could not be removed')
            self.error_handler.add_error_v(response['message'])

        query = f"""
        UPDATE PS_DOC_HDR
        SET LOY_PGM_COD = 'BASIC'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Loyalty program code updated')
        else:
            self.error_handler.add_error_v('Loyalty program code could not be updated')
            self.error_handler.add_error_v(response['message'])

        sub_tot = float(bc_order['subtotal_ex_tax'] or 0)
        tot = float(bc_order['total_inc_tax'] or 0)
        document_discount = float(self.total_discount_amount or 0)
        gfc_amount = float(self.total_gfc_amount)

        tot_ext_cost = 0

        for line_item in payload['PS_DOC_HDR']['PS_DOC_LIN']:
            try:
                self.logger.info(f"Getting cost for {line_item['ITEM_NO']}")

                query = f"""
                SELECT LST_COST FROM IM_ITEM
                WHERE ITEM_NO = '{line_item["ITEM_NO"]}'
                """

                response = Database.query(query)

                tot_ext_cost += float(response[0][0])
            except Exception as e:
                self.error_handler.add_error_v('Could not get cost')
                self.error_handler.add_error_v(str(e))

        if self.is_refund(bc_order):
            self.total_lin_disc = abs(self.total_lin_disc)
            query = f"""
            INSERT INTO PS_DOC_HDR_TOT
            (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
            VALUES
            ('{doc_id}', 'S', 0, '!', 0, {len(payload["PS_DOC_HDR"]["PS_DOC_LIN"])}, 0, 0, {-(sub_tot)}, 0, {-tot_ext_cost}, 0, 0, 0, {tot_tndr}, {0}, 0, 0, {-tot}, 0, {self.total_hdr_disc}, {self.total_lin_disc}, 0, 0)
            """
        else:
            query = f"""
            INSERT INTO PS_DOC_HDR_TOT
            (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
            VALUES
            ('{doc_id}', 'S', 0, '!', 0, {len(payload["PS_DOC_HDR"]["PS_DOC_LIN"])}, {gfc_amount}, 0, {sub_tot - document_discount}, 0, {tot_ext_cost}, 0, 0, 0, {tot_tndr}, 0, 0, 0, {tot_tndr}, 0, {self.total_hdr_disc}, {self.total_lin_disc}, {sub_tot}, 0)
            """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Total written')
        else:
            self.error_handler.add_error_v('Total could not be written')
            self.error_handler.add_error_v(response['message'])

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

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Updated line types')
        elif response['code'] == 201:
            self.logger.info('No lines to update.')
        else:
            self.error_handler.add_error_v('Line types could not be updated')

        query = f"""
        UPDATE PS_DOC_PMT_APPLY
        SET APPL_TYP = 'S'
        WHERE DOC_ID = '{doc_id}'
        """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Updated payment application types')
        else:
            self.error_handler.add_error_v('Payment application types could not be updated')
            self.error_handler.add_error_v(response['message'])

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

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Updated line items')
        else:
            self.error_handler.add_error_v('Line items could not be updated')
            self.error_handler.add_error_v(response['message'])

        if self.is_refund(bc_order):
            pass
        else:
            query = f"""
            UPDATE PS_DOC_HDR
            SET TO_REL_LINS = {self.total_lin_items}
            WHERE DOC_ID = '{doc_id}'
            """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Updated line items to release')
        else:
            self.error_handler.add_error_v('Line items to release could not be updated')
            self.error_handler.add_error_v(response['message'])

        if self.is_refund(bc_order):
            query = f"""
            UPDATE PS_DOC_HDR
            SET RET_LIN_TOT = {(float(bc_order["total_inc_tax"] or 0))}
            WHERE DOC_ID = '{doc_id}'
            """
        else:
            query = f"""
            UPDATE PS_DOC_HDR
            SET SAL_LIN_TOT = {float(bc_order["total_inc_tax"] or 0)}
            WHERE DOC_ID = '{doc_id}'
            """

        response = Database.query(query)

        if response['code'] == 200:
            self.logger.success('Updated line total')
        else:
            self.error_handler.add_error_v('Line total could not be updated')
            self.error_handler.add_error_v(response['message'])

        def get_orig_doc_id():
            query = f"""
            SELECT ORIG_DOC_ID FROM PS_DOC_HDR_ORIG_DOC WHERE DOC_ID = '{doc_id}'
            """

            response = Database.query(query)
            try:
                return response[0][0]
            except:
                return None

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

        if not self.is_refund(bc_order):

            def commit_query(query):
                response = Database.query(query)
                return response

            def get_value(table, column, index):
                query = f"""
                SELECT {column} FROM {table}
                WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                """

                response = Database.query(query)

                try:
                    return float(response[0][0]) if response else None
                except Exception as e:
                    self.error_handler.add_error_v(f'[{table}] Line {index} {column} could not be retrieved')
                    raise e

            def set_value(table, column, value, index):
                r = commit_query(
                    f"""
                    UPDATE {table}
                    SET {column} = {value}
                    WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                    """
                )

                if r['code'] == 200:
                    self.logger.success(f'[{table}] Line {index} {column} set to {value}')
                else:
                    self.error_handler.add_error_v(f'[{table}] Line {index} {column} could not be set to {value}')
                    self.error_handler.add_error_v(r['message'])

            for i, line_item in enumerate(payload['PS_DOC_HDR']['PS_DOC_LIN'], start=1):

                def set_value_lin(column, value):
                    set_value('PS_DOC_LIN', column, value, i)

                def get_value_lin(column):
                    return get_value('PS_DOC_LIN', column, i)

                set_value_lin('QTY_ENTD', 0)
                set_value_lin('QTY_TO_REL', get_value_lin('QTY_SOLD'))
                set_value_lin('QTY_TO_LEAVE', 0)

        self.cleanup(doc_id)

    # Remove the original document and the reference to the original document
    # The original document is the ORDER document that was created at the NCR Counterpoint API POST.
    # The original document is different than the final TICKET document that we see and is not needed.
    def cleanup(self, doc_id):
        def get_orig_doc_id():
            query = f"""
            SELECT ORIG_DOC_ID FROM PS_DOC_HDR_ORIG_DOC WHERE DOC_ID = '{doc_id}'
            """

            response = Database.query(query)
            try:
                return response[0][0]
            except:
                return None

        self.logger.info(f'Cleaning up document {get_orig_doc_id()}')

        def get_customer_no():
            query = f"""
            SELECT CUST_NO FROM PS_DOC_HDR WHERE DOC_ID = '{doc_id}'
            """

            response = Database.query(query)

            try:
                return response[0][0]
            except:
                return None

        customer_number = get_customer_no()

        # Update the customer order count
        query = f"""
        UPDATE AR_CUST
        SET NO_OF_ORDS = NO_OF_ORDS - 1
        WHERE CUST_NO = '{customer_number}'"""
        response = Database.query(query)
        if response['code'] == 200:
            self.logger.success('Updated customer order count')
        else:
            self.error_handler.add_error_v('Customer order count could not be updated')
            self.error_handler.add_error_v(response['message'])

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
        return customers.lookup_customer(email_address=user_info['email'], phone_number=user_info['phone'])

    # Get the customer's phone number from the BigCommerce order
    @staticmethod
    def get_cust_phone(bc_order: dict):
        try:
            phone = bc_order['billing_address']['phone']

            if phone is None or phone == '':
                phone = bc_order['shipping_addresses']['url'][0]['phone']

            return phone
        except:
            return ''

    # Get the customer's email address from the BigCommerce order
    @staticmethod
    def get_cust_email(bc_order: dict):
        try:
            email = bc_order['billing_address']['email']

            if email is None or email == '':
                email = bc_order['shipping_addresses']['url'][0]['email']

            return email
        except:
            return ''

    # Get the customer's number from the BigCommerce order
    @staticmethod
    def get_cust_no(bc_order: dict):
        user_info = {'email': OrderAPI.get_cust_email(bc_order), 'phone': OrderAPI.get_cust_phone(bc_order)}

        cust_no = OrderAPI.get_customer_from_info(user_info)

        return cust_no

    # Get the BigCommerce order object for a given order ID.
    @staticmethod
    def get_order(order_id: str | int):
        url = f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/orders/{order_id}'
        order = JsonTools.get_json(url)

        order['transactions'] = JsonTools.get_json(
            f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/orders/{order_id}/transactions'
        )
        order = JsonTools.unpack(order)

        return order


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
