from setup import creds
import requests
import json
import math
from datetime import datetime
from database import Database
from setup.error_handler import ProcessInErrorHandler
from customer_tools import customers
from integration.shopify_api import Shopify
from integration.models.shopify_orders import ShopifyOrder, GiftCard, InventoryItem, Delivery
from integration.models.cp_orders import CPNote
from traceback import format_exc as tb


ORDER_PREFIX = 'S'
REFUND_SUFFIX = 'R'
LOYALTY_EXCLUSIONS = ['SERVICE', 'DELIVERY']
LOYALTY_CUSTOMER_EXCLUSIONS = ['CASH']
SHOPIFY_PAYCODE = 'SHOP'
LOYALTY_PAYCODE = 'LOYALTY'
GIFT_CARD_PAYCODE = 'GC'
LOYALTY_MULTIPLIER = 0.05


class CounterPointAPI:
    """This class is used to interact with the NCR Counterpoint API.
    https://github.com/NCRCounterpointAPI/APIGuide/blob/master/Endpoints/POST_Document.md"""

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

    def post(self, url, payload: dict):
        response = self.session.post(url, headers=self.post_headers, json=payload, verify=False)

        return response


class DocumentAPI(CounterPointAPI):
    """This class is used to interact with the NCR Counterpoint API's Document endpoint"""

    def __init__(self, session: requests.Session = requests.Session()):
        super().__init__(session=session)
        self.base_url = f'{self.base_url}Document'
        self.payload: dict = None

    def get_document(self, doc_id):
        url = f'{self.base_url}/{doc_id}'

        response = self.get(url)

        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)

        return pretty

    def post_document(self) -> dict:
        url = self.base_url

        response = self.post(url, payload=self.payload)

        return response.json()


class OrderAPI(DocumentAPI):
    """This class is used to create orders and refunds."""

    def __init__(self, order: ShopifyOrder, session: requests.Session = requests.Session(), verbose: bool = False):
        super().__init__(session=session)
        self.verbose: bool = verbose
        self.order: ShopifyOrder = order
        self.is_refund: bool = self.order.is_refund
        self.doc_id: str = None
        self.orig_doc_id: str = None
        self.cp_tkt_no: str = None  # S1133
        self.cp_tkt_no_full: str = None  # S1133R1 or S1133 or S1133PR1
        self.discount_seq_no: int = 1
        self.total_gfc_amount: float = 0
        self.total_hdr_disc: float = 0
        self.total_lin_disc: float = 0
        self.refund_index: int = None
        self.total_tender: float = 0
        self.total_lin_items: int = 0
        self.store_id: str = OrderAPI.get_store_id(self.order)
        self.station_id: str = OrderAPI.get_station_id(self.order)
        self.drawer_id: str = OrderAPI.get_drawer_id(self.order)
        self.cust_no: str = OrderAPI.get_customer_number(self.order)
        self.notes: list[CPNote] = self.get_notes()
        self.payload = self.get_post_payload()

    def get_notes(self) -> list[CPNote]:
        """Returns a list of notes from a order."""
        notes = []
        if self.order.customer_message:
            notes.append(CPNote(self.order))
        return notes

    def get_ps_doc_lin(self) -> list[dict]:
        result = []
        for item in self.order.line_items:
            if isinstance(item, InventoryItem | Delivery):
                result.append(item.payload)
        return result

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
                'CUST_NO': self.cust_no,
                'TKT_TYP': 'T',
                'DOC_TYP': 'O',
                'USR_ID': 'POS',
                'HAS_ENTD_LINS': 'N',
                'TAX_COD': 'EXEMPT',
                'NORM_TAX_COD': 'EXEMPT',
                'SHIP_VIA_COD': 'CPC_FLAT' if self.is_refund else 'T' if order.is_shipping else 'C',
                'PS_DOC_NOTE': [note.payload for note in self.notes],
                'PS_DOC_LIN': self.get_ps_doc_lin(),
                'PS_DOC_PMT': self.order.payments.payload,
                'PS_DOC_TAX': [
                    {
                        'AUTH_COD': 'EXEMPT',
                        'RUL_COD': 'TAX',
                        'TAX_DOC_PART': 'S',
                        'TAX_AMT': '0',
                        'TOT_TXBL_AMT': order.total - order.shipping_cost,
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

        if self.verbose:
            self.logger.info(f'\nPayload: \n\n{json.dumps(payload, indent=4)}\n')

        return payload

    def write_discounts(self):
        """Write discounts to the PS_DOC_DISC table."""
        # Processing document discounts
        if self.order.total_discount:
            Database.CP.Discount.write_discount(
                doc_id=self.doc_id,
                disc_seq_no=self.discount_seq_no,
                disc_amt=-self.order.total_discount if self.is_refund else self.order.total_discount,
                apply_to='H',  # L for line, H for header
                disc_type='A',  # Amount,
                disc_pct=0,
                disc_amt_shipped=0,
            )
            self.total_discount_amount = self.order.total_discount

        # #  Processing line discounts
        # for line_item in self.order.line_items:
        #     if isinstance(line_item, GiftCard):
        #         continue

        #     if line_item.discount_amount and not line_item.is_refunded:
        #         Database.CP.Discount.write_discount(
        #             doc_id=self.doc_id,
        #             disc_seq_no=self.discount_seq_no,
        #             disc_amt=line_item.unit_retail_value * line_item.quantity - line_item.extended_price,
        #             apply_to='L',
        #             disc_type='A',
        #             disc_pct=0,
        #             disc_amt_shipped=0,
        #             lin_seq_no=line_item.lin_seq_no,
        #         )
        #         self.total_lin_disc += line_item.extended_discount
        #         self.discount_seq_no += 1

    def write_loyalty(self):
        self.logger.info('Writing loyalty')
        if self.cust_no in LOYALTY_CUSTOMER_EXCLUSIONS:
            self.logger.warn('Customer is CASH. Loyalty not applied')
            return

        points_earned: int = 0
        points_redeemed = Database.CP.Loyalty.get_points_used(self.doc_id)
        point_balance = Database.CP.Customer.get_loyalty_balance(self.cust_no)

        refund_multiplier = -1 if self.is_refund else 1

        for line in self.order.line_items:
            if isinstance(line, GiftCard):
                continue

            if line.sku in LOYALTY_EXCLUSIONS:
                continue

            item_points_earned = (float(line.extended_price or 0) * LOYALTY_MULTIPLIER) or 0
            item_points_earned *= refund_multiplier
            points_earned += item_points_earned
            Database.CP.Loyalty.write_line(self.doc_id, line.lin_seq_no, item_points_earned)

        points_earned = math.floor(points_earned)
        Database.CP.Loyalty.write_ps_doc_hdr_loy_pgm(self.doc_id, points_earned, points_redeemed, point_balance)

        new_bal = point_balance + points_earned
        if new_bal < 0:
            new_bal = 0

        Database.CP.Customer.set_loyalty_balance(self.cust_no, new_bal)

    def create_customer(self):
        """Create a new customer in Counterpoint from an order."""
        OrderAPI.logger.info('Creating new customer')

        order = self.order

        first_name = order.billing_address.first_name or order.shipping_address.first_name
        last_name = order.billing_address.last_name or order.shipping_address.last_name
        phone_number = order.billing_address.phone or order.shipping_address.phone
        email_address = order.billing_address.email or order.shipping_address.email

        street_address = order.billing_address.address_1
        city = order.billing_address.city
        state = order.billing_address.state
        zip_code = order.billing_address.zip

        self.cust_no = customers.add_new_customer(
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
        Database.Shopify.Customer.insert(order.customer.id, self.cust_no)
        if order.shipping_address:
            self.write_shipping_address()

    def update_customer(self):
        """Update an existing customer in Counterpoint from an order."""
        if self.cust_no == 'CASH':
            return
        self.logger.info('Updating existing customer')
        order = self.order
        customer = order.customer
        billing_address = order.billing_address

        customers.update_customer(
            cust_no=self.cust_no,
            first_name=customer.first_name,
            last_name=customer.last_name,
            phone_number=customer.phone,
            email_address=customer.email,
            street_address=billing_address.address_1,
            city=billing_address.city,
            state=billing_address.state,
            zip_code=billing_address.zip,
        )

        # Update the customer in the middleware database
        Database.Shopify.Customer.update(cp_cust_no=self.cust_no, shopify_cust_no=customer.id)

        if order.shipping_address:
            self.write_shipping_address()

    def write_shipping_address(self):
        s = self.order.shipping_address
        first_name = s.first_name
        last_name = s.last_name
        phone_number = s.phone
        email_address = s.email
        street_address = s.address_1
        city = s.city
        state = s.state
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
            cust_no=self.cust_no,
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
        for i in self.order.line_items:
            if isinstance(i, GiftCard):
                self.total_gfc_amount += i.amount
                Database.CP.GiftCard.insert_ps_doc_gfc(
                    doc_id=self.doc_id,
                    pay_code=i.pay_code,
                    card_no=i.number,
                    amount=i.amount,
                    lin_seq_no=i.lin_seq_no,
                    descr=i.description,
                    gfc_seq_no=i.gfc_seq_no,
                )
                Database.CP.GiftCard.insert_sy_gfc(
                    doc_id=self.doc_id,
                    card_no=i.number,
                    tkt_no=self.cp_tkt_no_full,
                    cust_no=self.cust_no,
                    amount=i.amount,
                )
                Database.CP.GiftCard.insert_activity(
                    doc_id=self.doc_id, tkt_no=self.cp_tkt_no, card_no=i.number, amount=i.amount
                )

    def post_order(self):
        """Posts an order/refund/partial refund to Counterpoint."""
        if self.is_refund:
            self.logger.info('Posting order as refund')
        else:
            self.logger.info(f'Posting order: {self.order.id}')

        if not self.cust_no:
            self.error_handler.add_error_v('Valid customer number is required')
            return
        response = self.post_document()

        if response['ErrorCode'] == 'SUCCESS':
            try:
                self.doc_id = response['Documents'][0]['DOC_ID']
            except:
                self.error_handler.add_error_v('Document ID could not be retrieved')
                return
        else:
            self.error_handler.add_error_v('Order could not be created')
            raise Exception(response)

        self.logger.success(f'Order Created. DOC_ID: {self.doc_id}')

        # Set Order Properties
        self.cp_tkt_no = self.payload['PS_DOC_HDR']['TKT_NUM']  # S1133
        self.total_tender = self.order.refund_total if self.is_refund else self.get_total_tender()

        # Write Tables
        self.write_ticket_no()

        if not self.is_refund:
            # Flow for Standard Orders
            self.write_loyalty()
            self.redeem_loyalty_pmts()
            self.write_gift_cards()
        else:
            self.invalidate_gfc_purchases()
            Database.CP.OpenOrder.update_payment_amount(doc_id=self.doc_id, amount=-self.order.total)
            self.refund_payments()

        Database.CP.OpenOrder.set_ticket_date(self.doc_id, self.order.date_created)
        Database.CP.OpenOrder.delete_hdr_total_entry(self.doc_id)
        Database.CP.OpenOrder.set_loyalty_program(self.doc_id)
        self.write_ps_doc_tot()
        Database.CP.OpenOrder.set_line_type(self.doc_id, line_type='R' if self.is_refund else 'S')
        Database.CP.OpenOrder.set_apply_type(self.doc_id, apply_type='S')
        self.update_total_sale_lines()
        self.set_ps_doc_lin_properties()

        # Clean up the original document and the reference to the original document.
        # The original document is the ORDER document that was created at the NCR Counterpoint API POST.
        # The original document is different than the final TICKET document that we see and is not needed.
        self.orig_doc_id = Database.CP.OpenOrder.get_orig_doc_id(self.doc_id)
        self.logger.info(f'Cleaning up document {self.orig_doc_id}')
        Database.CP.Customer.decrement_orders(self.cust_no)
        Database.CP.OpenOrder.delete(self.orig_doc_id)
        Database.CP.OpenOrder.delete(self.doc_id, orig_doc=True)
        self.logger.success(f'Order {self.doc_id} created')
        return response

    def write_ticket_no(self) -> None:
        """Updates tables with the ticket number for the order."""
        if self.cp_tkt_no:
            if self.is_refund:
                suffix = REFUND_SUFFIX
                refund_index = self.get_refund_index(suffix=suffix)
                tkt_no = f'{self.cp_tkt_no}{suffix}{refund_index}'
            else:
                tkt_no = self.cp_tkt_no

        self.cp_tkt_no_full = tkt_no

        Database.CP.OpenOrder.write_ticket_number(doc_id=self.doc_id, tkt_no=tkt_no)

    def invalidate_gfc_purchases(self):
        """Removes the balance from PURCHASED gift cards."""
        for item in self.order.line_items:
            if isinstance(item, GiftCard):
                amt = 0 - Database.CP.GiftCard.get_balance(item.number)
                doc_id = self.doc_id
                tkt_no = self.cp_tkt_no_full
                activity = 'R'  # Redemption
                Database.CP.GiftCard.insert_activity(
                    tkt_no=tkt_no, card_no=item.number, amount=amt, doc_id=doc_id, activity=activity
                )
                Database.CP.GiftCard.update_balance(item.number, 0)

    def refund_payments(self):
        order_db = Database.CP.OpenOrder
        payments = self.payload['PS_DOC_HDR']['PS_DOC_PMT']

        shopify_payment = 0
        gc_payments: bool = False
        loyalty_payment = 0

        for payment in payments:
            if payment['PAY_COD'] == SHOPIFY_PAYCODE:
                shopify_payment = abs(float(payment['AMT']))
            elif payment['PAY_COD'] == LOYALTY_PAYCODE:
                loyalty_payment = abs(float(payment['AMT']))
            elif payment['PAY_COD'] == GIFT_CARD_PAYCODE:
                gc_payments = True

        if shopify_payment:
            shop_refund = -shopify_payment
            order_db.update_payment_amount(self.doc_id, shop_refund, SHOPIFY_PAYCODE)
            order_db.update_payment_apply(self.doc_id, shop_refund, SHOPIFY_PAYCODE)

        if gc_payments:
            for payment_index, payment in enumerate(payments, start=1):
                if not payment['PAY_COD'] == GIFT_CARD_PAYCODE:
                    continue
                amt_spent = abs(payment['AMT'])
                gc_refund = -amt_spent

                # Sets PS_DOC_PMT to negative
                order_db.update_payment_amount(self.doc_id, gc_refund, pmt_seq_no=payment_index)
                order_db.update_payment_apply(self.doc_id, gc_refund, pmt_seq_no=payment_index)

                # ADD balance back to gift card
                card_no = payment['CARD_NO']
                Database.CP.GiftCard.insert_activity(self.cp_tkt_no_full, card_no, amt_spent, self.doc_id)
                current_balance = Database.CP.GiftCard.get_balance(card_no)
                new_balance = current_balance + amt_spent
                Database.CP.GiftCard.update_balance(card_no, new_balance)

        if loyalty_payment:
            Database.CP.Loyalty.add_points(cust_no=self.cust_no, points=loyalty_payment)
            loy_refund = -loyalty_payment
            order_db.update_payment_amount(self.doc_id, loy_refund, LOYALTY_PAYCODE)
            order_db.update_payment_apply(self.doc_id, loy_refund, LOYALTY_PAYCODE)

    def redeem_loyalty_pmts(self):
        for payment in self.payload['PS_DOC_HDR']['PS_DOC_PMT']:
            if payment['PAY_COD'] == LOYALTY_PAYCODE:
                amount = abs(math.floor(float(payment['AMT'])))
                Database.CP.Loyalty.redeem(amount, self.cust_no)

    def get_total_tender(self) -> float:
        """Returns the total tender amount for an order."""
        total = 0

        for payment in self.payload['PS_DOC_HDR']['PS_DOC_PMT']:
            total += abs(float(payment['AMT']))

        return total

    def update_total_sale_lines(self):
        if self.is_refund:
            refunds: int = 0
            for item in self.order.line_items:
                if item.is_refunded:
                    refunds += item.quantity_refunded
            Database.CP.OpenOrder.set_sale_lines(self.doc_id, 0, refunds)
        else:
            sales = 0
            for item in self.order.line_items:
                if not isinstance(item, GiftCard):
                    sales += item.quantity
            Database.CP.OpenOrder.set_sale_lines(self.doc_id, sales, 0)

    def write_ps_doc_tot(self):
        gfc_amount = 0 if self.is_refund else self.total_gfc_amount

        Database.CP.OpenOrder.insert_hdr_total_entry(
            doc_id=self.doc_id,
            lines=len(self.order.line_items),
            gfc_amt=gfc_amount,
            sub_tot=-self.order.refund_total if self.is_refund else self.order.total,
            tot_ext_cost=-self.order.total_extended_cost if self.is_refund else self.order.total_extended_cost,
            tot_tender=self.total_tender,
            tot=self.order.refund_total if self.is_refund else self.order.total,
            total_hdr_disc=self.total_hdr_disc,
            total_lin_disc=self.order.total_discount,
            tot_hdr_discntbl_amt=self.order.subtotal,
        )

    def set_ps_doc_lin_properties(self):
        order_db = Database.CP.OpenOrder

        if not self.is_refund:
            # Process Sales Lines
            for item in self.order.line_items:
                if isinstance(item, GiftCard):
                    continue

                order_db.update_line(
                    doc_id=self.doc_id,
                    lin_seq_no=item.lin_seq_no,
                    qty_to_rel=item.quantity,
                    prc=item.extended_unit_price,
                    prc_1=item.unit_retail_value,
                    ext_cost=item.extended_cost,
                    ext_prc=item.extended_price,
                    unit_retail_value=item.unit_retail_value,
                    gross_ext_prc=item.extended_price,
                    gross_disp_ext_prc=item.extended_price,
                    calc_prc=item.extended_unit_price,
                    calc_ext_prc=item.extended_price,
                    qty_entd=0,
                    qty_to_leave=0,
                )
        else:
            # Process Refund Lines
            for item in self.order.line_items:
                if not item.is_refunded or isinstance(item, GiftCard):
                    continue
                else:
                    order_db.update_line(
                        doc_id=self.doc_id,
                        lin_seq_no=item.lin_seq_no,
                        qty_entd=0,
                        orig_qty=-item.quantity,
                        qty_to_rel=-item.quantity_refunded,
                        qty_to_leave=0,
                        prc=-item.extended_unit_price,
                        prc_1=-item.unit_retail_value,
                        qty_sold=-item.quantity_refunded,
                        ext_prc=-item.refund_amount,
                        unit_retail_value=-item.unit_retail_value,
                        gross_ext_prc=-item.refund_amount,
                        gross_disp_ext_prc=-item.refund_amount,
                        calc_prc=-item.refund_amount / item.quantity,
                        calc_ext_prc=-item.refund_amount,
                        ext_cost=-item.extended_cost,
                    )

                    order_db.update_line_price(
                        doc_id=self.doc_id,
                        lin_seq_no=item.lin_seq_no,
                        quantity=item.quantity_refunded,
                        unit_price=item.refund_amount / item.quantity_refunded,
                        prc_rul_seq_no=-1,
                        prc_brk_descr='I',
                    )

    def add_shipping_charges(self):
        pass

        # Our implementation uses a dummy shipping line item instead of a shipping charge.

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

    @staticmethod
    def get_store_id(order: ShopifyOrder):
        return 1 if order.channel.lower() == 'pos' else 'WEB'

    @staticmethod
    def get_station_id(order: ShopifyOrder):
        return 'POS' if order.channel.lower() == 'pos' else 'WEB'

    @staticmethod
    def get_drawer_id(order: ShopifyOrder):
        return 'POS' if order.channel.lower() == 'pos' else 1

    @staticmethod
    def get_customer_number(order: ShopifyOrder) -> str:
        if order.channel.lower() == 'pos' and not order.customer:
            # If the order is a POS order and the customer is not provided, the customer is CASH.
            return 'CASH'

        # Email
        if order.email:
            email = order.email
        elif order.billing_address.email:
            email = order.billing_address.email
        elif order.shipping_address.email:
            email = order.shipping_address.email
        else:
            email = None

        # Phone
        if order.billing_address.phone:
            phone = order.billing_address.phone
        elif order.shipping_address.phone:
            phone = order.shipping_address.phone
        else:
            phone = None

        return Database.CP.Customer.lookup_customer(email, phone)

    @staticmethod
    def process_order(order: ShopifyOrder, session: requests.Session = requests.Session(), verbose: bool = False):
        oapi = OrderAPI(order, session, verbose)
        print(oapi.payload)
        raise Exception('OrderAPI.process_order() is not implemented')

        if oapi.order.is_declined:
            oapi.error_handler.add_error_v('Order payment declined')
            raise Exception('Order payment declined')

        try:
            if not oapi.cust_no:
                oapi.create_customer()
            else:
                oapi.update_customer()
        except:
            raise Exception('Customer could not be created/updated')

        try:
            oapi.post_order()

        except Exception as e:
            OrderAPI.cleanup(e, oapi.cust_no)

    @staticmethod
    def cleanup(error: Exception, cust_no: str):
        """Cleans up the order if an error occurs."""
        OrderAPI.error_handler.add_error_v('Order could not be posted', traceback=tb())
        # OrderAPI.error_handler.add_error_v(str(error))

        query = f"""
        SELECT TOP 2 DOC_ID FROM PS_DOC_HDR
        WHERE CUST_NO = '{cust_no}'
        AND TKT_DT > '{datetime.now().strftime("%Y-%m-%d")}'
        ORDER BY LST_MAINT_DT DESC
        """
        OrderAPI.logger.info('Attempting to cleanup order')
        response = Database.query(query)

        if response is not None and len(response) > 0 and len(response) < 3:
            for result in response:
                doc_id = result[0]
                Database.CP.OpenOrder.delete(doc_id)
        else:
            OrderAPI.error_handler.add_error_v('Could not cleanup order')

        raise error

    @staticmethod
    def delete(doc_id: str = None, ticket_no: str = None):
        if not doc_id and not ticket_no:
            OrderAPI.logger.warning('No document ID or ticket number provided')
            return
        if doc_id:
            Database.CP.OpenOrder.delete(doc_id=doc_id)
        elif ticket_no:
            doc_id = Database.CP.OpenOrder.delete(tkt_no=ticket_no)


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

            if self.name.lower() == 'custom':
                sku = 'CUSTOM'

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


if __name__ == '__main__':
    OrderAPI.delete(ticket_no='107437188600167')
