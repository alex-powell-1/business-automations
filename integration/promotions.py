from datetime import datetime
from time import sleep
from setup.webDAV_engine import WebDAVJsonClient
from integration.database import Database
from setup.error_handler import ProcessOutErrorHandler
import requests
from setup import creds
from setup.utilities import convert_to_utc
from integration.catalog import Catalog
from product_tools.products import Product
from setup.utilities import VirtualRateLimiter, set_last_sync


class Promotions:
    dav = WebDAVJsonClient()

    def __init__(self, last_sync=None):
        self.last_sync = last_sync
        self.db = Database.db
        self.error_handler = ProcessOutErrorHandler.error_handler
        self.logger = ProcessOutErrorHandler.logger
        self.promotions = []
        self.update_count = 0
        self.get_promotions()
        self.sale_badges = {}
        self.sale_badge_items = []

    def get_promotions(self):
        # Get list of promotions from IM_PRC_GRP
        response = self.db.query('SELECT GRP_COD FROM IM_PRC_GRP')
        promotions = [x[0] for x in response] if response else []
        if promotions:
            # Get promotion details from IM_PRC_GRP and IM_PRC_GRP_RUL
            for promo in promotions:
                query = f"""
                SELECT TOP 1 GRP.GRP_TYP, GRP.GRP_COD, GRP.GRP_SEQ_NO, GRP.DESCR, GRP.CUST_FILT, GRP.BEG_DAT, 
                GRP.END_DAT, GRP.LST_MAINT_DT, GRP.ENABLED, GRP.MIX_MATCH_COD, MW.ID, MW.BC_ID
                FROM IM_PRC_GRP GRP FULL OUTER JOIN {creds.bc_promo_table} MW ON GRP.GRP_COD = MW.GRP_COD
                WHERE GRP.GRP_COD = '{promo}' and GRP.GRP_TYP = 'P'
                """
                response = self.db.query(query=query)
                promo_data = [x for x in response] if response else []
                if promo_data:
                    for data in promo_data:
                        self.promotions.append(self.Promotion(promo=data))

    def sync(self):
        for promotion in self.promotions:
            if promotion.lst_maint_dt > self.last_sync:
                self.logger.info(f'Promotion {promotion.grp_cod} has been updated. Will process.')
                promotion.get_price_rules()
                promotion.has_bogo_twoofer = promotion.has_bogo()
                promotion.process_promotion()
                # Update BOGO PROMO sale badges in promotion config
                self.process_sale_badges(promotion=promotion)
                self.update_count += 1

        if self.update_count == 0:
            self.logger.info('Promotions Sync: No Promotions to update.')

    @staticmethod
    def bc_get_promotions(id=None):
        if id:
            url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions/{id}'
        else:
            url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions'

        response = requests.get(url, headers=creds.bc_api_headers)
        if response.status_code == 200:
            return response.json()['data']

    def get_promotion_config_data(self):
        """Get Sale Badge Promotions from WebDAV. These are promotion badges that are displayed on the website
        for promotions like Buy 1 Get 1 Free, Buy 2 Get 1 Free, etc."""
        try:
            response = Promotions.dav.get_json_file(creds.promotion_config)
            sale_badges = response[1]['promotions']
            sale_badge_items = response[1]['promotion_products']
            return sale_badges, sale_badge_items
        except Exception as e:
            self.error_handler.add_error_v(error=e, origin='Sale Badge Promotion Retrieval')

    def update_promo_config(self):
        """Updates the promotion config file with the new Sale Badge Promotions and Items."""
        # Step 1: Delete the current promotion config file
        response = Promotions.dav.delete_json_file(creds.promotion_config)
        if response[0]:
            self.logger.log(f'Promotion Config deleted successfully. {response[1]}')

        # Step 2: Create a new promotion config file with the updated Sale Badge Promotions and Items
        response = Promotions.dav.update_json_file(
            file_path=creds.promotion_config,
            json_data={'promotions': self.sale_badges, 'promotion_products': self.sale_badge_items},
        )
        if response[0]:
            self.logger.success(f'Promotion Config created successfully. {response[1]}')

    def process_sale_badges(self, promotion):
        # Process property additions
        for rule in promotion.price_rules:
            if rule.use_bogo_twoofer == 'Y' and rule.req_full_group_for_bogo == 'Y':
                # BOGO Twoofer Promotion FOUND. Create Sale Badge
                promotion.create_promotion_message(rule)
                # Check to see if badge already exists
                if rule.message_key not in self.sale_badges:
                    # Add Sale Badge to Promotion
                    self.sale_badges[rule.message_key] = rule.message
                for item in rule.items:
                    product_id = Catalog.get_product_id_from_sku(item)
                    # Check to see if item already has a badge
                    if product_id and product_id not in [x['product_id'] for x in self.sale_badge_items]:
                        # Add Sale Badge to Item
                        self.sale_badge_items.append({'product_id': product_id, 'promotion': rule.message_key})
                    else:
                        # Update Sale Badge to Item
                        for item in self.sale_badge_items:
                            if item['product_id'] == product_id:
                                item['promotion'] = rule.message_key

        self.update_promo_config()

    class Promotion:
        def __init__(self, promo):
            self.db = Database.db
            self.dav = WebDAVJsonClient()
            self.error_handler = ProcessOutErrorHandler.error_handler
            self.logger = ProcessOutErrorHandler.logger
            self.grp_typ = promo[0]
            self.grp_cod = promo[1]
            self.group_seq_no = promo[2]
            self.descr = promo[3]
            self.cust_filt = promo[4]
            self.beg_dat = promo[5]
            self.end_dat = promo[6]
            self.lst_maint_dt = promo[7]
            self.enabled = True if promo[8] == 'Y' else False
            self.mix_match_code = promo[9]
            self.db_id = promo[10]
            self.bc_id = promo[11]
            self.max_uses = None
            self.price_rules = []

        def __str__(self) -> str:
            result = f'Group Code: {self.grp_cod}\n'
            result += f'Description: {self.descr}\n'
            result += f'Customer Filter: {self.cust_filt}\n'
            result += f'Begin Date: {self.beg_dat}\n'
            result += f'End Date: {self.end_dat}\n'
            result += f'Last Maintenance Date: {self.lst_maint_dt}\n'
            result += f'Enabled: {self.enabled}\n'
            result += f'Mix Match Code: {self.mix_match_code}\n'
            # result += f'Price Rule Count: {self.price_rule_count}\n'
            return result

        def get_price_rules(self):
            self.logger.info(f'Getting Price Rules for {self.grp_cod}')
            query = f"""
            SELECT RUL.GRP_TYP, RUL.GRP_COD, RUL.RUL_SEQ_NO, RUL.DESCR, RUL.CUST_FILT, RUL.ITEM_FILT, 
            RUL.SAL_FILT, RUL.IS_CUSTOM, RUL.USE_BOGO_TWOFER, RUL.REQ_FULL_GRP_FOR_BOGO_TWOFER, MW.ID, MW.BC_ID
            FROM IM_PRC_RUL RUL
            FULL OUTER JOIN SN_PROMO MW on rul.GRP_COD = MW.GRP_COD
            WHERE RUL.GRP_COD = '{self.grp_cod}'
            """
            response = Database.db.query(query)
            self.price_rules = [self.PriceRule(rule) for rule in response] if response else []

        def has_bogo(self):
            for rule in self.price_rules:
                if rule.use_bogo_twoofer == 'Y' and rule.req_full_group_for_bogo == 'Y':
                    return True

        def get_rule_items(self, rul_seq_no, bc=True):
            """Takes in a rule sequence number and returns a list of Associated Item BC Product IDs."""
            items = []
            for rule in self.price_rules:
                if rul_seq_no == rule.rul_seq_no:
                    items = rule.items
            if bc:
                # Get BC Product IDs
                bc_prod_ids = []
                for item in items:
                    bc_prod_id = Catalog.get_product_id_from_sku(item)
                    if bc_prod_id:
                        bc_prod_ids.append(bc_prod_id)
                return bc_prod_ids
            else:
                return items

        def get_price_breaks(self, rul_seq_no):
            """Takes in a rule sequence number and returns a list of Price Breaks."""
            breaks = []
            for rule in self.price_rules:
                if rul_seq_no == rule.rul_seq_no:
                    breaks = rule.price_breaks
            return breaks

        def bc_create_bogo_promotion(self):
            if self.has_bogo_twoofer:
                payload = self.create_payload()
                url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions'
                response = requests.post(url, headers=creds.bc_api_headers, json=payload)
                if response.status_code == 201:
                    self.logger.success(f'Promotion {self.grp_cod} created successfully.')
                    self.bc_id = response.json()['data']['id']
                    return True

                elif response.status_code == 429:
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    sleep(seconds_to_wait)
                    response = requests.post(url, headers=creds.bc_api_headers, json=payload)
                    if response.status_code == 201:
                        self.logger.success(f'Promotion {self.grp_cod} created successfully.')
                        self.bc_id = response.json()['data']['id']
                        return True
                    else:
                        self.error_handler.add_error_v(
                            error=f'Error: {response.status_code}\n' f'{response.text}', origin='Promotion creation'
                        )
                        return False
                else:
                    self.error_handler.add_error_v(
                        error=f'Error: {response.status_code}\n' f'{response.text}', origin='Promotion Creation'
                    )
                    return False

        def bc_update_bogo_promotion(self):
            payload = self.create_payload()
            url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions/{self.bc_id}'
            response = requests.put(url, headers=creds.bc_api_headers, json=payload)
            if response.status_code == 200:
                self.logger.success(f'Promotion {self.grp_cod} updated successfully.')
                return True

            elif response.status_code == 429:
                ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                seconds_to_wait = (ms_to_wait / 1000) + 1
                VirtualRateLimiter.pause_requests(seconds_to_wait)
                sleep(seconds_to_wait)
                response = requests.put(url, headers=creds.bc_api_headers, json=payload)
                if response.status_code in [200]:
                    self.logger.success(f'Promotion {self.grp_cod} updated successfully.')
                    return True
                else:
                    self.error_handler.add_error_v(
                        error=f'Error: {response.status_code}\n' f'{response.text}', origin='Promotion Update'
                    )
                    return False
            else:
                self.error_handler.add_error_v(
                    error=f'Error: {response.status_code}\n' f'{response.text}', origin='Promotion Update'
                )
                return False

        def bc_delete_bogo_promotion(self):
            url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions/{self.bc_id}'
            response = requests.delete(url, headers=creds.bc_api_headers)
            if response.status_code == 204:
                self.logger.success(f'Promotion {self.grp_cod} deleted successfully.')
                return True
            else:
                self.error_handler.add_error_v(
                    error=f'Error: {response.status_code}\n' f'{response.text}', origin='Promotion Deletion'
                )
                return False

        def get_discount_amount(self, price_rule):
            """Get the discount amount of the final price break."""
            return int(price_rule.price_breaks[-1].amt_or_pct)

        def get_promotion_rules(self):
            """Creates the line item rules payload for the promotion API"""
            result = []
            for rule in self.price_rules:
                if rule.use_bogo_twoofer == 'Y' and rule.req_full_group_for_bogo == 'Y':
                    items = self.get_rule_items(rule.rul_seq_no)

                    required_qty, reward_qty = self.get_reward_quantity(rule)

                    rule_payload = {
                        'action': {
                            'cart_items': {
                                'discount': {'percentage_amount': self.get_discount_amount(rule)},
                                'strategy': 'LEAST_EXPENSIVE',
                                'add_free_item': False,
                                'as_total': False,
                                'include_items_considered_by_condition': False,
                                'exclude_items_on_sale': False,
                                'items': {'products': items},
                                'quantity': reward_qty,
                            }
                        },
                        'apply_once': False,
                        'stop': False,
                        # Condition is the rule that must be met for the discount to be applied
                        'condition': {'cart': {'items': {'products': items}, 'minimum_quantity': required_qty}},
                    }

                    result.append(rule_payload)
            return result

        def get_reward_quantity(self, rule):
            """Calculates the number of reward products given to a customer after a condition is met.
            Example: Buy 1 Get 1 Free = 1 reward product, Buy 2 Get 1 Free = 1 reward product, etc.
            """
            first_rule_qty = int(rule.price_breaks[0].min_qty)
            second_rule_qty = int(rule.price_breaks[1].min_qty)
            reward = second_rule_qty - first_rule_qty
            return first_rule_qty, reward

        def create_payload(self):
            """Creates the payload for the BigCommerce API Promotion."""
            payload = {
                'name': self.descr,
                'rules': self.get_promotion_rules(),
                'notifications': [],
                'stop': False,
                'currency_code': 'USD',
                'redemption_type': 'AUTOMATIC',
                'status': 'ENABLED' if self.enabled else 'DISABLED',
                'can_be_used_with_other_promotions': True,
            }

            if self.max_uses:
                payload['max_uses'] = self.max_uses

            if self.beg_dat:
                payload['start_date'] = convert_to_utc(self.beg_dat)
            if self.end_dat:
                payload['end_date'] = convert_to_utc(self.end_dat)

            return payload

        def process_promotion(self):
            if self.enabled:
                if self.has_bogo_twoofer:
                    # process BOGO Twoofers
                    if self.db_id:
                        # Update Promotion
                        if self.bc_update_bogo_promotion():
                            if self.mw_update_bogo_promotion():
                                self.logger.success(f'Promotion {self.grp_cod} processed successfully.')
                    else:
                        # Create Promotion
                        if self.bc_create_bogo_promotion():
                            if self.mw_insert_bogo_promotion():
                                self.logger.success(f'BOGO Promotions for {self.grp_cod} processed successfully.')

                # Process Regular Fixed Price Promotions
                self.add_sale_price()
            else:
                if self.has_bogo_twoofer:
                    # Check if Promotion exists in MW
                    if self.db_id is not None:
                        # Delete Promotion
                        if self.bc_delete_bogo_promotion():
                            if self.mw_delete_bogo_promotion():
                                self.logger.success(f'BOGO Promotions for {self.grp_cod} processed successfully.')
                # Process Regular Fixed Price Promotions
                self.remove_sale_price()

        def add_sale_price(self):
            """Updates the sale price for non-bogo items in the promotion."""

            def get_target_price(item, target_method, target_amount):
                if target_method == 'P':
                    # pick price. Do nothing
                    pass
                if target_method == 'D':
                    # Percentage Discount
                    self.logger.log(f'Item: {item.item_no} Price 1: {item.price_1} Target Amount: {target_amount}')
                    item_sale_price = round(float(item.price_1 * ((100 - target_amount) / 100)), 2)
                elif target_method == 'F':
                    # Fixed Price Adjustment
                    item_sale_price = target_amount
                elif target_method == 'A':
                    # Amount Discount
                    item_sale_price = round(float(item.price_1) - target_amount, 2)

                return item_sale_price

            for rule in self.price_rules:
                print(rule.descr)
                print(rule.use_bogo_twoofer)
                if rule.use_bogo_twoofer == 'N':
                    self.logger.info(f'GRP_COD: {self.grp_cod}: Processing Rule: {rule.rul_seq_no}')
                    # Get CP SKU list of items for the rule.
                    items = self.get_rule_items(rule.rul_seq_no, bc=False)
                    print(items)
                    target = rule.price_breaks[0]
                    target_method = target.prc_meth
                    target_amount = target.amt_or_pct

                    if items:
                        new_timestamp = f'{datetime.now():%Y-%m-%d %H:%M:%S}'
                        counter = 1
                        for i in items:
                            print(counter, '/', len(items))
                            # Create Product Object
                            item = Product(i)
                            current_sale_price = round(float(item.price_2), 2)
                            target_sale_price = get_target_price(item, target_method, target_amount)
                            print(
                                f'CURRENT SALE TYPE: {type(current_sale_price)}, OBJECT PRICE: {current_sale_price}'
                            )
                            print(f'TARGET SALE TYPE: {type(target_sale_price)}, Sale_price: {target_sale_price}')
                            if current_sale_price != target_sale_price:
                                print('NOT THE SAME')
                                query = f"""
                                UPDATE IM_PRC
                                SET PRC_2 = {target_sale_price}, LST_MAINT_DT = '{new_timestamp}'
                                WHERE ITEM_NO = '{i}'				
                                
                                UPDATE IM_ITEM
                                SET LST_MAINT_DT = '{new_timestamp}'
                                WHERE ITEM_NO = '{i}'
                                
                                INSERT INTO EC_CATEG_ITEM(ITEM_NO, CATEG_ID, ENTRY_SEQ_NO, LST_MAINT_DT, LST_MAINT_USR_ID)
                                VALUES('{i}', '{creds.on_sale_category}', '{counter}', '{new_timestamp}', 'POS')
                                """
                                # Updating Sale Price, Last Maintenance Date, and Adding to On Sale Category
                                response = self.db.query(query, commit=True)
                                if response['code'] == 200:
                                    self.logger.success(
                                        f'Item: {i} Price 1: {item.price_1} adjusted to Sale Price: {target_sale_price}'
                                    )
                                # return completed timestamp for use in updating last_sync_time
                                else:
                                    self.error_handler.add_error_v(
                                        error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Addition")'
                                    )
                            counter += 1
                    else:
                        self.error_handler.add_error_v(
                            error=f'Error: No Items Found for Rule {rule.rul_seq_no}', origin='Sale Price Addition'
                        )

        def remove_sale_price(self):
            for rule in self.price_rules:
                if rule.use_bogo_twoofer == 'N':
                    # Fixed Price Promotion
                    items = self.get_rule_items(rule.rul_seq_no, bc=False)

                    if items:
                        new_timestamp = f'{datetime.now():%Y-%m-%d %H:%M:%S}'
                        if len(items) > 1:
                            where_filter = f'WHERE ITEM_NO IN {tuple(items)}'
                        else:
                            where_filter = f"WHERE ITEM_NO = '{items[0]}'"

                        query = f"""
                        UPDATE IM_PRC
                        SET PRC_2 = NULL, LST_MAINT_DT = '{new_timestamp}'
                        {where_filter}
                
                        UPDATE IM_ITEM
                        SET LST_MAINT_DT = '{new_timestamp}'
                        {where_filter}

                        DELETE FROM EC_CATEG_ITEM
                        {where_filter} AND CATEG_ID = '{creds.on_sale_category}'"""
                        # Removing Sale Price, Last Maintenance Date, and Removing from On Sale Category
                        response = self.db.query(query, commit=True)

                        if response['code'] == 200:
                            self.logger.success(f'Sale Price {self.grp_cod} removed successfully from {items}.')
                            return new_timestamp
                        else:
                            self.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Removal")'
                            )

        def mw_insert_bogo_promotion(self):
            query = f"""
            INSERT INTO SN_PROMO(GRP_COD, BC_ID, ENABLED)
            VALUES('{self.grp_cod}', {self.bc_id}, {1 if self.enabled else 0})
            """
            response = self.db.query(query, commit=True)
            if response['code'] == 200:
                self.logger.success(f'Promotion {self.grp_cod} inserted successfully.')
                return True
            else:
                self.error_handler.add_error_v(
                    error=f'Error: {response["code"]}\n' f'{response["message"]}', origin='Promotion Insertion'
                )
                return False

        def mw_update_bogo_promotion(self):
            query = f"""
            UPDATE SN_PROMO
            SET BC_ID = {self.bc_id}, ENABLED = {1 if self.enabled else 0}, LST_MAINT_DT = GETDATE()
            WHERE GRP_COD = '{self.grp_cod}'
            """
            response = self.db.query(query, commit=True)
            if response['code'] == 200:
                self.logger.success(f'Promotion {self.grp_cod} updated successfully.')
                return True
            else:
                self.error_handler.add_error_v(
                    error=f'Error: {response["code"]}\n' f'{response["message"]}', origin='Promotion Update'
                )
                return False

        def mw_delete_bogo_promotion(self):
            query = f"""
            DELETE FROM SN_PROMO
            WHERE BC_ID = {self.bc_id}
            """
            response = self.db.query(query, commit=True)
            if response['code'] == 200:
                self.logger.success(f'Promotion {self.grp_cod} deleted successfully.')
                return True
            else:
                self.error_handler.add_error_v(
                    error=f'Error: {response["code"]}\n' f'{response["message"]}', origin='Promotion Deletion'
                )
                return False

        def create_promotion_message(self, rule):
            required_qty, reward_qty = self.get_reward_quantity(rule)
            if required_qty and reward_qty:
                message = f'Buy {required_qty}, Get {reward_qty}'
                amount = self.get_discount_amount(rule)
                price_method = rule.price_breaks[-1].prc_meth
                if price_method == 'D':
                    if amount < 100:
                        message += f' {amount}% Off'
                    elif amount == 100:
                        message += ' FREE'
                elif rule.price_breaks[-1].prc_meth == 'A':
                    message += f' ${amount} Off!'
                # add message to rule
                promo_key = (
                    ''.join([x[0] for x in message.replace('$', '').replace('%', '').split(' ')]).upper()
                    + '-'
                    + price_method
                )
                rule.message = message
                rule.message_key = promo_key

        class PriceRule:
            def __init__(self, rule):
                self.error_handler = ProcessOutErrorHandler.error_handler
                self.logger = ProcessOutErrorHandler.logger
                self.grp_typ = rule[0]
                self.grp_cod = rule[1]
                self.rul_seq_no = rule[2]
                self.descr = rule[3]
                self.cust_filt = rule[4]
                self.item_filt = rule[5]
                self.sal_filt = rule[6]
                self.is_custom = rule[7]
                self.use_bogo_twoofer = rule[8]
                self.req_full_group_for_bogo = rule[9]
                self.db_id = rule[10]
                self.bc_id = rule[11]
                self.price_breaks = []
                self.items = []
                self.get_price_breaks()
                self.get_items()

            def __str__(self) -> str:
                result = f'Rule Sequence Number: {self.rul_seq_no}\n'
                result += f'Group Code: {self.grp_cod}\n'
                result += f'db_id: {self.db_id}\n'
                result += f'BC ID: {self.bc_id}\n'
                result += f'Description: {self.descr}\n'
                result += f'Customer Filter: {self.cust_filt}\n'
                result += f'Item Filter: {self.item_filt}\n'
                result += f'Sale Filter: {self.sal_filt}\n'
                result += f'Is Custom: {self.is_custom}\n'
                result += f'Use BOGO Twoofer: {self.use_bogo_twoofer}\n'
                result += f'Require Full Group for BOGO Twoofer: {self.req_full_group_for_bogo}\n'
                return result

            def isBogoTwoofer(self, price_rule):
                return price_rule.use_bogo_twoofer == 'Y' and price_rule.req_full_group_for_bogo == 'Y'

            def get_price_breaks(self):
                query = f"""
                        SELECT MIN_QTY, PRC_METH, PRC_BASIS, AMT_OR_PCT
                        FROM IM_PRC_RUL_BRK
                        WHERE GRP_COD = '{self.grp_cod}' AND RUL_SEQ_NO = {self.rul_seq_no}
                        """
                response = Database.db.query(query)
                if response:
                    for break_data in response:
                        self.price_breaks.append(self.PriceBreak(break_data))

            def get_items(self):
                if self.item_filt:
                    where_filter = f'WHERE {self.item_filt}'
                else:
                    where_filter = ''
                query = f'SELECT ITEM_NO FROM IM_ITEM {where_filter}'
                response = Database.db.query(query)
                if response:
                    for item in response:
                        item_no = item[0]
                        self.items.append(item_no)

            class PriceBreak:
                def __init__(self, break_data):
                    self.min_qty = break_data[0]
                    self.prc_meth = break_data[1]
                    self.prc_basis = break_data[2]
                    self.amt_or_pct = break_data[3]


if __name__ == '__main__':
    promo = Promotions(last_sync=datetime(2024, 7, 15))
    promo.sync()
