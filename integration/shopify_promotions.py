from datetime import datetime
from integration.database import Database
from setup.error_handler import ProcessOutErrorHandler
from setup import creds
from setup.utilities import convert_to_utc
from integration.catalog import Catalog
from product_tools.products import Product
from integration.shopify_api import Shopify
from traceback import format_exc as tb
import json


class Promotions:
    def __init__(self, last_sync=None):
        self.last_sync = last_sync
        self.db = Database.db
        self.error_handler = ProcessOutErrorHandler.error_handler
        self.logger = ProcessOutErrorHandler.logger
        self.promotions = []
        self.sync_queue = []
        self.update_count = 0
        self.sale_badges = {}
        self.sale_badge_items = []
        self.get_promotions()

    def get_promotions(self):
        promo_data = Database.Counterpoint.Promotion.get()
        if promo_data:
            for promo in promo_data:
                self.promotions.append(self.Promotion(promo=promo))

    def process_deletes(self):
        # List of Group Codes from Counterpoint
        cp_promotions = [x.grp_cod for x in self.promotions]
        # List of Group Codes from Middleware
        mw_promotions = Database.Shopify.Discount.get()

        if mw_promotions:
            delete_count = 0
            for mw_promotion in mw_promotions:
                if mw_promotion not in cp_promotions:
                    # Promotion has been deleted in Counterpoint. Delete in Shopify and Middleware
                    shopify_id = Database.Shopify.Discount.get_id(mw_promotion)
                    Shopify.Discount.Automatic.delete(shopify_id)
                    Database.Shopify.Discount.delete(shopify_id)
                    delete_count += 1

            if delete_count == 0:
                self.logger.info('Promotions Sync: No Promotions to delete.')

    def get_sync_queue(self):
        for promotion in self.promotions:
            if promotion.lst_maint_dt > self.last_sync:
                self.sync_queue.append(promotion)

    def sync(self):
        self.process_deletes()
        self.get_sync_queue()

        if not self.sync_queue:
            self.logger.info('Promotions Sync: No Promotions to update.')
        else:
            success_count = 0
            fail_count = 0
            fail_group_codes = []

            for promotion in self.sync_queue:
                try:
                    promotion.process()
                except Exception as e:
                    self.error_handler.add_error_v(error=e, origin='Promotion Sync', traceback=tb())
                    fail_count += 1
                    fail_group_codes.append(promotion.grp_cod)
                else:
                    success_count += 1

            self.logger.info(f'Promotions Sync: {success_count} Promotions updated successfully.')
            if fail_count > 0:
                self.logger.warn(
                    f'Promotions Sync: {fail_count} Promotions failed to update. \n\nGroup Codes: {fail_group_codes}'
                )

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
            self.shopify_id = promo[11]
            self.max_uses = None
            self.price_rules = []
            self.get_price_rules()

        def __str__(self) -> str:
            result = '-----------------------------------\n'
            result += f'PROMOTION: {self.grp_cod}\n'
            result += f'Group Code: {self.grp_cod}\n'
            result += f'Description: {self.descr}\n'
            result += f'Customer Filter: {self.cust_filt}\n'
            result += f'Begin Date: {self.beg_dat}\n'
            result += f'End Date: {self.end_dat}\n'
            result += f'Last Maintenance Date: {self.lst_maint_dt}\n'
            result += f'Enabled: {self.enabled}\n'
            result += f'Mix Match Code: {self.mix_match_code}\n'
            result += '-----------------------------------\n'
            counter = 1
            for rule in self.price_rules:
                result += f'\tRule {counter}:\n'
                result += f'{rule}'
                result += '\t-----------------------------------\n'
                counter += 1
                break_counter = 1
                for price_break in rule.price_breaks:
                    result += f'\t\tPrice Break {break_counter}:\n'
                    result += f'{price_break}'
                    result += '\t\t-----------------------------------\n'
                    break_counter += 1
            result += '\n'

            return result

        def get_price_rules(self):
            rules = Database.Counterpoint.Promotion.PriceRule.get(self.grp_cod)
            if rules:
                for rule in rules:
                    self.price_rules.append(self.PriceRule(rule))

        def has_bogo(self) -> bool:
            """Checks all the price rules for a BOGO Twoofer promotion."""
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

        def get_bxgy_payload(self, rule):
            """Creates the payload for the BOGO Twoofer promotion."""
            required_qty, reward_qty = rule.get_reward_quantity()
            discount_amount = rule.get_discount_amount() / 100

            payload = {
                'automaticBxgyDiscount': {
                    'title': rule.descr,  # 'Buy 1 Get 1 50% Off'
                    'combinesWith': {'orderDiscounts': True, 'productDiscounts': True, 'shippingDiscounts': True},
                    'customerBuys': {
                        'value': {'quantity': str(required_qty)},
                        'items': {
                            'products': {
                                'productsToAdd': [
                                    f'gid://shopify/Product/{Database.Shopify.Product.get_id(item_no=x)}'
                                    for x in rule.items
                                ]
                            }
                        },
                    },
                    'customerGets': {
                        'value': {
                            'discountOnQuantity': {
                                'quantity': json.dumps(reward_qty),
                                'effect': {'percentage': discount_amount},
                            }
                        },
                        'items': {
                            'products': {
                                'productsToAdd': [
                                    f'gid://shopify/Product/{Database.Shopify.Product.get_id(item_no=x)}'
                                    for x in rule.items
                                ]
                            }
                        },
                    },
                }
            }

            if self.beg_dat:
                payload['automaticBxgyDiscount']['startsAt'] = convert_to_utc(self.beg_dat)

            if self.end_dat:
                payload['automaticBxgyDiscount']['endsAt'] = convert_to_utc(self.end_dat)

            return payload

        def process(self):
            for rule in self.price_rules:
                if rule.is_bogo_twoofer:
                    # process BOGO Twoofers
                    variables = self.get_bxgy_payload(rule)
                    if self.db_id:
                        # Update Promotion
                        Shopify.Discount.Automatic.Bxgy.update(variables)
                        if self.enabled:
                            Shopify.Discount.Automatic.activate(self.shopify_id)
                        else:
                            Shopify.Discount.Automatic.deactivate(self.shopify_id)
                    else:
                        # Create Promotion -
                        self.shopify_id = Shopify.Discount.Automatic.Bxgy.create(variables)
                        if not self.enabled:
                            Shopify.Discount.Automatic.deactivate(self.shopify_id)

                    # Sync BOGO Twoofer Promotions to Middleware.
                    # Fixed Price Promotions are processed outside this block.
                    Database.Shopify.Discount.sync(rule)

                # Process Non-BOGO Twoofer Promotions
                if self.enabled:
                    self.add_sale_price()
                else:
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
                                SET IS_ON_SALE = 'Y', SALE_DESCR = '{rule.badge_text}', LST_MAINT_DT = '{new_timestamp}'
                                WHERE ITEM_NO = '{i}'
                                """
                                # Updating Sale Price, Last Maintenance Date, and Adding to On Sale Category
                                response = self.db.query(query)
                                if response['code'] == 200:
                                    self.logger.success(
                                        f'Item: {i} Price 1: {item.price_1} adjusted to Sale Price: {target_sale_price}'
                                    )
                                elif response['code'] == 201:
                                    self.logger.warn(f'No Rows Affected for Item: {i}')
                                # return completed timestamp for use in updating last_sync_time
                                else:
                                    self.error_handler.add_error_v(
                                        error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Addition")'
                                    )
                            counter += 1
                    else:
                        self.error_handler.add_error_v(
                            error=f'Error: No Items Found for GRP_COD: {self.grp_cod} Rule {rule.rul_seq_no}',
                            origin='Sale Price Addition',
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
                        SET IS_ON_SALE = 'N', SALE_DESCR = NULL, LST_MAINT_DT = '{new_timestamp}'
                        {where_filter}
                        """
                        # Removing Sale Price, Last Maintenance Date, and Removing from On Sale Category
                        response = self.db.query(query)

                        if response['code'] == 200:
                            self.logger.success(f'Sale Price {self.grp_cod} removed successfully from {items}.')
                            return new_timestamp
                        elif response['code'] == 201:
                            self.logger.warn(f'No Rows Affected for {items}')
                            return new_timestamp
                        else:
                            self.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Removal")'
                            )

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
                self.shopify_id = rule[11]
                self.price_breaks = []
                self.items = []
                self.get_price_breaks()
                self.get_items()
                self.badge_text = self.get_badge_text()

            def __str__(self) -> str:
                result = f'\tRule Sequence Number: {self.rul_seq_no}\n'
                result += f'\tGroup Code: {self.grp_cod}\n'
                result += f'\tdb_id: {self.db_id}\n'
                result += f'\tShopify ID: {self.shopify_id}\n'
                result += f'\tDescription: {self.descr}\n'
                result += f'\tCustomer Filter: {self.cust_filt}\n'
                result += f'\tItem Filter: {self.item_filt}\n'
                result += f'\tSale Filter: {self.sal_filt}\n'
                result += f'\tIs Custom: {self.is_custom}\n'
                result += f'\tUse BOGO Twoofer: {self.use_bogo_twoofer}\n'
                result += f'\tRequire Full Group for BOGO Twoofer: {self.req_full_group_for_bogo}\n'
                result += f'\tBadge Text: {self.badge_text}\n'
                return result

            def is_bogo_twoofer(self) -> bool:
                return self.use_bogo_twoofer == 'Y' and self.req_full_group_for_bogo == 'Y'

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

            def get_reward_quantity(self):
                """Calculates the number of reward products given to a customer after a condition is met.
                Example: Buy 1 Get 1 Free = 1 reward product, Buy 2 Get 1 Free = 1 reward product, etc.
                """
                first_rule_qty = int(self.price_breaks[0].min_qty)
                second_rule_qty = int(self.price_breaks[1].min_qty)
                reward = second_rule_qty - first_rule_qty
                return first_rule_qty, reward

            def get_discount_amount(self, fixed_price=False):
                """Get the discount amount of the final price break."""
                if fixed_price:
                    retail_price = None
                    response = Database.db.query(f"SELECT PRC_1 FROM IM_ITEM WHERE ITEM_NO = '{self.items[0]}'")
                    retail_price = response[0][0] if response else None
                    if retail_price:
                        if self.price_breaks[-1].amt_or_pct:
                            fixed_price = self.price_breaks[-1].amt_or_pct
                            import math

                            discount_amount = math.floor(100 - (fixed_price * 100 / retail_price))
                            return discount_amount
                else:
                    return int(self.price_breaks[-1].amt_or_pct)

            def get_badge_text(self):
                # Create sale description to be used on items in catalog view and search view
                if self.is_bogo_twoofer():
                    required_qty, reward_qty = self.get_reward_quantity()
                    if required_qty and reward_qty:
                        message = f'Buy {required_qty}, Get {reward_qty}'
                        amount = self.get_discount_amount()
                        price_method = self.price_breaks[-1].prc_meth
                        if price_method == 'D':
                            if amount < 100:
                                message += f' {amount}% OFF'
                            elif amount == 100:
                                message += ' FREE'
                        elif self.price_breaks[-1].prc_meth == 'A':
                            message += f' ${amount} OFF!'

                elif self.req_full_group_for_bogo == 'Y':
                    if self.price_breaks[-1].prc_meth == 'F':
                        try:
                            min_qty = int(self.price_breaks[-1].min_qty)
                            unit_prc = self.price_breaks[-1].amt_or_pct
                        except:
                            message = 'SALE'
                        else:
                            message = f'{min_qty} FOR ${round(min_qty * unit_prc, 2)}'

                else:
                    price_method = self.price_breaks[-1].prc_meth
                    if price_method == 'D':
                        amount = self.get_discount_amount()
                        if amount < 100:
                            message = f'{amount}% OFF'
                        elif amount == 100:
                            message = 'FREE'

                    elif price_method == 'A':
                        amount = self.get_discount_amount()
                        message = f'${amount} OFF'

                    elif price_method == 'F' and len(self.items) == 1:
                        # Fixed Price
                        # If there is one item affected by this rule, then create a custom badge for that item.
                        # Usually this is the case for a fixed price promotion. If there are multiple items, then
                        # the badge will be the same for all items (else clause).
                        amount = self.get_discount_amount(fixed_price=True)
                        message = f'{amount}% OFF'

                    else:  #'P' pick price
                        message = 'SALE'

                return message

            class PriceBreak:
                def __init__(self, break_data):
                    try:
                        self.min_qty = int(break_data[0])
                    except:
                        self.min_qty = 1
                    self.prc_meth = break_data[1]
                    self.prc_basis = break_data[2]
                    self.amt_or_pct = break_data[3]

                def __str__(self) -> str:
                    result = f'\t\tMin Qty: {self.min_qty}\n'
                    result += f'\t\tPrice Method: {self.prc_meth}\n'
                    result += f'\t\tPrice Basis: {self.prc_basis}\n'
                    result += f'\t\tAmount or Percentage: {self.amt_or_pct}\n'
                    return result


if __name__ == '__main__':
    promo = Promotions(last_sync=datetime(2024, 7, 15))
    for p in promo.promotions:
        if p.grp_cod == 'TEST':
            p.process()
            break
