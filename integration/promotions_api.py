from datetime import datetime
from database import Database
from setup.error_handler import ProcessOutErrorHandler
from setup.utilities import convert_to_utc
from product_tools.products import Product
from integration.shopify_api import Shopify
from traceback import format_exc as tb
import json


class Promotions:
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, last_sync=None, verbose=False, enabled=True):
        self.last_sync = last_sync
        self.verbose = verbose
        self.promotions: list[Promotions.Promotion] = []
        self.sync_queue: list[Promotions.Promotion] = []
        self.update_count = 0
        self.sale_badges = {}
        self.sale_badge_items = []
        self.get_promotions()
        if enabled:
            self.get_sync_queue()

    def __str__(self) -> str:
        result = ''
        if self.sync_queue:
            result = f'Promotions to Process: {len(self.sync_queue)}\n'
        return result

    def get_promotions(self):
        promo_data = Database.Counterpoint.Promotion.get()
        if promo_data:
            for promo in promo_data:
                try:
                    self.promotions.append(self.Promotion(promo=promo))
                except Exception as e:
                    Promotions.error_handler.add_error_v(error=e, origin='Promotion Sync', traceback=tb())

    def process_deletes(self):
        cp_promotions = [x.grp_cod for x in self.promotions]
        mw_promotions = Database.Shopify.Promotion.get()
        if mw_promotions:
            delete_count = 0
            for mw_promotion in mw_promotions:
                if mw_promotion not in cp_promotions:
                    shopify_id = Database.Shopify.Promotion.get(mw_promotion)
                    Promotions.Promotion.delete(shopify_discount_code_id=shopify_id)
                    delete_count += 1

            if delete_count == 0 and self.verbose:
                Promotions.logger.info('PROMOTIONS: No Promotions to delete.')

    def get_sync_queue(self):
        for promotion in self.promotions:
            if promotion.lst_maint_dt > self.last_sync:
                self.sync_queue.append(promotion)

    def sync(self):
        self.process_deletes()

        if not self.sync_queue:
            Promotions.logger.info('PROMOTIONS: No Promotions to update.')
        else:
            success_count = 0
            fail_count = 0
            fail_group_codes = []

            for promotion in self.sync_queue:
                try:
                    promotion.process()
                except Exception as e:
                    Promotions.error_handler.add_error_v(error=e, origin='Promotion Sync', traceback=tb())
                    fail_count += 1
                    fail_group_codes.append(promotion.grp_cod)
                else:
                    success_count += 1
            if success_count > 0:
                Promotions.logger.info(f'PROMOTIONS: {success_count} Promotions updated successfully.')
            if fail_count > 0:
                Promotions.logger.warn(
                    f'PROMOTIONS: {fail_count} Promotions failed to update. \n\nGroup Codes: {fail_group_codes}'
                )
            Promotions.logger.info('PROMOTIONS: Sync Complete')

    class Promotion:
        """Promotion Class for handling Counterpoint Promotions.
        Promotions are given a group code and can have multiple price rules. Each price rule can be a fixed price
        adjustment or a BOGO Twoofer promotion with multiple price breaks. Each price rule can have multiple items."""

        def __init__(self, promo):
            self.grp_typ = promo[0]
            self.grp_cod = promo[1]
            self.group_seq_no = promo[2]
            self.descr = promo[3]
            self.cust_filt = promo[4]
            self.no_beg_date = True if promo[5] == 'Y' else False
            self.beg_date = promo[6]
            self.beg_time_flag = int(promo[7])
            self.no_end_date = True if promo[8] == 'Y' else False
            self.end_dat = promo[9]
            self.end_time_flag = int(promo[10])
            self.lst_maint_dt = promo[11]
            self.is_enabled = True if promo[12] == 'Y' else False
            self.mix_match_code = promo[13]
            self.shopify_id = promo[14]
            self.max_uses = None
            self.price_rules: list[Promotions.Promotion.PriceRule] = []
            self.get_start_end_dates()
            self.get_price_rules()

        def __str__(self) -> str:
            result = '-----------------------------------\n'
            result += f'PROMOTION: {self.grp_cod}\n'
            result += f'Group Code: {self.grp_cod}\n'
            result += f'Description: {self.descr}\n'
            result += f'Customer Filter: {self.cust_filt}\n'
            result += f'Begin Date: {self.beg_date}\n'
            result += f'End Date: {self.end_dat}\n'
            result += f'Last Maintenance Date: {self.lst_maint_dt}\n'
            result += f'Enabled: {self.is_enabled}\n'
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

        def get_start_end_dates(self):
            """Converts the start and end dates to UTC time."""
            if self.beg_date:
                if self.beg_time_flag != 0:  # 0 represents the beginning of the day, ie. 12:00:00 AM.
                    self.beg_date = self.beg_date.replace(hour=self.beg_time_flag)
                self.beg_date = convert_to_utc(self.beg_date)
            if self.end_dat:
                if self.end_time_flag == 0:
                    # 0 Represents the end of the day, ie. 11:59:59 PM.
                    self.end_dat = self.end_dat.replace(hour=23, minute=59, second=59)
                else:
                    self.end_dat = self.end_dat.replace(hour=self.end_time_flag)
                self.end_dat = convert_to_utc(self.end_dat)

        def get_price_rules(self):
            rules = Database.Counterpoint.Promotion.PriceRule.get(self.grp_cod)
            if rules:
                for rule in rules:
                    self.price_rules.append(Promotions.Promotion.PriceRule(rule))

        def get_bxgy_payload(self, rule: 'Promotions.Promotion.PriceRule') -> dict:
            """Creates the payload for the BOGO Twoofer promotion."""
            required_qty, reward_qty = rule.get_reward_quantity()

            if rule.price_breaks[-1].prc_meth == 'D':
                discount_amount = int(rule.get_discount_amount()) / 100
                effect = {'percentage': discount_amount}

            elif rule.price_breaks[-1].prc_meth == 'A':
                discount_amount = float(rule.get_discount_amount())
                effect = {'amount': discount_amount}

            payload = {
                'automaticBxgyDiscount': {
                    'title': rule.shopify_title,  # 'Buy 1 Get 1 50% Off'
                    'combinesWith': {'orderDiscounts': True, 'productDiscounts': True, 'shippingDiscounts': True},
                    'customerBuys': {
                        'value': {'quantity': str(required_qty)},
                        'items': {
                            'products': {
                                'productsToAdd': [],
                                'productsToRemove': [],
                                'productVariantsToAdd': [],
                                'productVariantsToRemove': [],
                            }
                        },
                    },
                    'customerGets': {
                        'value': {'discountOnQuantity': {'quantity': json.dumps(reward_qty), 'effect': effect}},
                        'items': {
                            'products': {
                                'productsToAdd': [],
                                'productsToRemove': [],
                                'productVariantsToAdd': [],
                                'productVariantsToRemove': [],
                            }
                        },
                    },
                }
            }

            if rule.shopify_id:  # For updating existing promotions
                payload['id'] = f'{Shopify.Discount.Automatic.prefix}{rule.shopify_id}'

            if rule.items:
                for i in rule.items:
                    shopify_id = Database.Shopify.Product.get_id(item_no=i)
                    variant_id = Database.Shopify.Product.Variant.get_id(sku=i)
                    if shopify_id:
                        if variant_id:
                            # Individual Variants for Bound Products
                            payload['automaticBxgyDiscount']['customerBuys']['items']['products'][
                                'productVariantsToAdd'
                            ].append(f'gid://shopify/ProductVariant/{variant_id}')

                            payload['automaticBxgyDiscount']['customerGets']['items']['products'][
                                'productVariantsToAdd'
                            ].append(f'gid://shopify/ProductVariant/{variant_id}')
                        else:
                            # Single Product
                            payload['automaticBxgyDiscount']['customerBuys']['items']['products'][
                                'productsToAdd'
                            ].append(f'gid://shopify/Product/{shopify_id}')
                            payload['automaticBxgyDiscount']['customerGets']['items']['products'][
                                'productsToAdd'
                            ].append(f'gid://shopify/Product/{shopify_id}')

            if rule.mw_bogo_items:
                for i in rule.mw_bogo_items:
                    if i not in rule.items:
                        shopify_id = Database.Shopify.Product.get_id(item_no=i)
                        variant_id = Database.Shopify.Product.Variant.get_id(sku=i)
                        if shopify_id:
                            if variant_id:
                                # Individual Variants for Bound Products
                                payload['automaticBxgyDiscount']['customerBuys']['items']['products'][
                                    'productVariantsToRemove'
                                ].append(f'gid://shopify/ProductVariant/{variant_id}')

                                payload['automaticBxgyDiscount']['customerGets']['items']['products'][
                                    'productVariantsToRemove'
                                ].append(f'gid://shopify/ProductVariant/{variant_id}')
                            else:
                                # Single Product
                                payload['automaticBxgyDiscount']['customerBuys']['items']['products'][
                                    'productsToRemove'
                                ].append(f'gid://shopify/Product/{shopify_id}')
                                payload['automaticBxgyDiscount']['customerGets']['items']['products'][
                                    'productsToRemove'
                                ].append(f'gid://shopify/Product/{shopify_id}')

            if not self.no_beg_date:
                payload['automaticBxgyDiscount']['startsAt'] = self.beg_date

            if self.no_end_date:
                payload['automaticBxgyDiscount']['endsAt'] = None
            else:
                payload['automaticBxgyDiscount']['endsAt'] = self.end_dat
            return payload

        def process(self):
            Promotions.logger.info(f'Processing Promotion: {self.grp_cod}')
            for rule in self.price_rules:
                if not rule.is_retail:
                    ############################################################################
                    ########################### Wholesale Promotions ###########################
                    ############ Wholesale Promotions are NOT added by this script. ############
                    ############################################################################
                    Promotions.logger.info(
                        f'Promotion: {self.grp_cod} Rule: {rule.seq_no} is a Wholesale Promotion. Skipping...'
                    )
                    # Remove Sale Price if the promotion is not retail. This could happen if a promotion is setup as
                    # retail and then changed to wholesale.
                    if Database.Shopify.Promotion.has_fixed_priced_items(rule):
                        self.remove_sale_price(rule)
                else:
                    #########################################################################
                    ########################### Retail Promotions ###########################
                    #########################################################################

                    Promotions.logger.info(f'Processing Retail Rule: {rule.seq_no}\n\n{rule}')

                    if rule.is_bogo_twoofer():
                        ############################## RETAIL ###############################
                        ###################### BOGO Twoofer Promotions ######################
                        #####################################################################
                        self.process_line_deletes(rule, bogo=True)
                        # process BOGO Twoofers
                        variables = self.get_bxgy_payload(rule)
                        if rule.shopify_id:
                            Promotions.logger.info(f'Updating BOGO Twoofer: {rule.shopify_id}')
                            Shopify.Discount.Automatic.Bxgy.update(variables)
                        else:
                            rule.shopify_id = Shopify.Discount.Automatic.Bxgy.create(variables)

                        self.set_sale_status(rule)

                        # Sync BOGO Twoofer Promotions to Middleware.
                        # Fixed Price Promotions are processed outside this block.
                        Database.Shopify.Promotion.sync(rule)
                    else:
                        ############################## RETAIL ################################
                        ######################################################################
                        ####################### Fixed Price Promotions #######################
                        ######################################################################
                        # Process Non-BOGO Twoofer Promotions by adding PRC_2 to the item if they
                        # are not already on sale. This also adds the item to the Shopify Promotion
                        # Fixed Price Line table. This table will be used to determine if an item
                        # is on sale and should be removed if the promotion is disabled.
                        self.process_line_deletes(rule, fixed_price=True)

                        if rule.is_enabled_cp:
                            self.add_sale_price(rule)
                        else:
                            self.remove_sale_price(rule)

        def process_line_deletes(self, rule: 'Promotions.Promotion.PriceRule', bogo=False, fixed_price=False):
            cp_rule_items = rule.items
            mw_rule_items = rule.mw_bogo_items
            if mw_rule_items:
                delete_list = [x for x in mw_rule_items if x not in cp_rule_items]
                if delete_list:
                    if bogo:
                        Database.Shopify.Promotion.BxgyLine.delete(item_no_list=delete_list)
                    elif fixed_price:
                        for item in delete_list:
                            Database.Shopify.Promotion.FixLine.delete(
                                group_cod=self.grp_cod, rul_seq_no=rule.seq_no, item_no_list=item
                            )
                    Database.Counterpoint.Product.set_sale_status(items=delete_list, status=False)

        def set_sale_status(self, rule: 'Promotions.Promotion.PriceRule'):
            Database.Counterpoint.Product.set_sale_status(
                items=rule.items, status=rule.is_enabled_cp, description=rule.badge_text
            )

            if rule.is_enabled_cp and not rule.is_enabled_mw:
                Shopify.Discount.Automatic.activate(rule.shopify_id)

            elif not rule.is_enabled_cp and rule.is_enabled_mw:
                Shopify.Discount.Automatic.deactivate(rule.shopify_id)

            elif rule.is_enabled_cp == rule.is_enabled_mw:
                pass

            else:
                Promotions.logger.info(f'You missed something. CP: {rule.is_enabled_cp} MW: {rule.is_enabled_mw}')

        def add_sale_price(self, rule: 'Promotions.Promotion.PriceRule'):
            """Updates the sale price for non-bogo items in the promotion. Adds on sale flag and sale description."""

            def get_target_price(item, target_method, target_amount):
                if target_method == 'P':
                    # pick price. Do nothing
                    pass
                if target_method == 'D':
                    # Percentage Discount
                    Promotions.logger.log(
                        f'Item: {item.item_no} Price 1: {item.price_1} Target Amount: {target_amount}'
                    )
                    item_sale_price = round(float(item.price_1 * ((100 - target_amount) / 100)), 2)
                elif target_method == 'F':
                    # Fixed Price Adjustment
                    item_sale_price = target_amount
                elif target_method == 'A':
                    # Amount Discount
                    item_sale_price = round(float(item.price_1) - target_amount, 2)

                return item_sale_price

            if not rule.items:
                Promotions.logger.warn(f'Add Sale Prices: No Items Found for Rule: {rule.seq_no}')
                return

            Database.Counterpoint.Product.set_sale_status(
                items=rule.items, status=True, description=rule.badge_text
            )

            # Set Sale Price
            target_price_break = rule.price_breaks[0]
            target_method = target_price_break.prc_meth
            target_amount = target_price_break.amt_or_pct
            for i in rule.items:
                print(f'Processing Item: {i}')
                item = Product(i)
                try:
                    current_sale_price = round(float(item.price_2), 2)
                except:
                    current_sale_price = None

                target_sale_price = get_target_price(item, target_method, target_amount)

                if current_sale_price == target_sale_price:
                    continue  # Skip if the sale price is already set
                else:
                    Database.Counterpoint.Product.set_sale_price(sku=i, price=target_sale_price)

                    if i not in rule.mw_fixed_price_items:
                        Database.Shopify.Promotion.FixLine.insert(
                            group_cod=self.grp_cod, rul_seq_no=rule.seq_no, item_no=i
                        )

        def remove_sale_price(self, rule: 'Promotions.Promotion.PriceRule'):
            """Removes the sale price for Non BOGO TWOOFER items in the promotion.
            Removes on sale flag and sale description."""
            if not rule.items:
                Promotions.logger.warn(f'Remove Sale Prices: No Items Found for Rule: {rule.seq_no}')
                return
            Database.Counterpoint.Product.remove_sale_price(rule.items)

            Database.Counterpoint.Product.set_sale_status(items=rule.items, status=False)
            # Remove Sale Price
            for item in rule.items:
                Database.Shopify.Promotion.FixLine.delete(
                    group_cod=self.grp_cod, rul_seq_no=rule.seq_no, item_no=item
                )

        @staticmethod
        def delete(group_code=None, shopify_discount_code_id=None):
            if not group_code and not shopify_discount_code_id:
                Promotions.logger.warn('PROMOTIONS: No Group Code or Shopify ID provided.')
                return
            if shopify_discount_code_id:
                rules = shopify_discount_code_id
            if group_code:
                rules = Database.Shopify.Promotion.get(
                    group_code=group_code
                )  # Get all rules with the same group code
            if not rules:
                Promotions.logger.warn(f'PROMOTIONS: No Shopify ID found for Group Code: {group_code}')
                return

            for shopify_id in rules:
                items = Database.Shopify.Promotion.BxgyLine.get(shopify_id)
                if items:
                    Database.Counterpoint.Product.set_sale_status(items=items, status=False)

                Shopify.Discount.Automatic.delete(shopify_id)
                Database.Shopify.Promotion.delete(shopify_id)

        class PriceRule:
            def __init__(self, rule):
                self.grp_typ = rule[0]
                self.grp_cod = rule[1]
                self.seq_no = rule[2]
                self.descr: str = rule[3]
                self.shopify_title = self.get_shopify_title()
                self.cust_filt = rule[4]
                if self.cust_filt:
                    self.is_retail = False if 'WHOLESALE' in self.cust_filt else True
                else:
                    self.is_retail = True
                self.item_filt = rule[5]
                self.sal_filt = rule[6]
                self.is_custom = rule[7]
                self.use_bogo_twoofer = rule[8]
                self.req_full_group_for_bogo = rule[9]
                self.shopify_id = rule[10]
                self.is_enabled_cp = True if rule[11] == 'Y' else False
                self.is_enabled_mw = True if rule[12] == 1 else False
                self.db_id = rule[13]

                self.price_breaks: list[Promotions.Promotion.PriceRule.PriceBreak] = self.get_price_breaks()

                self.items: list[str] = self.get_cp_items()  # List of CP Item Numbers

                self.mw_bogo_items: list[str] = Database.Shopify.Promotion.BxgyLine.get(self.shopify_id)

                self.mw_fixed_price_items: list[str] = Database.Shopify.Promotion.FixLine.get(
                    group_cod=self.grp_cod, rul_seq_no=self.seq_no
                )
                self.badge_text = self.get_badge_text()

            def __str__(self) -> str:
                result = f'\tRule Sequence Number: {self.seq_no}\n'
                result += f'\tGroup Code: {self.grp_cod}\n'
                result += f'\tCP Enabled: {self.is_enabled_cp}\n'
                result += f'\tMW Enabled: {self.is_enabled_mw}\n'
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

            def get_shopify_title(self) -> str:
                return str(self.descr).strip()

            def is_bogo_twoofer(self) -> bool:
                return self.use_bogo_twoofer == 'Y' and self.req_full_group_for_bogo == 'Y'

            def get_price_breaks(self) -> list[object]:
                query = f"""
                        SELECT MIN_QTY, PRC_METH, PRC_BASIS, AMT_OR_PCT
                        FROM IM_PRC_RUL_BRK
                        WHERE GRP_COD = '{self.grp_cod}' AND RUL_SEQ_NO = {self.seq_no}
                        """
                response = Database.query(query)
                if response:
                    result = []
                    for break_data in response:
                        result.append(self.PriceBreak(break_data))
                    return result

            def get_cp_items(self) -> list[str]:
                result = []
                if self.item_filt:
                    where_filter = f'WHERE {self.item_filt}'
                else:
                    where_filter = ''

                where_filter + " AND IS_ECOMM_ITEM = 'Y'"

                query = f'SELECT ITEM_NO FROM IM_ITEM {where_filter}'
                response = Database.query(query)
                if response:
                    for item in response:
                        item_no = item[0]
                        result.append(item_no)
                return result

            def get_reward_quantity(self) -> tuple:
                """Calculates the number of reward products given to a customer after a condition is met.
                Example: Buy 1 Get 1 Free = 1 reward product, Buy 2 Get 1 Free = 1 reward product, etc.
                """
                if len(self.price_breaks) == 1:
                    raise ValueError('Single Price Break Found. This is not supported at this time.')

                elif len(self.price_breaks) == 2:
                    first_rule_qty = int(self.price_breaks[0].min_qty)
                    second_rule_qty = int(self.price_breaks[1].min_qty)
                    reward = second_rule_qty - first_rule_qty
                else:
                    raise ValueError('More than two price breaks found. This is not supported at this time.')

                return first_rule_qty, reward

            def get_discount_amount(self, fixed_price=False):
                """Get the discount amount of the final price break."""
                import math

                if fixed_price:
                    retail_price = None
                    response = Database.query(f"SELECT PRC_1 FROM IM_ITEM WHERE ITEM_NO = '{self.items[0]}'")
                    retail_price = response[0][0] if response else None
                    if retail_price:
                        if self.price_breaks[-1].amt_or_pct:
                            fixed_price = self.price_breaks[-1].amt_or_pct

                            discount_amount = math.floor(100 - (fixed_price * 100 / retail_price))
                            return discount_amount
                else:
                    try:
                        discount_amount = math.floor(self.price_breaks[-1].amt_or_pct)
                    except:
                        discount_amount = self.price_breaks[-1].amt_or_pct

                    return discount_amount

            def get_badge_text(self) -> str:
                """Creates a custom badge for the promotion."""
                if self.is_bogo_twoofer():
                    message = self.descr.split('-')[-1].strip()
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

            @staticmethod
            def get_shopify_items(rule: 'Promotions.Promotion.PriceRule') -> list[dict]:
                """Takes in a rule sequence number and returns a list of associated Item Shopify Product IDs."""
                result = []
                for item in rule.items:
                    shopify_prod_id = Database.Shopify.Product.get_id(item_no=item)
                    shopify_variant_id = Database.Shopify.Product.Variant.get_id(sku=item)
                    if shopify_prod_id:
                        result.append(
                            {'sku': item, 'shopify_id': shopify_prod_id, 'variant_id': shopify_variant_id}
                        )
                return result

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

            class FixedPriceItem:
                def __init__(self, item_no):
                    self.item_no = item_no


if __name__ == '__main__':
    promo = Promotions(last_sync=datetime(2024, 9, 17, 17))
    for p in promo.promotions:
        p.process()