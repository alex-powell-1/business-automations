from datetime import datetime
from database import Database as db
from setup.error_handler import ProcessOutErrorHandler
from setup.utilities import convert_to_utc
from product_tools.products import Product
from integration.shopify_api import Shopify
from traceback import format_exc as tb
import json


class PromotionItem:
    def __init__(self, item_no, group_code: str, group_seq_no: int, rule_seq_no: int):
        self.item_no = item_no
        self.group_code = group_code
        self.group_seq_no = group_seq_no
        self.rule_seq_no = rule_seq_no

    def __str__(self) -> str:
        return f'Item: {self.item_no} Group Code: {self.group_code} Group Seq No: {self.group_seq_no} Rule Seq No: {self.rule_seq_no}'


class Promotions:
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, last_sync=None, verbose=False):
        self.last_sync = last_sync
        self.verbose = verbose
        self.promotions: list[Promotion] = []
        self.valid_items: set[str] = set()
        """A set of all items that are in a valid promotion."""
        self.sync_queue: list[Promotion] = []
        self.update_count = 0
        self.sale_badges = {}
        self.sale_badge_items = []

        self.get_promotions()
        self.get_sync_queue()

    def __str__(self) -> str:
        result = ''
        if self.sync_queue:
            result = f'Promotions to Process: {len(self.sync_queue)}\n'
            if self.verbose:
                for promo in self.sync_queue:
                    result += f'{promo}'
        return result

    def get_promotions(self):
        promo_data = db.CP.Promotion.get()
        for promo in promo_data:
            if promo['GRP_COD']:  # Check for empty group codes
                try:
                    promotion = Promotion(promo=promo, verbose=self.verbose)
                    self.promotions.append(promotion)
                    if promotion.is_valid:
                        for item in promotion.items:
                            self.valid_items.add(item)
                except Exception as e:
                    Promotions.error_handler.add_error_v(error=e, origin='Promotion Sync', traceback=tb())

    def process_deletes(self):
        """Deletes promotions that are no longer in Counterpoint."""

        def delete_invalid_items(items_list: list[str]):
            # Check if any of the items are in the valid items list. If so, these
            # items are in another promotion and should not be removed from the sale status.
            invalid_items: list[str] = []
            for item in items_list:
                if item not in self.valid_items:
                    invalid_items.append(item)
            # Remove the items from the sale in Counterpoint if they aren't in
            # any other valid promotion
            if invalid_items:
                db.CP.Product.remove_from_sale(invalid_items, verbose=self.verbose)

        def delete_dangling_items():
            """Finds and removes items that are on sale in counterpoint but are not in
            the validated items list."""
            if self.verbose:
                Promotions.logger.info('Processing Dangling Sale Items')
            on_sale_items = db.CP.Product.get_on_sale_items()
            if not on_sale_items:
                if self.verbose:
                    Promotions.logger.info('No Items on Sale.')
                return

            dangling_items = []

            for item in on_sale_items:
                if item not in self.valid_items:
                    dangling_items.append(item)

            if not dangling_items:
                if self.verbose:
                    Promotions.logger.info('No Dangling Sale Items.')
                return

            db.CP.Product.remove_from_sale(dangling_items, verbose=self.verbose)
            # db.Shopify.Promotion.BxgyLine.delete(items=dangling_items, verbose=self.verbose)
            db.Shopify.Promotion.FixLine.delete(items=dangling_items, verbose=self.verbose)

        def delete_dangling_promotions():
            """Deletes promotions that are no longer in Counterpoint."""
            if self.verbose:
                Promotions.logger.info('Processing Dangling Promotional Price Groups')
            mw_promotions = db.Shopify.Promotion.get()
            if not mw_promotions:
                if self.verbose:
                    Promotions.logger.info('PROMOTIONS: No Promotional Price Groups to delete.')
                return

            cp_promotions: list[str] = [x.grp_cod for x in self.promotions]

            delete_queue = []
            for mw_promotion in mw_promotions:
                if mw_promotion not in cp_promotions:
                    delete_queue.append(mw_promotion)

            if not delete_queue:
                if self.verbose:
                    Promotions.logger.info('No Promotional Price Groups to delete.')
                return

            for promo in delete_queue:
                shopify_id = db.Shopify.Promotion.get(promo)
                if shopify_id:
                    Promotions.logger.info(f'Deleting Promotional Price Groups: {promo}')
                    Promotion.delete(shopify_discount_code_id=shopify_id)

        def delete_dangling_rules():
            """Deletes price rules that are no longer in Counterpoint."""
            for promo in self.promotions:
                if self.verbose:
                    Promotions.logger.info(f'Promotion: {promo.grp_cod}-Processing Price Rule Deletes')

                mw_lin_seq_nos: list[int] = db.Shopify.Promotion.FixLine.get_rule_seq_nos(group_cod=promo.grp_cod)
                cp_lin_seq_nos = db.CP.Promotion.get_rul_seq_nos(promo.grp_cod)
                if not mw_lin_seq_nos:
                    if self.verbose:
                        Promotions.logger.info(f'No Middleware Price Rule for Group Code: {promo.grp_cod}')
                    return

                delete_queue = []
                for seq_no in mw_lin_seq_nos:
                    if seq_no not in cp_lin_seq_nos:
                        delete_queue.append(seq_no)

                if not delete_queue:
                    if self.verbose:
                        Promotions.logger.info(f'No Price Rule Deletes for Group Code: {promo.grp_cod}')
                    return

                if self.verbose:
                    Promotions.logger.info(f'{promo.grp_cod} Price Rule Delete Queue: {delete_queue}')

                for seq_no in delete_queue:
                    # Get all the items associated with the rule number from the middleware
                    items_list = db.Shopify.Promotion.get_items_by_rule(promo.grp_cod, seq_no)
                    delete_invalid_items(items_list)
                    db.Shopify.Promotion.delete_items_by_rule_no(
                        group_code=promo.grp_cod, rule_seq_no=seq_no, verbose=self.verbose
                    )

                db.CP.Promotion.update_timestamp(promo.grp_cod)

                if self.verbose:
                    Promotions.logger.info(f'Line Deletes Complete for Group Code: {promo.grp_cod}')

        def delete_invalid_rules():
            """Deletes price rules references that exist but are no longer valid.
            ex. A rule was created for retail but then changed to wholesale after creation."""
            for promo in self.promotions:
                if self.verbose:
                    Promotions.logger.info(f'Promotion: {promo.grp_cod}-Processing Invalid Price Rule Deletes')

                for rule in promo.price_rules:
                    if not rule.is_valid:
                        if self.verbose:
                            Promotions.logger.info(f'Deleting Invalid Rule: {rule.seq_no}')
                        # Get all the items associated with the rule number from the middleware
                        items_list = db.Shopify.Promotion.get_items_by_rule(promo.grp_cod, rule.seq_no)
                        delete_invalid_items(items_list)
                        # Delete the rule items from the middleware
                        db.Shopify.Promotion.delete_items_by_rule_no(promo.grp_cod, rule.seq_no)

        delete_dangling_items()
        delete_dangling_promotions()
        delete_dangling_rules()
        delete_invalid_rules()

    def get_sync_queue(self):
        for promotion in self.promotions:
            if promotion.lst_maint_dt > self.last_sync:
                self.sync_queue.append(promotion)

    def process(self, promotion: 'Promotion'):
        Promotions.logger.info(f'Processing Promotion: {promotion.grp_cod}')
        if not promotion.is_valid:
            if self.verbose:
                Promotions.logger.warn(f'Promotion: {promotion.grp_cod} is not valid. Skipping...')
            return

        for rule in promotion.price_rules:
            if not rule.is_valid:
                if self.verbose:
                    Promotions.logger.warn(f'Rule: {rule.seq_no} is not valid. Skipping...')
                continue

            Promotions.logger.info(f'Processing Retail Rule: {rule.seq_no}\n\n{rule}')

            if rule.is_bogo_twoofer():
                continue
                # Processing of BOGO Twoofer Promotions has been suspended.
                # This is due to the complexity of the promotion and the lack of
                # a clear understanding of how the promotion should be processed.
                # The promotion is still being processed in the middleware, but the
                # Shopify promotion is not being updated.

                #####################################################################
                ###################### BOGO Twoofer Promotions ######################
                #####################################################################
                # process BOGO Twoofers
                if promotion.is_enabled:
                    variables = promotion.get_bxgy_payload(rule)
                    if rule.shopify_id:
                        Promotions.logger.info(f'Updating BOGO Twoofer: {rule.shopify_id}')
                        Shopify.Discount.Automatic.Bxgy.update(variables)
                    else:
                        rule.shopify_id = Shopify.Discount.Automatic.Bxgy.create(variables)

                else:
                    Promotions.logger.info(f'Group Code: {promotion.grp_cod} Rule: {rule.seq_no} is not enabled.')

                promotion.set_sale_status(rule)  # will handle activating/deactivating the promotion in Shopify

                # Sync BOGO Twoofer Promotions to Middleware.
                # Fixed Price Promotions are processed outside this block.
                db.Shopify.Promotion.sync(rule)
            else:
                ######################################################################
                ####################### Fixed Price Promotions #######################
                ######################################################################
                # Process Non-BOGO Twoofer Promotions by adding PRC_2 to the item if they
                # are not already on sale. This also adds the item to the Shopify Promotion
                # Fixed Price Line table. This table will be used to determine if an item
                # is on sale and should be removed if the promotion is disabled.
                if rule.is_enabled_cp:
                    print(rule)
                    for item in rule.items:
                        print(item)
                    # promotion.add_sale_price(rule)
                # else:
                # promotion.remove_sale_price(rule)

    def sync(self):
        self.process_deletes()

        if not self.sync_queue:
            Promotions.logger.info('PROMOTIONS: No Promotions to update.')
            return

        success_count = 0
        fail_count = 0
        fail_group_codes = []

        for promotion in self.sync_queue:
            try:
                self.process(promotion)
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

    def __init__(self, promo, verbose=False):
        self.verbose = verbose
        self.grp_typ = promo['GRP_TYP']
        self.grp_cod = promo['GRP_COD']
        self.grp_seq_no = promo['GRP_SEQ_NO']
        self.descr = promo['DESCR']
        self.cust_filt = promo['CUST_FILT']
        self.no_beg_date = True if promo['NO_BEG_DAT'] == 'Y' else False
        self.beg_date: datetime = promo['BEG_DAT']
        self.beg_date_utc: str = None
        self.beg_time_flag = int(promo['BEG_TIM_FLG']) if promo['BEG_TIM_FLG'] else 0
        self.no_end_date = True if promo['NO_END_DAT'] == 'Y' else False
        self.end_date: datetime = promo['END_DAT']
        self.end_date_utc: str = None
        self.end_time_flag = int(promo['END_TIM_FLG']) if promo['END_TIM_FLG'] else 0
        self.lst_maint_dt = promo['LST_MAINT_DT']
        self.is_enabled = True if promo['ENABLED'] == 'Y' else False
        self.mix_match_code = promo['MIX_MATCH_COD']
        self.max_uses = None
        self.price_rules: list[PriceRule] = []
        self.items: list[PromotionItem] = []
        self.get_start_end_dates()
        self.get_price_rules()
        self.is_valid = True
        self.validate()

    def __str__(self) -> str:
        result = '-----------------------------------\n'
        result += f'PROMOTION: {self.grp_cod}\n'
        result += f'Group Code: {self.grp_cod}\n'
        result += f'Description: {self.descr}\n'
        result += f'Customer Filter: {self.cust_filt}\n'
        result += f'Begin Date: {self.beg_date}\n'
        result += f'End Date: {self.end_date}\n'
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

    def validate(self):
        if not self.is_enabled:
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Promotion: {self.grp_cod} is disabled.')
        if not self.no_beg_date and self.beg_date > datetime.now():
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Promotion: {self.grp_cod} has not started yet.')
        if not self.no_end_date and self.end_date < datetime.now():
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Promotion: {self.grp_cod} has ended.')
        if not self.price_rules:
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Promotion: {self.grp_cod} has no price rules.')
        if not self.items:
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Promotion: {self.grp_cod} has no items.')

    def get_start_end_dates(self):
        """Converts the start and end dates to UTC time."""
        if self.beg_date:
            if self.beg_time_flag != 0:  # 0 represents the beginning of the day, ie. 12:00:00 AM.
                self.beg_date = self.beg_date.replace(hour=self.beg_time_flag)
            self.beg_date_utc = convert_to_utc(self.beg_date)

        if self.end_date:
            if self.end_time_flag == 0:
                # 0 Represents the end of the day, ie. 11:59:59 PM.
                self.end_date = self.end_date.replace(hour=23, minute=59, second=59)
            else:
                self.end_date = self.end_date.replace(hour=self.end_time_flag)
            self.end_dat_utc = convert_to_utc(self.end_date)

    def get_price_rules(self):
        rules = db.CP.Promotion.PriceRule.get(self.grp_cod)
        if rules:
            for rule in rules:
                price_rule = PriceRule(rule, verbose=self.verbose)
                self.price_rules.append(price_rule)
                self.items += price_rule.items

    def get_bxgy_payload(self, rule: 'PriceRule') -> dict:
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
                'title': rule.descr,  # 'Buy 1 Get 1 50% Off'
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

        for i in rule.items:
            shopify_id = db.Shopify.Product.get_id(item_no=i)
            variant_id = db.Shopify.Product.Variant.get_id(sku=i)
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
                    payload['automaticBxgyDiscount']['customerBuys']['items']['products']['productsToAdd'].append(
                        f'gid://shopify/Product/{shopify_id}'
                    )
                    payload['automaticBxgyDiscount']['customerGets']['items']['products']['productsToAdd'].append(
                        f'gid://shopify/Product/{shopify_id}'
                    )

        for i in rule.mw_bogo_items:
            if i not in rule.items:
                shopify_id = db.Shopify.Product.get_id(item_no=i)
                variant_id = db.Shopify.Product.Variant.get_id(sku=i)
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
            payload['automaticBxgyDiscount']['startsAt'] = self.beg_date_utc

        if self.no_end_date:
            payload['automaticBxgyDiscount']['endsAt'] = None
        else:
            payload['automaticBxgyDiscount']['endsAt'] = self.end_date_utc
        return payload

    def set_sale_status(self, rule: 'PriceRule'):
        if rule.items:
            db.CP.Product.add_to_sale(items=rule.items, description=rule.badge_text, verbose=self.verbose)
        if rule.is_enabled_cp and not rule.is_enabled_mw:
            Shopify.Discount.Automatic.activate(rule.shopify_id)
        elif not rule.is_enabled_cp and rule.is_enabled_mw:
            Shopify.Discount.Automatic.deactivate(rule.shopify_id)
        else:
            Promotions.logger.info(f'You missed something. CP: {rule.is_enabled_cp} MW: {rule.is_enabled_mw}')

    def add_sale_price(self, rule: 'PriceRule'):
        """Updates the sale price for non-bogo items in the promotion. Adds on sale flag and sale description."""

        def get_target_price(item: Product, target_method, target_amount):
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

        # Add IS_ON_SALE flag to items and SALE_DESCR and Sale Category
        db.CP.Product.add_to_sale(items=rule.items, description=rule.badge_text, verbose=self.verbose)

        # Set Sale Price
        target_price_break = rule.price_breaks[0]
        target_method = target_price_break.prc_meth
        target_amount = float(target_price_break.amt_or_pct)
        for i in rule.items:
            item = Product(i)
            try:
                current_sale_price = round(float(item.price_2), 2)
            except:
                current_sale_price = None

            target_sale_price = get_target_price(item, target_method, target_amount)

            if current_sale_price == target_sale_price:
                continue  # Skip if the sale price is already set
            else:
                db.CP.Product.set_sale_price(sku=i, price=target_sale_price)

                if rule.mw_fixed_price_items:
                    if i not in rule.mw_fixed_price_items:
                        db.Shopify.Promotion.FixLine.insert(
                            group_cod=self.grp_cod,
                            rule_seq_no=rule.seq_no,
                            item_no=item.item_no,
                            group_seq_no=self.grp_seq_no,
                        )
                else:
                    db.Shopify.Promotion.FixLine.insert(
                        group_cod=self.grp_cod,
                        rule_seq_no=rule.seq_no,
                        item_no=item.item_no,
                        group_seq_no=self.grp_seq_no,
                    )

    def remove_sale_price(self, rule: 'PriceRule'):
        """Removes the sale price for Non BOGO TWOOFER items in the promotion.
        Removes on sale flag and sale description."""
        if not rule.items:
            Promotions.logger.warn(f'Remove Sale Prices: No Items Found for Rule: {rule.seq_no}')
            return
        db.CP.Product.remove_from_sale(rule.items)
        # Remove Sale Price
        for item in rule.items:
            db.Shopify.Promotion.FixLine.delete(group_cod=self.grp_cod, rule_seq_no=rule.seq_no, item_no=item)

    @staticmethod
    def delete(group_code=None, shopify_discount_code_id=None, verbose=False):
        if not group_code and not shopify_discount_code_id:
            Promotions.logger.warn('PROMOTIONS: No Group Code or Shopify ID provided.')
            return
        if shopify_discount_code_id:
            rules = shopify_discount_code_id
        if group_code:
            rules = db.Shopify.Promotion.get(group_code=group_code)  # Get all rules with the same group code
        if not rules:
            Promotions.logger.warn(f'PROMOTIONS: No Shopify ID found for Group Code: {group_code}')
            return

        for shopify_id in rules:
            items = db.Shopify.Promotion.BxgyLine.get(shopify_id)
            if items:
                db.CP.Product.remove_from_sale(items=items, verbose=verbose)
            Shopify.Discount.Automatic.delete(shopify_id)
            db.Shopify.Promotion.delete(shopify_id)


class PriceRule:
    def __init__(self, rule, verbose=False):
        self.verbose = verbose
        self.grp_typ = rule['GRP_TYP']
        self.grp_cod = rule['GRP_COD']
        self.grp_seq_no = rule['GRP_SEQ_NO']
        self.seq_no: str = rule['RUL_SEQ_NO']
        self.descr: str = rule['DESCR']
        self.cust_filt = rule['CUST_FILT']
        if self.cust_filt:
            self.is_retail = False if 'WHOLESALE' in self.cust_filt else True
        else:
            self.is_retail = True
        self.item_filt = rule['ITEM_FILT']
        self.sal_filt = rule['SAL_FILT']
        self.is_custom = rule['IS_CUSTOM']
        self.use_bogo_twoofer = rule['USE_BOGO_TWOFER']
        self.req_full_group_for_bogo = rule['REQ_FULL_GRP_FOR_BOGO_TWOFER']
        # self.shopify_id = rule['SHOP_ID']
        self.is_enabled_cp = True if rule['ENABLED'] == 'Y' else False
        # self.is_enabled_mw = True if rule['MW_ENABLED'] == 1 else False
        # self.db_id = rule['ID']
        self.is_valid: bool = True

        self.price_breaks: list[PriceBreak] = self.get_price_breaks()

        self.items: list[PromotionItem] = []

        # self.mw_bogo_items: list[str] = db.Shopify.Promotion.BxgyLine.get(self.shopify_id)

        self.mw_fixed_price_items: list[str] = db.Shopify.Promotion.FixLine.get(
            group_cod=self.grp_cod, rule_seq_no=self.seq_no
        )

        self.badge_text = self.get_badge_text()

        self.validate()
        self.get_items()

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

    def validate(self):
        if not self.is_retail:
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Rule: {self.seq_no} is a Wholesale Promotion. Skipping...')

        if not self.items:
            self.is_valid = False
            if self.verbose:
                Promotions.logger.warn(f'Rule: {self.seq_no} has no items.')

    def get_items(self):
        item_nos = self.get_cp_items()  # List of CP Item Numbers
        for item in item_nos:
            self.items.append(
                PromotionItem(
                    item_no=item, group_code=self.grp_cod, group_seq_no=self.grp_seq_no, rule_seq_no=self.seq_no
                )
            )

    def is_bogo_twoofer(self) -> bool:
        return self.use_bogo_twoofer == 'Y' and self.req_full_group_for_bogo == 'Y'

    def get_price_breaks(self) -> list[object]:
        query = f"""
                SELECT MIN_QTY, PRC_METH, PRC_BASIS, AMT_OR_PCT
                FROM IM_PRC_RUL_BRK
                WHERE GRP_COD = '{self.grp_cod}' AND RUL_SEQ_NO = '{self.seq_no}'
                """
        response = db.query(query)
        if response:
            result = []
            for break_data in response:
                result.append(PriceBreak(break_data, verbose=self.verbose))
            return result

    def get_cp_items(self) -> list[str]:
        result = []
        if self.item_filt:
            where_filter = f'WHERE {self.item_filt}'
        else:
            where_filter = ''

        where_filter + " AND IS_ECOMM_ITEM = 'Y'"

        query = f"""SELECT ITEM_NO FROM IM_ITEM {where_filter} AND IS_ECOMM_ITEM = 'Y'"""

        response = db.query(query)

        if response:
            for item in response:
                item = item[0]
                result.append(item[0])
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
            for x in self.price_breaks:
                print(x)
            raise ValueError(
                f'More than two price breaks found. This is not supported at this time. Total breaks: {len(self.price_breaks)}'
            )

        return first_rule_qty, reward

    def get_discount_amount(self, fixed_price=False):
        """Get the discount amount of the final price break."""
        import math

        if fixed_price:
            retail_price = None
            response = db.query(f"SELECT PRC_1 FROM IM_ITEM WHERE ITEM_NO = '{self.items[0]}'")
            try:
                retail_price = response[0][0]
            except:
                retail_price = None

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
        if not self.items:
            return ''

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

            elif price_method == 'F':
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
    def get_shopify_items(rule: 'PriceRule') -> list[dict]:
        """Takes in a rule sequence number and returns a list of associated Item Shopify Product IDs."""
        result = []
        for item in rule.items:
            shopify_prod_id = db.Shopify.Product.get_id(item_no=item)
            shopify_variant_id = db.Shopify.Product.Variant.get_id(sku=item)
            if shopify_prod_id:
                result.append({'sku': item, 'shopify_id': shopify_prod_id, 'variant_id': shopify_variant_id})
        return result


class PriceBreak:
    def __init__(self, break_data, verbose=False):
        self.verbose = verbose
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
    promos = Promotions(last_sync=datetime(2020, 10, 4), verbose=True)
    promos.sync()
