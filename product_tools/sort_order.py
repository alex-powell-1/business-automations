from product_tools import products
from reporting.product_reports import create_top_items_report
from setup.date_presets import *
from setup.error_handler import ProcessOutErrorHandler as error_handler
from database import Database as db
from setup import creds

import concurrent.futures

import time

from integration.shopify_api import Shopify, MoveInput, MovesCollection

import math


def constrain(n: float | int, low: float | int, high: float | int):
    return max(min(n, high), low)


def map_val(
    n: float | int,
    start1: float | int,
    stop1: float | int,
    start2: float | int,
    stop2: float | int,
    within_bounds: bool = False,
):
    newval = (n - start1) / (stop1 - start1) * (stop2 - start2) + start2
    if not within_bounds:
        return newval

    if start2 < stop2:
        return constrain(newval, start2, stop2)
    else:
        return constrain(newval, stop2, start2)


class SortOrderEngine:
    eh = error_handler
    error_handler = eh.error_handler
    logger = error_handler.logger

    def group_ecomm_items_by_collection(top_ecomm_items_with_stock):
        """Groups top ecomm items by collection"""
        collections: dict[str, list[dict[str, str | int]]] = {}

        items_not_found = 0

        for i, item in enumerate(top_ecomm_items_with_stock):
            try:
                collection_ids = db.Shopify.Product.get_collection_ids(product_id=item['product_id'])

                for collection_id in collection_ids:
                    if collection_id not in collections:
                        collections[collection_id] = []

                    collections[collection_id].append(item)
            except Exception as e:
                SortOrderEngine.error_handler.add_error_v(
                    error=f'Error getting collection ids for {item}: {e}',
                    origin='SortOrderEngine.group_ecomm_items_by_collection',
                )
                items_not_found += 1

        if items_not_found > 0:
            SortOrderEngine.logger.warn(f'{items_not_found} items not found in Shopify')
        return collections

    def promote_fixed_price_sales(items: list):
        orig_items = items
        try:
            for item_index, item in enumerate(items):

                def insert_item_at(index1, index2):
                    item = items[index1]
                    items.pop(index1)
                    items.insert(index2, item)

                # print('ITEM: ', item['item_no'])
                if item['price_2'] is not None and item['price_1'] > item['price_2']:
                    prc_1 = float(item['price_1'])
                    prc_2 = float(item['price_2'])

                    percent_off = math.floor((1 - prc_2 / prc_1) * 100)

                    new_index = int(map_val(percent_off, 0, 80, item_index, 0, within_bounds=True))

                    # print(percent_off)
                    # print(new_index)

                    insert_item_at(item_index, new_index)

            return items
        except Exception as e:
            SortOrderEngine.error_handler.add_error_v(
                error=f'Error promoting fixed price sales: {e}', origin='SortOrderEngine.promote_fixed_price_sales'
            )
            return orig_items

    def promote_sale_items(items: list):
        return SortOrderEngine.promote_fixed_price_sales(items)

    def get_new_items():
        # {'item_no': item, 'product_id': product_id, 'price_1': price_1, 'price_2': price_2}
        new_items = [
            {'item_no': x[0], 'product_id': x[1], 'price_1': None, 'price_2': None}
            for x in products.get_all_new_items(start_date=Dates().one_month_ago)
        ]

        return new_items

    def adjust_order(items):
        """Adjusts order of items"""
        new_items = []
        item_skus = []

        featured_items = []

        try:
            query = """
            SELECT ITEM_NO FROM IM_ITEM WHERE PROMOTE_DT_EXP > GETDATE()
            """

            response = db.query(query)
            response = [x[0] for x in response] if response else []

            for item in items:
                try:
                    binding_id = products.get_binding_id(item['item_no'])
                    if binding_id is None:
                        if item['item_no'] in response:
                            featured_items.append(item)
                            item_skus.append(item['item_no'])

                        continue

                    parent = products.get_parent_product(binding_id)

                    if parent is None:
                        SortOrderEngine.error_handler.add_error_v(
                            error=f'No parent found for {item["item_no"]}', origin='SortOrderEngine.adjust_order'
                        )
                        continue

                    if parent in response:
                        featured_items.append(item)
                except Exception as e:
                    SortOrderEngine.error_handler.add_error_v(
                        error=f'Error getting parent: {e}', origin='SortOrderEngine.adjust_order'
                    )
        except:
            pass

        # print(featured_items)

        for item in items:
            if item['item_no'] in item_skus:
                continue

            new_items.append(item)
            item_skus.append(item['item_no'])

        top_4_ecomm_items = new_items[:4]
        ecomm_items = new_items[4:]

        return (
            featured_items
            + top_4_ecomm_items
            + SortOrderEngine.get_new_items()
            + SortOrderEngine.promote_sale_items(ecomm_items)
        )

    def remove_duplicate_products(items):
        """Removes duplicate products"""

        prod_ids = []
        new_items = []

        for item in items:
            if item['product_id'] not in prod_ids:
                new_items.append(item)
                prod_ids.append(item['product_id'])

        return new_items

    def parse_items(items):
        """Parses items for sorting"""
        new_items = []

        items_not_found = 0

        for item in items:
            try:
                product_id = db.Shopify.Product.get_id(item_no=item)
                product_id = int(product_id)

                def get_price(item_no):
                    query = f"""
                    SELECT PRC_1, PRC_2 FROM IM_PRC WHERE ITEM_NO = '{item_no}'
                    """
                    response = db.query(query)
                    return response[0][0], response[0][1]

                price_1, price_2 = get_price(item)

                new_items.append(
                    {'item_no': item, 'product_id': product_id, 'price_1': price_1, 'price_2': price_2}
                )
            except Exception as e:
                SortOrderEngine.error_handler.add_error_v(
                    error=f'Error parsing item {item}: {e}', origin='SortOrderEngine.parse_items'
                )
                items_not_found += 1

        if items_not_found > 0:
            SortOrderEngine.logger.warn(f'{items_not_found} items not found in Shopify')

        return new_items

    def remove_excluded_collections(collections: dict) -> dict:
        """Removes excluded collections from collections dictionary"""
        new_collections = {}

        for collection_id, items in collections.items():
            query = f"""
                SELECT IMG_FILE FROM VI_SN_SHOP_CATEG
                WHERE COLLECTION_ID = '{collection_id}'
            """

            response = db.query(query)
            try:
                do_sort = str(response[0][0]).lower().strip() == 'true'
            except Exception as e:
                SortOrderEngine.error_handler.add_error_v(
                    error=f'Error checking if collection {collection_id} is do sort: {e}',
                    origin='SortOrderEngine.remove_excluded_collections',
                )
                do_sort = False

            if do_sort:
                new_collections[collection_id] = items
            else:
                SortOrderEngine.logger.info(f'Excluding collection {collection_id}')

        return new_collections

    def sort(print_mode=False, out_of_stock_mode=True):
        """Sets sort order based on revenue data from prior year during the forecasted time period"""
        SortOrderEngine.logger.info('Sort Order: Starting')
        start_time = time.time()

        ###############################################################################################
        ############################### Get top ecomm items with stock. ###############################
        ###############################################################################################

        SortOrderEngine.logger.info('Getting top ecomm items with stock')
        top_ecomm_items_with_stock = create_top_items_report(
            beginning_date=one_year_ago,
            ending_date=last_year_forecast,
            mode='sales',
            number_of_items=products.get_ecomm_items(in_stock_only=True),
            return_format=3,
            in_stock_only=True,
        )
        # top_ecomm_items_with_stock = ['BTSP5OZ']
        SortOrderEngine.logger.success('Top ecomm items with stock retrieved')

        ###############################################################################################
        ######################################### Parse Items #########################################
        ###############################################################################################

        SortOrderEngine.logger.info('Parsing items')
        top_ecomm_items_with_stock = SortOrderEngine.parse_items(top_ecomm_items_with_stock)
        SortOrderEngine.logger.success('Items parsed')

        ###############################################################################################
        ###################################### Remove Duplicates ######################################
        ###############################################################################################

        SortOrderEngine.logger.info('Removing duplicates')
        top_ecomm_items_with_stock = SortOrderEngine.remove_duplicate_products(top_ecomm_items_with_stock)
        SortOrderEngine.logger.success('Duplicates removed')

        ###############################################################################################
        ######################################## Adjust Order. ########################################
        ###############################################################################################

        SortOrderEngine.logger.info('Adjusting order')
        top_ecomm_items_with_stock = SortOrderEngine.adjust_order(top_ecomm_items_with_stock)
        SortOrderEngine.logger.success('Order adjusted')

        ###############################################################################################
        ############################### Group eComm Items By Collection ###############################
        ###############################################################################################

        SortOrderEngine.logger.info('Grouping ecomm items by collection')
        collections: dict = SortOrderEngine.group_ecomm_items_by_collection(top_ecomm_items_with_stock)
        SortOrderEngine.logger.success('Ecomm items grouped by collection')

        ###############################################################################################
        ################################# Remove excluded collections #################################
        ###############################################################################################

        SortOrderEngine.logger.info('Removing excluded collections')
        collections = SortOrderEngine.remove_excluded_collections(collections)
        SortOrderEngine.logger.success('Excluded collections removed')

        ###############################################################################################
        ##################################### Process Collections #####################################
        ###############################################################################################

        if print_mode:
            print(collections)
            return collections

        collections_list = [(collection_id, items) for collection_id, items in collections.items()]

        def task(collection):
            collection_id, items = collection
            Shopify.Collection.change_sort_order_to_manual(collection_id=collection_id)

            mc = MovesCollection()
            for item_index, item in enumerate(items):
                product_id = item['product_id']

                move = MoveInput(item_id=product_id, position=item_index)
                mc.add(move)

            return Shopify.Collection.reorder_items(collection_id=collection_id, collection_of_moves=mc)

        SortOrderEngine.logger.info(f'Processing {len(collections_list)} collections')

        with concurrent.futures.ThreadPoolExecutor(max_workers=creds.max_workers) as executor:
            responses = executor.map(task, collections_list)

        SortOrderEngine.logger.success('Collections processed')

        if not out_of_stock_mode:
            duration = time.time() - start_time
            SortOrderEngine.logger.info(f'Sort Order: Completed in {duration:.2f} seconds')
            return []

        ###############################################################################################
        #################### Move all out of stock items to bottom of collections. ####################
        ###############################################################################################

        responses = Shopify.Collection.move_all_out_of_stock_to_bottom(eh=SortOrderEngine.eh)

        duration = time.time() - start_time

        SortOrderEngine.logger.info(f'Sort Order: Completed in {duration:.2f} seconds')

        return responses


if __name__ == '__main__':
    SortOrderEngine.sort()
