from pyodbc import Error
from setup import creds
from product_tools import products
from product_tools.products import Product
from big_commerce.big_products import bc_update_product_batch
from reporting.product_reports import create_top_items_report
from setup.date_presets import *
from setup.error_handler import ScheduledTasksErrorHandler as error_handler
from database import Database as db

import time

from integration.shopify_api import Shopify, MoveInput, MovesCollection


class SortOrderEngine:
    eh = error_handler
    error_handler = eh.error_handler
    logger = error_handler.logger
    origin = 'sort_order.py'

    def group_ecomm_items_by_collection(top_ecomm_items_with_stock):
        """Groups top ecomm items by collection"""
        collections: dict[str, list[dict[str, str | int]]] = {}

        items_not_found = 0

        for i, item in enumerate(top_ecomm_items_with_stock):
            if (len(top_ecomm_items_with_stock) > 15 and i % 10 == 0) or len(top_ecomm_items_with_stock) <= 15:
                SortOrderEngine.logger.info(f'Processing item {i + 1}/{len(top_ecomm_items_with_stock)}')
            try:
                # product_id = db.Shopify.Product.get_id(item_no=item)

                ################################################################################
                ################ Instead of using the Database helper functions ################
                ############ I'm going to directly query the table to improve speed ############
                ################################################################################

                query = f"""
                SELECT PRODUCT_ID, CATEG_ID FROM {creds.Table.Middleware.products}
                WHERE ITEM_NO = '{item}'
                """

                response = db.query(query)
                try:
                    product_id = response[0][0]
                    collection_ids = [int(x) for x in response[0][1].split(',')]
                except:
                    product_id = None
                    collection_ids = []

                if product_id is None or len(collection_ids) == 0:
                    items_not_found += 1
                    continue

                product_id = int(product_id)

                # collection_ids = db.Shopify.Product.get_collection_ids(product_id=product_id)

                for collection_id in collection_ids:
                    if collection_id not in collections:
                        collections[collection_id] = []

                    collections[collection_id].append({'item_no': item, 'product_id': product_id})
            except Exception as e:
                SortOrderEngine.error_handler.add_error_v(
                    error=f'Error getting collection ids for {item}: {e}',
                    origin='SortOrderEngine.group_ecomm_items_by_collection',
                )

        if items_not_found > 0:
            SortOrderEngine.logger.warn(f'{items_not_found} items not found in Shopify')
        return collections

    def sort():
        """Sets sort order based on revenue data from prior year during the forecasted time period"""
        SortOrderEngine.logger.info('Sort Order: Starting')
        start_time = time.time()

        ###############################################################################################
        ######################################### First Step. #########################################
        ###############################################################################################

        SortOrderEngine.logger.info('Getting top ecomm items with stock')
        top_ecomm_items_with_stock = create_top_items_report(
            beginning_date=one_year_ago,
            ending_date=last_year_forecast,
            mode='sales',
            number_of_items=products.get_ecomm_items(),
            return_format=3,
        )
        # top_ecomm_items_with_stock = ['BTSP5OZ']
        SortOrderEngine.logger.success('Top ecomm items with stock retrieved')

        ###############################################################################################
        ############################### Group eComm Items By Collection ###############################
        ###############################################################################################

        SortOrderEngine.logger.info('Grouping ecomm items by collection')
        collections: dict = SortOrderEngine.group_ecomm_items_by_collection(top_ecomm_items_with_stock)
        SortOrderEngine.logger.success('Ecomm items grouped by collection')

        collections_list = [(collection_id, items) for collection_id, items in collections.items()]

        # TODO: Remove duplicate product id entries. Keep first index.
        SortOrderEngine.logger.info('Removing duplicate product IDs')
        for i, collection in enumerate(collections_list):
            collection_id, items = collection

            prod_ids = []
            new_items = []

            for item in items:
                if item['product_id'] not in prod_ids:
                    new_items.append(item)
                    prod_ids.append(item['product_id'])

            collections_list[i] = (collection_id, new_items)

        SortOrderEngine.logger.success('Duplicate product IDs removed')

        ###############################################################################################
        ########################################## Last Step ##########################################
        ###############################################################################################

        for collection_index, collection in enumerate(collections_list):
            collection_id, items = collection
            SortOrderEngine.logger.info(f'Processing collection {collection_id}')
            SortOrderEngine.logger.info(f'Collection {collection_index + 1}/{len(collections_list)}')

            # Change sort order to manual
            Shopify.Collection.change_sort_order_to_manual(collection_id=collection_id)

            mc = MovesCollection()
            for item_index, item in enumerate(items):
                product_id = item['product_id']

                move = MoveInput(item_id=product_id, position=item_index)
                mc.add(move)

            responses = Shopify.Collection.reorder_items(collection_id=collection_id, collection_of_moves=mc)
            SortOrderEngine.logger.success(f'Collection {collection_id} processed')

        responses = Shopify.Collection.move_all_out_of_stock_to_bottom(eh=SortOrderEngine.eh)

        duration = time.time() - start_time

        SortOrderEngine.logger.info(f'Sort Order: Completed in {duration:.2f} seconds')

        return responses


def sort_order_engine():
    """Sets sort order based on revenue data from prior year during the forecasted time period"""
    error_handler.logger.info(f'Sort Order: Starting at {datetime.now():%H:%M:%S}')
    # # WITH SALES HISTORY DURING SALES WINDOW
    top_ecomm_items_with_stock = create_top_items_report(
        beginning_date=one_year_ago,
        ending_date=last_year_forecast,
        mode='sales',
        number_of_items=products.get_ecomm_items(),
        return_format=3,
    )

    # ITEMS WITH STOCK AND SALES HISTORY -- STEP 1:
    # Set sort order for items with sales history during the selected date range
    # Sort order is generated by revenue.
    # Big Commerce prioritized the lowest value in category display. So, highest rank items get
    # the lowest value

    count = -1 * len(top_ecomm_items_with_stock)
    # iterators
    x = 0
    y = 1
    max_retries = 25
    error_handler.logger.info('Setting sort order for merged items: children only -- Starting')
    while x < len(top_ecomm_items_with_stock) and y <= max_retries:
        # check this out. Maybe problematic
        # took this out because of issues. 10093 not showing as high rank.
        # if ecomm_items_with_stock[x] in top_ecomm_items_with_stock:
        try:
            item = Product(top_ecomm_items_with_stock[x])
        except Error:
            # continues to the next iteration of the loop without increasing x for retry
            # y is increased so that if there are more than y failed requests, operation stops
            y += 1
            continue
        else:
            # Set sort order for child products
            if item.is_parent == 'N':
                try:
                    item.set_sort_order(target_sort_order=count)
                except Error:
                    y += 1
                    continue
                else:
                    count += 1
                    x += 1
            else:
                x += 1
        # else:
        #     x += 1

    if y == max_retries:
        error_handler.error_handler.add_error_v(
            error='Could not complete. Max Tries Reached.', origin='sort_order step 1'
        )
    else:
        error_handler.logger.success('Setting sort order for merged items: children only -- Completed!')

    # ITEMS WITH STOCK AND SALES HISTORY -- STEP 2:
    # Set sort order for parent items based on revenue of best-selling child product_tools

    error_handler.logger.info('Setting sort order for parents based on top child')

    x = 0
    y = 1
    max_tries = 25
    binding_ids = products.get_binding_ids()
    while x < len(binding_ids) and y <= max_tries:
        try:
            parent_product = Product(products.get_parent_product(binding_ids[x]))
            # Get sort order for highest performing child
            top_child_sort_order = Product(parent_product.get_top_child_product()).sort_order
            # Set the parent product to the sort order of the highest performing child
            parent_product.set_sort_order(target_sort_order=int(top_child_sort_order))
        except Error:
            y += 1
        else:
            x += 1
    if y == max_tries:
        error_handler.error_handler.add_error_v(
            error='Could not complete. Max Tries Reached.', origin='sort_order step 2'
        )
    else:
        error_handler.logger.info('Setting sort order for parents based on top child: Completed!')

    # ITEMS WITH STOCK AND SALES HISTORY -- STEP 3:
    # Clear out sort order values for child products

    query = """
    UPDATE IM_ITEM
    SET USR_PROF_ALPHA_27 = NULL
    WHERE USR_PROF_ALPHA_16 IS NOT NULL AND IS_ADM_TKT = 'N'
    """
    db.query(query)

    error_handler.logger.info('Flushed all child sort orders (with sales history)')

    # PRODUCTS WITH STOCK AND NO SALES HISTORY

    # iterators
    x = 0
    y = 1
    max_retries = 25

    error_handler.logger.info('Setting sort order for items with no history -- Starting')

    no_history_items = products.get_items_with_no_sales_history()
    while x < len(no_history_items) and y <= max_retries:
        new_items = products.get_new_items(two_weeks_ago, today, 14.95)

        # STEP 1: SET OLD PRODUCTS WITH NO SALES HISTORY IN WINDOW TO SORT: -1

        if no_history_items[x] not in new_items:
            item = Product(no_history_items[x])
            try:
                item.set_sort_order(target_sort_order=-1)
            except Error:
                y += 1
            else:
                x += 1
        else:
            # STEP 2: SET NEW ITEMS WITH NO SALES HISTORY TO TOP RESULT PLUS 4
            # This will present new items in the 5 position

            count = (-1 * len(top_ecomm_items_with_stock)) + 4
            item = Product(no_history_items[x])
            try:
                item.set_sort_order(target_sort_order=count)
            except Error:
                y += 1
            else:
                x += 1
    if y == max_retries:
        error_handler.logger.info('Could not complete. Max Tries Reached.')
    else:
        error_handler.logger.info('Setting sort order for items with no history -- Completed!')

    # NO STOCK ITEMS (BUFFERED)

    zero_stock_ecomm_items = products.get_zero_stock_ecomm_products()

    x = 0
    y = 1
    max_retries = 25
    error_handler.logger.info('Setting sort order for items with no stock -- Starting.')
    while x < len(zero_stock_ecomm_items) and y < max_retries:
        item = Product(zero_stock_ecomm_items[x])
        error_handler.logger.info(f'ZERO STOCK ECOMM ITEM: {item.item_no}')
        # SINGLE ITEM WITH NO STOCK
        if item.binding_key is None:
            error_handler.logger.info(f'NO BINDING KEY: {item.item_no}')
            # Set sort order to 0
            try:
                item.set_sort_order()
            except Error:
                error_handler.logger.info(f'Singled Item Error {y}/{max_retries}')
                y += 1
            else:
                x += 1

        # MERGED PRODUCT WITH NO STOCK
        else:
            if item.is_parent == 'Y':
                combined_stock = products.get_merged_product_combined_stock(item.binding_key)
                if combined_stock == 0:
                    try:
                        item.set_sort_order()
                    except Error:
                        error_handler.logger.info(f'Merged Product Error {y}/{max_retries}')
                        y += 1
                    else:
                        x += 1
                else:
                    x += 1
            else:
                x += 1

    if y == max_retries:
        error_handler.logger.info('\nCould not complete. Max Tries Reached.')
    else:
        error_handler.logger.info('\nSetting sort order for items with no stock -- Completed!')

    # Update sort order for all ecommerce items
    query = f"""
	SELECT MW.PRODUCT_ID, {creds.cp_sort_order} 
	FROM IM_ITEM CP
	INNER JOIN {creds.bc_product_table} MW on CP.ITEM_NO = MW.ITEM_NO
	WHERE {creds.cp_sort_order} IS NOT NULL"""
    response = db.query(query)
    queue = [{'id': x[0], 'sort_order': x[1]} for x in response] if response else []
    bc_update_product_batch(queue)

    error_handler.logger.info(f'Sort Order: Finished at {datetime.now():%H:%M:%S}')
    error_handler.logger.info('-----------------------')


if __name__ == '__main__':
    SortOrderEngine.sort()
