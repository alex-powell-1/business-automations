from datetime import datetime

from product_tools.products import Product
from database import Database as db
from setup.error_handler import ScheduledTasksErrorHandler

# Inactive Product Automation
#
# This automation will determine if a product should be inactive and set it to inactive status (V).
# Inactive products are automatically filtered out of the checkout choices at our POS touchscreen.
# This prevents sales team members from having to sift through and/or selling out of stock items.
#
# This script is carried out hourly at minute 0.
#
# When products are set to inactive, their status is logged in the inactive product log.
#
# Determining Inactive Product Status:
#
# A product is selected to be processed by this script when:
# - Qty available is < 1 and status is active and category is not 'SERVICES'
# - Product is not on an open order
# - Product is not in category 'BONNIE'
#


def get_active_products_with_no_stock():
    query = """
    SELECT item.ITEM_NO
    FROM IM_ITEM item
    INNER JOIN IM_INV inv on inv.ITEM_NO = item.ITEM_NO
    WHERE inv.QTY_AVAIL < 1 and item.STAT = 'A' AND item.CATEG_COD NOT IN ('SERVICES')
    ORDER BY inv.QTY_AVAIL DESC
    """
    response = db.query(query)
    if response is not None:
        result = []
        for x in response:
            result.append(x[0])
        return result


def get_products_on_open_order():
    query = """
    SELECT DISTINCT ITEM_NO
    FROM PS_DOC_LIN
    WHERE LIN_TYP = 'O'
    """
    response = db.query(query)
    item_list = []
    if response is not None:
        for x in response:
            item = x[0]
            item_list.append(item)
        return item_list
    else:
        return


def get_bonnie_items():
    query = """
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE LONG_DESCR LIKE '%BONNIE%'
    """
    response = db.query(query)
    item_list = []
    if response is not None:
        for x in response:
            item = x[0]
            item_list.append(item)
        return item_list
    else:
        return


def get_products_on_open_documents():
    query = """
    SELECT DISTINCT ITEM_NO
    FROM PS_DOC_LIN
    WHERE LIN_TYP = 'S'
    """
    response = db.query(query)
    item_list = []
    if response is not None:
        for x in response:
            item = x[0]
            item_list.append(item)
        return item_list
    else:
        return


def set_products_to_inactive(eh=ScheduledTasksErrorHandler):
    count = 0
    eh.logger.info(f'Inactive Products: Starting at {datetime.now():%H:%M:%S}')
    active_products = get_active_products_with_no_stock()
    if active_products is not None:
        # Update: Items on open order are not taken into account as of April 24
        # order_products = get_products_on_open_order()
        bonnie_products = get_bonnie_items()
        # Update: Hold Products are not taken into account as of April 24
        # hold_quote_products = get_products_on_quotes_or_holds()
        for x in active_products:
            item = Product(x)
            # If item is on open order, hold, or quote, skip this iteration
            # if item_number in order_products or item_number in hold_quote_products:
            # if item.item_no in order_products:
            #     print(f"Skipping {item.item_no}: {item.long_descr} - On Open Order")
            #     continue
            if item.item_no in bonnie_products:
                eh.logger.info(f'Skipping {item.item_no}: {item.long_descr} - Bonnie Product')
                continue
            else:
                try:
                    db.CP.Product.set_inactive(item.item_no, eh=eh)
                except:
                    continue
                else:
                    count += 1

    eh.logger.info(f'{count} product statuses changed. Process completed at {datetime.now():%H:%M:%S}.')


if __name__ == '__main__':
    set_products_to_inactive()
