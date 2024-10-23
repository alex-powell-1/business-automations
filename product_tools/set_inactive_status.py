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
# - Product not in inactive_exclude
# - Product is not in category 'BONNIE'
#


def get_active_products_with_no_stock() -> list[str]:
    query = """
    SELECT item.ITEM_NO
    FROM IM_ITEM item
    INNER JOIN IM_INV inv on inv.ITEM_NO = item.ITEM_NO
    WHERE inv.QTY_AVAIL < 1 and item.STAT = 'A' AND item.CATEG_COD NOT IN ('SERVICES')
    ORDER BY inv.QTY_AVAIL DESC
    """
    response = db.query(query)
    return [x[0] for x in response] if response is not None else []


def get_products_on_open_order() -> list[str]:
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


def get_bonnie_items() -> list[str]:
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


def get_products_on_open_documents() -> list[str]:
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


def set_products_to_inactive(eh=ScheduledTasksErrorHandler):
    count = 0
    eh.logger.info(f'Inactive Products: Starting at {datetime.now():%H:%M:%S}')
    active_products = get_active_products_with_no_stock()
    if not active_products:
        eh.logger.info('No products found with no stock.')
        return

    # Get Items that are not checked track inventory at 10X tab
    non_tracked_items = []

    query = """
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE TRK_INV = 'N'
    """
    response = db.query(query, mapped=True)
    if response['code'] == 200:
        non_tracked_items = [x[0] for x in response]

    for x in active_products:
        item = Product(x)
        if item.item_no in non_tracked_items or item.item_no in get_bonnie_items():
            eh.logger.info(f'Skipping {item.item_no}: {item.long_descr} - Excluded Product')
            continue
        else:
            try:
                db.CP.Product.set_inactive(item.item_no, eh=eh)
            except Exception as err:
                eh.error_handler.add_error_v(error=err, origin='Inactive Products')
            else:
                count += 1

    eh.logger.info(f'{count} product statuses changed. Process completed at {datetime.now():%H:%M:%S}.')


if __name__ == '__main__':
    pass