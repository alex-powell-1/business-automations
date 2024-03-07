from datetime import datetime
from setup.query_engine import QueryEngine
from setup.create_log import create_product_log
from setup.creds import inactive_product_log
from product_tools.products import Product
db = QueryEngine()


def set_inactive(item_number):
    query = f"""
    UPDATE IM_ITEM 
    SET STAT = 'V'
    WHERE ITEM_NO = '{item_number}'
    """
    db.query_db(query, commit=True)


def get_active_products_with_no_stock():
    query = """
    SELECT item.ITEM_NO
    FROM IM_ITEM item
    INNER JOIN IM_INV inv on inv.ITEM_NO = item.ITEM_NO
    WHERE inv.QTY_AVAIL < 1 and item.STAT = 'A' AND item.CATEG_COD NOT IN ('SERVICES')
    ORDER BY inv.QTY_AVAIL DESC
    """
    active_products = db.query_db(query)

    return active_products


def get_products_on_open_order():
    query = """
    SELECT DISTINCT ITEM_NO
    FROM PS_DOC_LIN
    WHERE LIN_TYP = 'O'
    """
    response = db.query_db(query)
    item_list = []
    if response is not None:
        for x in response:
            item = x[0]
            item_list.append(item)
        return item_list
    else:
        return


def get_products_on_quotes_or_holds():
    query = """
    SELECT DISTINCT ITEM_NO
    FROM PS_DOC_LIN
    WHERE LIN_TYP = 'S'
    """
    response = db.query_db(query)
    item_list = []
    if response is not None:
        for x in response:
            item = x[0]
            item_list.append(item)
        return item_list
    else:
        return


def set_products_to_inactive():
    print(f"Setting inactive products: Starting")
    active_products = get_active_products_with_no_stock()
    if active_products is not None:
        order_products = get_products_on_open_order()
        # hold_quote_products = get_products_on_quotes_or_holds()
        for x in active_products:
            item_number = x[0]
            # If item is on open order, hold, or quote, skip this iteration
            # if item_number in order_products or item_number in hold_quote_products:
            if item_number in order_products:
                continue
            else:
                set_inactive(item_number)
                item = Product(item_number)
                create_product_log(item_no=item.item_no,
                                   product_name=item.long_descr,
                                   qty_avail=item.quantity_available,
                                   status_1_col_name="status",
                                   status_1_data=item.status,
                                   log_location=inactive_product_log)

    print(f"Setting inactive products: completed at {datetime.now()}")
