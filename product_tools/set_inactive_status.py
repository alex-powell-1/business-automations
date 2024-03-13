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
    response = db.query_db(query)
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
    response = db.query_db(query)
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
    print(f"Setting inactive products: Starting\n")
    active_products = get_active_products_with_no_stock()
    if active_products is not None:
        order_products = get_products_on_open_order()
        bonnie_products = get_bonnie_items()
        # hold_quote_products = get_products_on_quotes_or_holds()
        for x in active_products:
            item = Product(x)
            # If item is on open order, hold, or quote, skip this iteration
            # if item_number in order_products or item_number in hold_quote_products:
            if item.item_no in order_products:
                print(f"Skipping {item.item_no}: {item.long_descr} - On Open Order")
                continue
            elif item.item_no in bonnie_products:
                print(f"Skipping {item.item_no}: {item.long_descr} - Bonnie Product")
                continue
            else:
                print(f"Setting {item.item_no}: {item.web_title} to inactive")
                set_inactive(item.item_no)
                item = Product(item.item_no)
                create_product_log(item_no=item.item_no,
                                   product_name=item.long_descr,
                                   qty_avail=item.quantity_available,
                                   status_1_col_name="status",
                                   status_1_data=item.status,
                                   log_location=inactive_product_log)

    print(f"\nSetting inactive products: completed at {datetime.now()}")
