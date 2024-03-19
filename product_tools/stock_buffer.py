from setup.query_engine import QueryEngine
from product_tools.products import get_products_by_category, Product
from datetime import datetime
from setup import creds

db = QueryEngine()


def set_stock_buffer_by_vendor(buffer: int, db_filter: str, filter_input: str, only_nulls=False):
    if only_nulls:
        nulls = "AND PROF_NO_1 IS NULL"
    else:
        nulls = ""

    query = f"""
    UPDATE IM_ITEM
    SET PROF_NO_1 = '{buffer}', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
    WHERE {db_filter} = '{filter_input}' {nulls}
    """
    db.query_db(query, commit=True)


def set_stock_buffer(category, base_buffer=3):
    """Sets stock buffers based on 'price 1' thresholds defined in dictionary in creds"""
    product_list = get_products_by_category(category, ecomm_only=True)
    buffer_bank = creds.buffer_bank[category]
    for x in product_list:
        item = Product(x)
        if item.price_1 > buffer_bank['tier_2']['price']:
            item.set_buffer(buffer_bank['tier_2']['buffer'])
        elif item.price_1 > buffer_bank['tier_1']['price']:
            item.set_buffer(buffer_bank['tier_1']['buffer'])
        else:
            item.set_buffer(base_buffer)


def get_stock_buffer(item_number):
    query = f"""
    SELECT PROF_NO_1
    FROM IM_ITEM
    WHERE ITEM_NO = '{item_number}'
    """

    response = db.query_db(query)
    if response is not None:
        return response[0][0]
    else:
        return "NULL"


def stock_buffer_updates():
    print("-------------")
    print("Stock Buffers")
    print("-------------")
    print(f"Stock Buffers: starting at {datetime.now()}")
    # Vendor Updates
    vendor_dict = {
        "EVERGREEN": 0
    }
    for k, v in vendor_dict.items():
        set_stock_buffer_by_vendor(v, "ITEM_VEND_NO", k)

    # Category Updates
    for k in creds.buffer_bank:
        set_stock_buffer(k)

    print(f"Stock Buffers: complete at {datetime.now()}\n")
