from setup.query_engine import QueryEngine
from product_tools.products import get_products_by_category, Product
from datetime import datetime
from setup import creds

db = QueryEngine()


def set_stock_buffer_by_vendor(buffer: int, db_filter: str, filter_input: str, only_nulls=False):
    # WORK ON GETTING THIS TO ONLY AFFECT ITEMS WITH NO STOCK BUFFER ALREADY SET BY DEFAULT

    if only_nulls:
        nulls = "AND PROF_NO_1 IS NULL"
    else:
        nulls = ""

    query = f"""
    UPDATE IM_ITEM
    SET PROF_NO_1 = '{buffer}', LST_MAINT_DT = GETDATE()
    WHERE {db_filter} = '{filter_input}' {nulls}
    """
    db.query_db(query, commit=True)


def set_stock_buffer(category, log_file, base_buffer=3):
    """Sets stock buffers based on 'price 1' thresholds defined in dictionary in creds"""
    product_list = get_products_by_category(category, ecomm_only=True)
    buffer_bank = creds.buffer_bank[category]
    for x in product_list:
        item = Product(x)
        # Highest Priced Tier
        if item.price_1 > buffer_bank['tier_2']['price']:
            item.set_buffer(buffer_bank['tier_2']['buffer'], log_file)
        # Middle Tier
        elif item.price_1 > buffer_bank['tier_1']['price']:
            item.set_buffer(buffer_bank['tier_1']['buffer'], log_file)
        # Lowest Tier
        else:
            item.set_buffer(buffer_bank['tier_0']['buffer'], log_file)


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


def stock_buffer_updates(log_file):
    print(f"Setting Stock Buffers: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    # Vendor Updates
    vendor_dict = {
        "EVERGREEN": 0
    }
    for k, v in vendor_dict.items():
        set_stock_buffer_by_vendor(buffer=v,
                                   db_filter="ITEM_VEND_NO",
                                   filter_input=k,
                                   only_nulls=True)
    print("Vendor Updates Complete", file=log_file)
    # Category Updates
    for k in creds.buffer_bank:
        print(f"Setting Category Updates for {k}", file=log_file)
        set_stock_buffer(k, log_file=log_file)

    print(f"Setting Stock Buffers: Complete at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
