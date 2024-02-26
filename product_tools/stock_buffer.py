from setup.query_engine import QueryEngine
from datetime import datetime

db = QueryEngine()


def set_stock_buffer(buffer: int, db_filter: str, filter_input: str, only_nulls=False):
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
    # Vendor Updates
    vendor_dict = {
        "EVERGREEN": 0
    }

    for k, v in vendor_dict.items():
        set_stock_buffer(v, "ITEM_VEND_NO", k)
