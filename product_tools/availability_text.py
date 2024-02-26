from setup.query_engine import QueryEngine
from datetime import datetime

db = QueryEngine()

text = "Ready for pickup in 1-2 business hours."


def set_availability_text():
    print("Setting Availability Text: Ready for pickup in 1-2 business hours.")
    query = f"""
    UPDATE IM_ITEM
    SET USR_PROF_ALPHA_20 = '{text}', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
    WHERE USR_PROF_ALPHA_20 IS NULL
    AND IS_ECOMM_ITEM = 'Y'
    """
    db.query_db(query, commit=True)
    print(f"Setting Availability Text: Completed at {datetime.now()}")


def get_availability_text(item_number):
    query = f"""
    SELECT USR_PROF_ALPHA_20
    FROM IM_ITEM
    WHERE ITEM_NO = '{item_number}'
    """
    response = db.query_db(query)
    if response is not None:
        return response[0][0]
    else:
        return "NULL"
