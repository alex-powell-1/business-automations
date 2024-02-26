from setup.query_engine import QueryEngine
from setup import creds
from datetime import datetime

db = QueryEngine()


def set_company_brand():
    print(f"Set Company Brand: Starting")
    query = f"""
    UPDATE IM_ITEM
    SET PROF_COD_1 = '{creds.db_brand}', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
    WHERE PROF_COD_1 IS NULL
    """
    db.query_db(query, commit=True)
    print(f"Set Company Brand: Completed at {datetime.now()}")


def set_product_brand():
    brand_list = creds.brand_list

    for k, v in brand_list.items():
        query = f"""
        UPDATE IM_ITEM
        SET PROF_COD_1 = '{v}', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
        WHERE LONG_DESCR like '%{k}%'
        """
        db.query_db(query, commit=True)


def get_branded_products(brand):
    """Returns a list of items that have major brand names in description"""
    query = f"""
        SELECT ITEM_NO 
        FROM IM_ITEM
        WHERE LONG_DESCR like '%{brand.title()}%'
        """
    response = db.query_db(query)
    if response is not None:
        result = []
        for x in response:
            item_no = x[0]
            result.append(item_no)
        return result
    else:
        return None


def get_product_brand(item_number):
    query = f"""
    SELECT PROF_NO_1
    FROM IM_ITEM
    WHERE ITEM_NO = '{item_number}'
    """

    response = db.query_db(query)
    if response is not None:
        return response[0][0]
    else:
        return None


def update_brands():
    set_company_brand()
    set_product_brand()
