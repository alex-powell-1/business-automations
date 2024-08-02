from setup.query_engine import QueryEngine as db
from setup import creds
from datetime import datetime
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


def set_product_brand():
    """Sets products with specific keywords to specific brands in the event that team members do not include a brand"""
    brand_list = creds.brand_list
    for k, v in brand_list.items():
        error_handler.logger.info(f'Setting {k} to {v}')
        query = f"""
        UPDATE IM_ITEM
        SET PROF_COD_1 = '{v}', LST_MAINT_DT = GETDATE()
        WHERE LONG_DESCR like '%{k}%' and PROF_COD_1 IS NULL
        """
        db.query(query, commit=True)


def set_company_brand():
    query = f"""
    UPDATE IM_ITEM
    SET PROF_COD_1 = '{creds.company_product_brand}', LST_MAINT_DT = GETDATE()
    WHERE PROF_COD_1 IS NULL
    """
    db.query(query, commit=True)
    error_handler.logger.info(f'Set null brand fields to {creds.company_product_brand}')


def get_branded_products(brand):
    """Returns a list of items that have major brand names in description"""
    query = f"""
        SELECT ITEM_NO 
        FROM IM_ITEM
        WHERE LONG_DESCR like '%{brand.title()}%'
        """
    response = db.query(query)
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

    response = db.query(query)
    if response is not None:
        return response[0][0]
    else:
        return None


def update_brands():
    error_handler.logger.info(f'Updating Product Brands: Starting at {datetime.now():%H:%M:%S}')
    set_product_brand()
    set_company_brand()
    error_handler.logger.info(f'Updating Product Brands: Completed at {datetime.now():%H:%M:%S}')
