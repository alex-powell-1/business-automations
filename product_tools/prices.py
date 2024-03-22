import time

import pyodbc

from product_tools.products import *

# This module allows you to set percentage based discounts per category and to null out
# sale prices once sales are over.


def set_price_two_null(category):
    """Nulls price 2 (used for sale prices) and updated last maintained date"""
    category_items = get_products_by_category(category)
    for x in category_items:
        query = f"""
        UPDATE IM_PRC
        SET PRC_2 = NULL, LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{x}'

        UPDATE IM_ITEM
        SET LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{x}'
        """
        db.query_db(query, commit=True)
        print(f"updated {x}")


def set_sale_price_by_category(category, percentage_discount):
    """Sets sale price for prc_2 and updated last maintained date"""
    category_items = get_products_by_category(category)

    for x in category_items:
        item = Product(x)
        item_sale_price = round(float(item.price_1) * ((100 - percentage_discount) / 100), 2)
        query = f"""
        UPDATE IM_PRC
        SET PRC_2 = '{item_sale_price}', LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{x}'

        UPDATE IM_ITEM
        SET LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{x}'
        """
        db.query_db(query, commit=True)
        print(f"updated {x}")


def set_reg_price_by_category(category, percentage_discount):
    """Updates reg_prc and updated last maintained date"""
    category_items = get_products_by_category(category)
    counter = 1
    for x in category_items:
        try:
            item = Product(x)
            item_reg_price = round(float(item.price_1) * ((100 - percentage_discount) / 100), 2)
            query = f"""
            UPDATE IM_PRC
            SET REG_PRC = '{item_reg_price}'
            WHERE ITEM_NO = '{x}'
    
            UPDATE IM_ITEM
            SET REG_PRC = '{item_reg_price}'
            WHERE ITEM_NO = '{x}'
            """
            db.query_db(query, commit=True)
            print(f"#{counter}/{len(category_items)}: updated {x}")
            counter
        except pyodbc.Error:
            time.sleep(3)
            db.query_db(query, commit=True)
        finally:
            counter += 1



