import time

import pyodbc

from product_tools.products import *


# This module allows you to set percentage based discounts per category and to null out
# sale prices once sales are over.


def remove_sale_price(category=None):
    """Nulls price 2 (used for sale prices) and updated last maintained date"""
    # If category is provided, get all items in that category
    if category:
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
            db.query(query, commit=True)
            print(f'updated {x}')
    else:
        # Otherwise, get all items with a sale price
        query = """
        SELECT ITEM_NO
        FROM IM_PRC
        WHERE PRC_2 IS NOT NULL
        """
        response = db.query(query)
        items = [x[0] for x in response] if response else []
        for x in items:
            query = f"""
            UPDATE IM_PRC
            SET PRC_2 = NULL, LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{x}'

            UPDATE IM_ITEM
            SET LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{x}'
            """
            db.query(query, commit=True)
            print(f'updated {x}')


# remove_sale_price()


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
        db.query(query, commit=True)
        print(f'updated {x}')


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
            db.query(query, commit=True)
            print(f'#{counter}/{len(category_items)}: updated {x}')
            counter += 1
        except pyodbc.Error:
            time.sleep(3)
            db.query(query, commit=True)
        finally:
            counter += 1


def memorial_day_sale() -> None:
    # Counter is for seq_number in the EC_CATEG_ITEM table
    counter = 2

    # ---------------------
    # Set 1 is all active e-commerce items except roses and hanging baskets and annuals
    # ---------------------

    set_1_query = """
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE IS_ECOMM_ITEM = 'Y' AND STAT = 'A' and
    (SUBCAT_COD != 'ROSE' or SUBCAT_COD IS NULL) and
    LONG_DESCR not like '%hanging basket%' and
    CATEG_COD != 'annual'"""
    response = db.query(set_1_query)
    set_1 = [x[0] for x in response] if response else []

    set_1_discount = 10

    print('Setting PRC_2 for to 10% off for the store - exclusions\n')

    for x in set_1:
        item = Product(x)
        item_sale_price = round(float(item.price_1) * ((100 - set_1_discount) / 100), 2)
        # query = f"""
        # UPDATE IM_PRC
        # SET PRC_2 = '{item_sale_price}', LST_MAINT_DT = GETDATE()
        # WHERE ITEM_NO = '{x}'
        #
        # UPDATE IM_ITEM
        # SET LST_MAINT_DT = GETDATE()
        # WHERE ITEM_NO = '{x}'
        #
        # INSERT INTO EC_CATEG_ITEM(ITEM_NO, CATEG_ID, ENTRY_SEQ_NO, LST_MAINT_DT, LST_MAINT_USR_ID)
        # VALUES('{x}', '{creds.on_sale_category}', '{counter}', GETDATE(), 'AP')
        # """
        query = f"""
                UPDATE IM_PRC
                SET PRC_2 = '{item_sale_price}', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO = '{x}'

                UPDATE IM_ITEM
                SET LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO = '{x}'
                """
        try:
            db.query(query, commit=True)
        except pyodbc.Error:
            time.sleep(1)
            db.query(query, commit=True)
        print(f'Set Sale Price for {item.long_descr}{item.item_no} to {item_sale_price}')
        counter += 1

    print('\nSale Prices Set for store - exclusions\n')

    # ---------------------
    # Set 2 is all active e-commerce items in the annual category, except hanging baskets
    # ---------------------
    set_2_query = """
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE IS_ECOMM_ITEM = 'y' AND STAT = 'a' and
    CATEG_COD = 'annual' and LONG_DESCR not like '%hanging basket%'"""
    response = db.query(set_2_query)
    set_2 = [x[0] for x in response] if response else []

    set_2_discount = 30

    print('Setting PRC_2 for to 30% off annuals - hanging baskets\n')

    for x in set_2:
        item = Product(x)
        item_sale_price = round(float(item.price_1) * ((100 - set_2_discount) / 100), 2)
        query = f"""
            UPDATE IM_PRC
            SET PRC_2 = '{item_sale_price}', LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{x}'

            UPDATE IM_ITEM
            SET LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{x}'
            
            INSERT INTO EC_CATEG_ITEM(ITEM_NO, CATEG_ID, ENTRY_SEQ_NO, LST_MAINT_DT, LST_MAINT_USR_ID)
            VALUES('{x}', '{creds.on_sale_category}', '{counter}', GETDATE(), 'AP')
            """
        try:
            db.query(query, commit=True)
        except pyodbc.Error:
            time.sleep(1)
            db.query(query, commit=True)
        print(f'Set Sale Price for {item.long_descr}{item.item_no} to {item_sale_price}')
        counter += 1

    print('\nSale Prices Set for Annuals - hanging baskets 1\n')


def remove_item_from_on_sale_section():
    query = f"""
    select ITEM_NO from EC_CATEG_ITEM
    WHERE CATEG_ID = '{creds.on_sale_category}'
    """
    response = db.query(query)
    items = [x[0] for x in response] if response else []

    for x in items:
        item = Product(x)
        if item.price_2 is None:
            query = f"""
            DELETE FROM EC_CATEG_ITEM
            WHERE ITEM_NO = '{x}' AND CATEG_ID = '{creds.on_sale_category}'"""
            db.query(query, commit=True)
            print(f'Removed {item.long_descr} from on sale section')
