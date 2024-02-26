from setup.query_engine import QueryEngine
from setup.date_presets import *
from setup.create_log import *
from setup import creds

db = QueryEngine()


def get_ecomm_flags(item_number):
    query = f"""
        SELECT IS_ECOMM_ITEM, USR_CPC_IS_ENABLED 
        FROM IM_ITEM
        WHERE ITEM_NO = '{item_number}'
        """
    response = db.query_db(query)
    if response is not None:
        web_enabled = response[0][0]
        web_visible = response[0][1]
        return web_enabled, web_visible
    else:
        return "Invalid Item", "No Data"


def get_products_for_removal():
    # Gets Products that are web-enabled, have no stock, are not top performers, haven't sold in at least a year
    # and are not parent items and have no binding ID.
    query = f"""
    SELECT ITEM.ITEM_NO, ITEM.LONG_DESCR, ITEM.STAT, INV.QTY_AVAIL, INV.LST_SAL_DAT
    FROM IM_ITEM ITEM
    INNER JOIN IM_INV INV on inv.ITEM_NO = item.ITEM_NO
    WHERE IS_ECOMM_ITEM = 'Y' and USR_ALWAYS_ONLINE = 'N' and QTY_AVAIL < 1 and LST_SAL_DAT < '{two_years_ago}' AND
    USR_PROF_ALPHA_16 IS NULL AND IS_ADM_TKT = 'N'
    ORDER BY LST_SAL_DAT
    """
    response = db.query_db(query)
    if response is not None:
        result = []
        for x in response:
            item_number = x[0]
            descr = x[1]
            status = x[2]
            qty = x[3]
            last_sale_date = x[4]
            result.append([item_number, descr, status, qty, last_sale_date])
        return result
    else:
        return None


def remove_web_enabled_flags():
    items = get_products_for_removal()
    if items is not None:
        for x in items:
            item_number = x[0]
            descr = x[1]
            qty = x[3]
            last_sale_date = x[4]
            set_web_enabled(item_number, flag='N')
            status = get_ecomm_flags(item_number)[0]
            create_product_log(item_number, descr, qty,
                               "web_enabled", "lst_sal_dat",
                               status, last_sale_date, creds.e_comm_flag_product_log)
            print(f"Removed Web Enabled for {item_number}")
    else:
        print("No E-commerce Flags to Remove")
        return


def set_web_enabled(item_number, flag='Y'):
    query = f"""
    UPDATE IM_ITEM 
    SET IS_ECOMM_ITEM = '{flag}', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
    WHERE ITEM_NO = '{item_number}'
    """
    db.query_db(query, commit=True)


def set_web_visible(item_number, flag='Y'):
    query = f"""
    UPDATE IM_ITEM 
    SET USR_CPC_IS_ENABLED = '{flag}', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
    WHERE ITEM_NO = '{item_number}'
    """
    db.query_db(query, commit=True)


def get_non_web_enabled_active_products():
    query = """
    SELECT item.ITEM_NO, item.LONG_DESCR, inv.QTY_AVAIL
    FROM IM_ITEM item
    INNER JOIN IM_INV inv on inv.ITEM_NO = item.ITEM_NO
    WHERE inv.QTY_AVAIL > 0 and item.STAT = 'A' AND 
    item.CATEG_COD NOT IN ('SERVICES') AND ADDL_DESCR_1 != 'EXCLUDE'
    AND (IS_ECOMM_ITEM = 'N' OR USR_CPC_IS_ENABLED = 'N')
    ORDER BY inv.QTY_AVAIL DESC
    """
    response = db.query_db(query)
    if response is not None:
        result = []
        for x in response:
            item_number = x[0]
            long_descr = x[1]
            qty_avail = int(x[2])
            result.append([item_number, long_descr, qty_avail])
        return result
    else:
        return None


def add_ecomm_flags():
    products = get_non_web_enabled_active_products()
    if products is not None:
        for x in products:
            item_number = x[0]
            long_descr = x[1]
            qty_avail = x[2]
            old_web_enabled, old_web_visible = get_ecomm_flags(item_number)
            set_web_enabled(item_number)
            set_web_visible(item_number)
            new_web_enabled, new_web_visible = get_ecomm_flags(item_number)
            if old_web_enabled == new_web_enabled:
                web_enabled_message = "No Change"
            else:
                web_enabled_message = f"From {old_web_enabled} to {new_web_enabled}"

            if old_web_visible == new_web_visible:
                web_visible_message = "No Change"
            else:
                web_visible_message = f"From {old_web_visible} to {new_web_visible}"

            create_product_log(item_number, long_descr, qty_avail,
                               "web_enabled", "web_visible",
                               web_enabled_message, web_visible_message, creds.e_comm_flag_product_log)

    else:
        print("No E-Commerce Flags to Add")
        return


def set_ecommerce_flags():
    print("Setting E-Commerce Flags")
    remove_web_enabled_flags()
    add_ecomm_flags()
