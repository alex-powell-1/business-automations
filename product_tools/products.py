import re

from big_commerce import big_products
from setup import creds
from setup import date_presets
from setup.create_log import *
from setup.query_engine import QueryEngine

db = QueryEngine()


class Product:
    def __init__(self, item_number):
        self.item_no = item_number
        self.binding_key = ""
        self.variant_name = ""
        self.product_id = ""
        self.variant_id = ""
        self.is_parent = ""
        self.descr = ""
        self.long_descr = ""
        self.brand = ""
        self.price_1 = 0
        self.price_2 = 0
        self.reg_price = 0
        self.quantity_available = 0
        self.buffer = 0
        self.buffered_quantity_available = self.quantity_available - self.buffer
        self.web_enabled = ""
        self.web_visible = ""
        self.always_online = ""
        self.gift_wrap = ""
        self.in_store_only = ""
        self.web_title = ""
        self.meta_title = ""
        self.meta_description = ""
        self.item_type = ""
        self.parent_category = ""
        self.sub_category = ""
        self.status = ""
        self.vendor = ""
        self.custom_field_botanical_name = ""
        self.custom_field_climate_zone = ""
        self.custom_field_plant_type = ""
        self.custom_field_type = ""
        self.custom_field_height = ""
        self.custom_field_width = ""
        self.custom_field_sun_exposure = ""
        self.custom_field_bloom_time = ""
        self.custom_field_flower_color = ""
        self.custom_field_attracts_pollinators = ""
        self.custom_field_growth_rate = ""
        self.custom_field_deer_resistant = ""
        self.custom_field_soil_type = ""
        self.custom_field_color = ""
        self.custom_field_size = ""
        self.search_key = ""
        self.sort_order = 0
        self.item_url = ""
        self.preorder_message = ""
        self.availability_description = ""
        self.e_comm_category = ""
        self.web_description = ""
        self.featured = ""
        self.get_product_details()

    def get_product_details(self):
        query = f"""
        select ITEM.ITEM_NO, ITEM.USR_PROF_ALPHA_16, ITEM.USR_PROF_ALPHA_17, ITEM.IS_ADM_TKT, ITEM.DESCR, 
        ITEM.LONG_DESCR, ITEM.PROF_COD_1, ITEM.PRC_1, PRC.PRC_2, ITEM.REG_PRC, ISNULL(INV.QTY_AVAIL, 0), 
        ISNULL(ITEM.PROF_NO_1, 0), ITEM.IS_ECOMM_ITEM, ITEM.USR_CPC_IS_ENABLED, ITEM.USR_ALWAYS_ONLINE,
        ITEM.IS_FOOD_STMP_ITEM, ITEM.USR_IN_STORE_ONLY, ITEM.ADDL_DESCR_1, ITEM.ADDL_DESCR_2, ITEM.USR_PROF_ALPHA_21, 
        ITEM.ITEM_TYP,ITEM.CATEG_COD, ITEM.SUBCAT_COD, ITEM.STAT, ITEM.VEND_ITEM_NO,ITEM.PROF_ALPHA_1,ITEM.PROF_ALPHA_2,  
        ITEM.PROF_ALPHA_3,  ITEM.PROF_ALPHA_4,  ITEM.PROF_ALPHA_5, ITEM.USR_PROF_ALPHA_6, ITEM.USR_PROF_ALPHA_7, 
        ITEM.USR_PROF_ALPHA_8,  ITEM.USR_PROF_ALPHA_9,  ITEM.USR_PROF_ALPHA_10,  ITEM.USR_PROF_ALPHA_11,
        ITEM.USR_PROF_ALPHA_12, ITEM.USR_PROF_ALPHA_13, ITEM.USR_PROF_ALPHA_14, ITEM.USR_PROF_ALPHA_15, 
        USR_PROF_ALPHA_26, USR_PROF_ALPHA_27, USR_PROF_ALPHA_18, USR_PROF_ALPHA_19, USR_PROF_ALPHA_20, 
        EC_CATEG.DESCR, EC_ITEM_DESCR.HTML_DESCR, ITEM.ECOMM_NEW
        FROM IM_ITEM ITEM
        INNER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
        LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
        LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
        LEFT OUTER JOIN EC_CATEG_ITEM ON ITEM.ITEM_NO=EC_CATEG_ITEM.ITEM_NO
        LEFT OUTER JOIN EC_CATEG ON EC_CATEG.CATEG_ID=EC_CATEG_ITEM.CATEG_ID
        WHERE ITEM.ITEM_NO = '{self.item_no}'
        """
        response = db.query_db(query)
        if response is not None:
            for x in response:
                self.item_no = x[0]
                self.binding_key = x[1]
                self.variant_name = x[2]
                self.is_parent = x[3]
                self.descr = x[4]
                self.long_descr = x[5]
                self.brand = x[6]
                self.price_1 = x[7]
                self.price_2 = x[8]
                self.reg_price = x[9]
                self.quantity_available = int(x[10])
                self.buffer = int(x[11])
                if self.quantity_available - self.buffer < 0:
                    self.buffered_quantity_available = 0
                else:
                    self.buffered_quantity_available = self.quantity_available - self.buffer
                self.web_enabled = x[12]
                self.web_visible = x[13]
                self.always_online = x[14]
                self.gift_wrap = x[15]
                self.in_store_only = x[16]
                self.web_title = x[17]
                self.meta_title = x[18]
                self.meta_description = x[19]
                self.item_type = x[20]
                self.parent_category = x[21]
                self.sub_category = x[22]
                self.status = x[23]
                self.vendor = x[24]
                self.custom_field_botanical_name = x[25]
                self.custom_field_climate_zone = x[26]
                self.custom_field_plant_type = x[27]
                self.custom_field_type = x[28]
                self.custom_field_height = x[29]
                self.custom_field_width = x[30]
                self.custom_field_sun_exposure = x[31]
                self.custom_field_bloom_time = x[32]
                self.custom_field_flower_color = x[33]
                self.custom_field_attracts_pollinators = x[34]
                self.custom_field_growth_rate = x[35]
                self.custom_field_deer_resistant = x[36]
                self.custom_field_soil_type = x[37]
                self.custom_field_color = x[38]
                self.custom_field_size = x[39]
                self.search_key = x[40]
                if x[41] is not None:
                    self.sort_order = int(x[41])
                self.preorder_message = x[43]
                self.availability_description = x[44]
                self.e_comm_category = x[45]
                self.web_description = x[46]
                self.featured = x[47]
                self.product_id = self.get_product_id()
                self.variant_id = self.get_variant_id()

        else:
            return "No Item Matching that SKU"

    def get_child_products(self):
        if self.binding_key is not None:
            if self.is_parent == 'Y':
                query = f"""
                SELECT ITEM_NO
                FROM IM_ITEM
                WHERE USR_PROF_ALPHA_16 = '{self.binding_key}' AND IS_ADM_TKT = 'N'
                ORDER BY PRC_1
                """
                response = db.query_db(query)
                if response is not None:
                    child_products = []
                    for x in response:
                        child_products.append(x[0])
                    return child_products

    def get_child_product_info(self, bc=True):
        if self.binding_key is not None:
            if self.is_parent == 'Y':
                child_products = get_all_child_products(self.binding_key)
                if child_products is not None:
                    child_info = ""
                    for x in child_products:
                        item = Product(x)
                        # if mode is bc, perform API call and get info from Big Commerce
                        if bc:
                            info = big_products.bc_get_variant(item.product_id, item.variant_id)
                            child_info += info
                        # else get information from Counterpoint
                        else:
                            for k, v in item.__dict__.items():
                                child_info += f"{k}: {v}\n"
                            child_info += "\n\n"

                    return child_info
            else:
                return "Not a parent product"
        else:
            return "Not a bound product"

    def get_product_id(self):
        if self.binding_key is not None:
            query = f"""
            SELECT TOP 1 PRODUCT_ID FROM CPI_BC_PRODUCTS
            WHERE ITEM_NO = '{self.binding_key}' AND WEB_ID = '1'
            ORDER BY CREATE_DATE DESC"""
        else:
            query = f"""
            SELECT TOP 1 PRODUCT_ID FROM CPI_BC_PRODUCTS
            WHERE ITEM_NO = '{self.item_no}' AND WEB_ID = '1'
            ORDER BY CREATE_DATE DESC"""
        response = db.query_db(query)
        if response is not None:
            return response[0][0]

    def get_variant_id(self):
        query = f"""
        SELECT VARIANT_ID
        FROM CPI_BC_PROD
        WHERE SKU = '{self.item_no}' AND WEB_ID = '1'
        """
        if self.binding_key is not None:
            response = db.query_db(query)
            if response is not None:
                return response[0][0]

    def get_top_child_product(self):
        """Get Top Performing child product of merged product (by sales in last year window)"""
        from reporting.product_reports import create_top_items_report
        children = create_top_items_report(beginning_date=date_presets.one_year_ago,
                                           ending_date=date_presets.last_year_forecast,
                                           merged=True,
                                           binding_id=self.binding_key,
                                           number_of_items=1,
                                           return_format=3)
        if children is not None:
            top_child = children[0]
            return top_child

    def set_buffer(self, buffer, log_file):
        """Set stock buffer for e-commerce purposes."""
        initial_buffer = self.buffer
        if buffer == initial_buffer:
            # print(f"Buffer for item: {self.item_no} - {self.long_descr} already at {self.buffer}", file=log_file)
            return
        else:
            query = f"""
            UPDATE IM_ITEM
            SET PROF_NO_1 = '{buffer}', LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{self.item_no}'"""
            # Update SQL Table
            db.query_db(query, commit=True)
            # Update Object Properties
            self.get_product_details()
            # Check for success
            if self.buffer == buffer:
                # Success!
                print(f"{self.item_no}: {self.long_descr} buffer changed from "
                      f"{initial_buffer} to {buffer}", file=log_file)
                # Write Success Log
                create_product_log(item_no=self.item_no,
                                   product_name=self.long_descr,
                                   qty_avail=self.quantity_available,
                                   status_1_col_name="buffer",
                                   status_1_data=self.buffer,
                                   status_2_col_name="Message",
                                   status_2_data=f"Item: {self.item_no} buffer updated from "
                                                 f"{initial_buffer} to {self.buffer}",
                                   log_location=creds.buffer_log)
                # If unsuccessful:
            else:
                print(f"{self.item_no}: {self.long_descr} failed to change sort order to {buffer}", file=log_file)
                # Write failure log
                create_product_log(item_no=self.item_no,
                                   product_name=self.long_descr,
                                   qty_avail=self.quantity_available,
                                   status_1_col_name="buffer",
                                   status_1_data=self.buffer,
                                   status_2_col_name="Message",
                                   status_2_data=f"Item: {self.item_no} buffer failed to update to {buffer}.",
                                   log_location=creds.buffer_log)

    def set_sort_order(self, log_file, target_sort_order=0):
        old_sort_order = self.sort_order
        # Check if item already has the correct sort order
        if old_sort_order == target_sort_order:
            print(f"{self.item_no}: {self.long_descr} sort order unchanged. Current Order: {self.sort_order}",
                  file=log_file)
            return
        # If not, change sort order to the target sort order
        else:
            query = f"""
            UPDATE IM_ITEM
            SET USR_PROF_ALPHA_27 = '{target_sort_order}', LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{self.item_no}'
            """
            try:
                db.query_db(query, commit=True)
            except Exception as err:
                print("UPDATE Error: Set Sort Order", file=log_file)
                print(err, file=log_file)
            else:
                self.get_product_details()
                # Check if write was successful
                if self.sort_order == target_sort_order:
                    # Success!
                    print(f"{self.item_no}: {self.long_descr} sort order changed from "
                          f"{old_sort_order} to {self.sort_order}", file=log_file)
                    # Write Success Log
                    create_product_log(item_no=self.item_no,
                                       product_name=self.long_descr,
                                       qty_avail=self.quantity_available,
                                       status_1_col_name="sort_order",
                                       status_1_data=self.sort_order,
                                       status_2_col_name="Message",
                                       status_2_data=f"Item: {self.item_no} sort order updated from "
                                                     f"{old_sort_order} to {self.sort_order}",
                                       log_location=creds.sort_order_log)
                # If unsuccessful:
                else:
                    print(f"{self.item_no}: {self.long_descr} failed to change sort order to {target_sort_order}",
                          file=log_file)
                    # Write failure log
                    create_product_log(item_no=self.item_no,
                                       product_name=self.long_descr,
                                       qty_avail=self.quantity_available,
                                       status_1_col_name="sort_order",
                                       status_1_data=self.sort_order,
                                       status_2_col_name="Message",
                                       status_2_data=f"Item: {self.item_no} sort order failed to update.",
                                       log_location=creds.sort_order_log)

    def set_featured(self, status, log_file):
        if self.binding_key is None:
            query = f"""
            UPDATE IM_ITEM
            SET ECOMM_NEW = '{status}', LST_MAINT_DT = GETDATE()
            WHERE ITEM_NO = '{self.item_no}'
            """
        else:
            query = f"""
            UPDATE IM_ITEM
            SET ECOMM_NEW = '{status}', LST_MAINT_DT = GETDATE()
            WHERE USR_PROF_ALPHA_16 = '{self.binding_key}' AND IS_ADM_TKT = 'Y'
            """
        db.query_db(query, commit=True)
        # Update the item details
        self.get_product_details()
        if status == 'Y':
            # Check if write was successful
            if self.featured == 'Y':
                print(f"Item: {self.item_no} updated to featured", file=log_file)
                # Write Success Log
                create_product_log(item_no=self.item_no,
                                   product_name=self.long_descr,
                                   qty_avail=self.quantity_available,
                                   status_1_col_name="featured",
                                   status_1_data=self.featured,
                                   status_2_col_name="Message",
                                   status_2_data=f"Item: {self.item_no} updated to featured",
                                   log_location=creds.featured_products)
            # If Unsuccessful
            else:
                print(f"Item: {self.item_no} failed to update to featured", file=log_file)
                # Write failure Log
                create_product_log(item_no=self.item_no,
                                   product_name=self.long_descr,
                                   qty_avail=self.quantity_available,
                                   status_1_col_name="featured",
                                   status_1_data=status,
                                   status_2_col_name="Message",
                                   status_2_data=f"Item: {self.item_no} failed to update to featured",
                                   log_location=creds.featured_products)
        else:
            if self.featured == 'N':
                print(f"Item: {self.item_no} updated to NOT featured", file=log_file)
                # Write Success Log
                create_product_log(item_no=self.item_no,
                                   product_name=self.long_descr,
                                   qty_avail=self.quantity_available,
                                   status_1_col_name="featured",
                                   status_1_data=self.featured,
                                   status_2_col_name="Message",
                                   status_2_data=f"Item: {self.item_no} updated to featured",
                                   log_location=creds.featured_products)

    def set_sale_price(self, discount):
        sale_price = round(float(self.price_1 * (100 - discount) / 100), 2)
        query = f"""
        UPDATE IM_PRC
        SET PRC_2 = '{sale_price}', LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{self.item_no}'
        """
        db.query_db(query, commit=True)
        print(f"updated {self.long_descr} from ${self.price_1} to ${sale_price}")

    def set_product_description(self, description, log_file):
        query = f"""
        INSERT INTO EC_ITEM_DESCR (ITEM_NO, HTML_DESCR,	LST_MAINT_DT, LST_MAINT_USR_ID)
        Values ('{self.item_no}', '{description}', GETDATE(), 'AP')
        """
        db.query_db(query, commit=True)
        print(f"Updated {self.item_no} description to {description}")
        print(f"Updated {self.item_no} description to {description}", file=log_file)

    def refresh_last_maintained_date(self, log_file):
        query = f"""
        UPDATE IM_ITEM
        SET LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{self.item_no}'
        """
        db.query_db(query, commit=True)
        print(f"Updated {self.item_no} LST_MAINT_DT")
        print(f"Updated {self.item_no} LST_MAINT_DT", file=log_file)


def get_ecomm_items_with_stock():
    query = """
    SELECT ITEM.ITEM_NO
    FROM IM_ITEM ITEM
    INNER JOIN IM_INV INV ON ITEM.ITEM_NO = INV.ITEM_NO
    WHERE INV.QTY_AVAIL - ITEM.PROF_NO_1 > 0
    """
    response = db.query_db(query)
    if response is not None:
        result = []
        for x in response:
            result.append(x[0])
        return result


def get_ecomm_items(mode=1):
    # Mode 1 returns a total count of all e-comm items
    if mode == 1:
        query = """
        SELECT COUNT(ITEM_NO)
        FROM IM_ITEM
        WHERE IS_ECOMM_ITEM = 'Y'
        """
        response = db.query_db(query)
        if response is not None:
            return response[0][0]
        else:
            return 0
    # Mode 2 returns a list of skus of all e-comm items
    if mode == 2:
        query = """
        SELECT ITEM_NO
        FROM IM_ITEM
        WHERE IS_ECOMM_ITEM = 'Y'
        """
        response = db.query_db(query)
        if response is not None:
            result = []
            for x in response:
                sku = x[0]
                result.append(sku)
            return result

    # Mode 3 returns a list of skus and bc product ID of single e-comm items, and unique binding keys
    if mode == 3:
        query = f"""
            SELECT ITEM_NO, PRODUCT_ID
            FROM CPI_BC_PRODUCTS
            WHERE WEB_ID = '1'
            """
        response = db.query_db(query)
        if response is not None:
            result = []
            for x in response:
                sku = x[0]
                product_id = int(x[1])
                result.append([sku, product_id])
            return result
        else:
            return None


def get_zero_stock_ecomm_products():
    query = f"""
    SELECT ITEM.ITEM_NO
    FROM IM_ITEM ITEM
    INNER JOIN IM_INV INV ON ITEM.ITEM_NO = INV.ITEM_NO
    WHERE ITEM.IS_ECOMM_ITEM = 'Y' AND (INV.QTY_AVAIL - ISNULL(ITEM.PROF_NO_1, 0)) < 1
    """
    response = db.query_db(query)
    if response is not None:
        result_list = []
        for x in response:
            result_list.append(x[0])
        return result_list


def get_ecomm_products_with_stock():
    query = f"""
    SELECT ITEM.ITEM_NO
    FROM IM_ITEM ITEM
    INNER JOIN IM_INV INV ON ITEM.ITEM_NO = INV.ITEM_NO
    WHERE ITEM.IS_ECOMM_ITEM = 'Y' AND (INV.QTY_AVAIL - ISNULL(ITEM.PROF_NO_1, 0)) > 0
    """
    response = db.query_db(query)
    if response is not None:
        result_list = []
        for x in response:
            result_list.append(x[0])
        return result_list


def get_variant_names(binding_id):
    query = f"""
    SELECT ITEM_NO, USR_PROF_ALPHA_17
    FROM IM_ITEM
    WHERE USR_PROF_ALPHA_16 = '{binding_id}'
    """
    response = db.query_db(query)
    result = []
    if response is not None:
        for x in response:
            item_number = x[0]
            variant_name = x[1]
            result.append([item_number, variant_name])
        return result
    else:
        return "No Variants with this ID"


def get_variant_info_from_big(sku):
    query = f"""
    SELECT PRODUCT_ID, VARIANT_ID
    FROM CPI_BC_PROD
    WHERE WEB_ID = '1' AND SKU = '{sku}'
    ORDER BY PRODUCT_ID
    """
    response = db.query_db(query)
    if response is not None:
        product_id = int(response[0][0])
        variant_id = int(response[0][1])
        return big_products.bc_get_variant(product_id, variant_id)


def get_binding_ids():
    query = f"""
    SELECT DISTINCT USR_PROF_ALPHA_16
    FROM IM_ITEM
    WHERE USR_PROF_ALPHA_16 IS NOT NULL
    ORDER BY USR_PROF_ALPHA_16
    """
    result = []
    response = db.query_db(query)
    if response is not None:
        for x in response:
            result.append(x[0])
        return result
    else:
        return


def get_parent_product(binding_id):
    query = f"""
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE USR_PROF_ALPHA_16 = '{binding_id}' AND IS_ADM_TKT = 'Y'
    """
    response = db.query_db(query)
    if response is not None:
        if len(response) > 1:
            result = []
            for x in response:
                result.append(x[0])
            return result
        else:
            return response[0][0]


def get_all_child_products(binding_id):
    """Returns a list of child product_tools for a binding ID"""
    query = f"""
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE USR_PROF_ALPHA_16 = '{binding_id}'    
    """
    response = db.query_db(query)
    if response is not None:
        child_products = []
        for x in response:
            child_products.append(x[0])
        return child_products


def get_merged_product_combined_stock(binding_id):
    """"""
    child_products = get_all_child_products(binding_id)
    if child_products is not None:
        if len(child_products) > 0:
            combined_stock = 0
            # Check each child for stock
            for x in child_products:
                item = Product(x)
                combined_stock += item.buffered_quantity_available
            return combined_stock
        else:
            return None
    else:
        return None


def get_items_with_no_sales_history():
    from reporting.product_reports import create_top_items_report
    all_ecomm_items = get_ecomm_items(mode=2)
    top_ecomm_items = create_top_items_report(
        beginning_date=date_presets.one_year_ago,
        ending_date=date_presets.last_year_forecast,
        mode="sales",
        number_of_items=get_ecomm_items(mode=1),
        return_format=3)
    result = []
    for x in all_ecomm_items:
        if x not in top_ecomm_items:
            result.append(x)
    return result


def get_new_items(start_date, end_date, min_price):
    query = f"""
    SELECT ITEM.ITEM_NO
    FROM PO_RECVR_HIST_LIN REC
    INNER JOIN IM_ITEM ITEM ON ITEM.ITEM_NO = REC.ITEM_NO
    WHERE RECVR_DAT >= '{start_date}' and RECVR_DAT <= '{end_date}' 
    AND ITEM.PRC_1 >= '{min_price}'
    ORDER BY RECVR_DAT DESC
    """
    response = db.query_db(query)
    if response is not None:
        result = []
        for x in response:
            result.append((x[0]))
        return result


def get_qty_sold_all_items():
    """Produces a list of all items with the total number of quantity sold"""
    query = f"""
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1 
    'select distinct IM_ITEM.ITEM_NO as GRP_ID, IM_ITEM.DESCR as GRP_DESCR 
    from IM_ITEM 
    where ( (1=1) ) 
    union select distinct VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, NULL as GRP_DESCR 
    from %HISTORY% where not exists(select 1 from IM_ITEM 
    where IM_ITEM.ITEM_NO = VI_PS_TKT_HIST_LIN.ITEM_NO) and ((VI_PS_TKT_HIST.STR_ID = ''1'')) and 
    ( (1=1) ) and %ANYPERIODFILTER%', 'select VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, %HISTCOLUMNS% 
    from %HISTORY% where ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) ) and %PERIODFILTER%', ' 
    (VI_PS_TKT_HIST.POST_DAT >= ''2020-01-01'') and (VI_PS_TKT_HIST.POST_DAT <= ''{date_presets.today}'')', ' 
    (1=0) ', ' (1=0) ', 0, 0, 'SLS_QTY_A - RTN_QTY_VALID_A - RTN_QTY_NONVALID_A', 2
    """
    response = db.query_db(query)
    if response is not None:
        item_dict = {}
        for x in response:
            item_dict[x[0]] = int(x[2])
        return item_dict


def update_total_sold(log_file):
    """Update Big Commerce with 'total_sold' amounts"""
    print(f"Update Total Sold on Big Commerce: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    ecomm_items = get_ecomm_items(mode=3)
    binding_ids = get_binding_ids()
    qty_sold_all_items = get_qty_sold_all_items()
    if ecomm_items is not None:
        count = 0
        for x in ecomm_items:
            count += 1
            sku = x[0]
            product_id = x[1]
            if sku in binding_ids:
                # Get all children
                child_skus = get_all_child_products(sku)

                # Calculate told sold all children
                total_sold_all_children = 0
                for y in child_skus:
                    if y in qty_sold_all_items:
                        total_sold_all_children += qty_sold_all_items[y]

                if total_sold_all_children > 0:
                    big_products.bc_update_product(product_id, {"total_sold": total_sold_all_children}, log_file)
                    print(f"#{count}/{len(ecomm_items)} Updated Item: {sku} to "
                          f"Total Sold: {total_sold_all_children}", file=log_file)

                    print(f"#{count}/{len(ecomm_items)} Updated Item: {sku} to "
                          f"Total Sold: {total_sold_all_children}")
                else:
                    print(f"#{count}/{len(ecomm_items)} Skipping Item: {sku} - Never Sold!", file=log_file)
                    print(f"#{count}/{len(ecomm_items)} Skipping Item: {sku} - Never Sold!")

            # This is for items without a valid binding key, i.e. Single Products
            else:
                if sku in qty_sold_all_items:
                    total_sold = qty_sold_all_items[sku]
                    if total_sold > 0:
                        big_products.bc_update_product(product_id, {"total_sold": total_sold}, log_file)
                        print(f"#{count}/{len(ecomm_items)} Updated Item: {sku} to Total Sold: {total_sold}",
                              file=log_file)
                        print(f"#{count}/{len(ecomm_items)} Updated Item: {sku} to Total Sold: {total_sold}")
                    else:
                        print(f"Skipping {sku}: Never Sold")

    print(f"Update Total Sold on Big Commerce: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)


def get_products_by_category(category, subcat="", ecomm_only=False):
    subcat_filter = ""
    ecomm_filter = ""
    if subcat != "":
        subcat_filter = f"AND SUBCAT_COD = '{subcat}'"
    if ecomm_only:
        ecomm_filter = "AND IS_ECOMM_ITEM = 'Y'"
    query = f"""
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE CATEG_COD = '{category}' {subcat_filter} {ecomm_filter}
    """
    response = db.query_db(query)
    if response is not None:
        items = []
        for x in response:
            items.append(x[0])
        return items


def get_products(query):
    """Generic Getter Method"""
    response = db.query_db(query)
    if response is not None:
        items = []
        for x in response:
            items.append(x[0])
        return items


def get_bc_product_id(sku):
    query = f"""
    SELECT TOP 1 PRODUCT_ID
    FROM CPI_BC_PROD
    WHERE WEB_ID = '1' AND SKU = '{sku}'
    ORDER BY LAST_MODIFIED DESC
    """
    response = db.query_db(query)
    if response is not None:
        return int(response[0][0])


def get_pottery_for_workshop(mode):
    query = f"""
    SELECT ITEM_NO
    FROM IM_ITEM
    WHERE CATEG_COD = 'POTTERY' AND IS_ECOMM_ITEM = 'Y' AND 
    ((USR_PROF_ALPHA_16 IS NOT NULL and IS_ADM_TKT = 'Y') OR
    (USR_PROF_ALPHA_16 IS NULL))"""
    response = db.query_db(query)
    if response is not None:
        result = []
        for x in response:
            if mode == "sku":
                result.append(x[0])
            else:
                result.append(get_bc_product_id(x[0]))
        return result


def get_product_categories_cp():
    query = f"""
    SELECT CATEG_COD
    FROM IM_CATEG_COD
    """
    response = db.query_db(query)
    if response is not None:
        categories = []
        for x in response:
            categories.append(x[0])
        return categories


def export_html_descr():
    log = creds.description_log

    query = f"""
    SELECT ec.ITEM_NO, ec.HTML_DESCR
    FROM EC_ITEM_DESCR ec
    inner join im_ITEM item on ec.item_no=item.item_no
    WHERE ec.HTML_DESCR IS NOT NULL AND item.CATEG_COD = 'POTTERY'
    """

    response = db.query_db(query)

    if response is not None:
        items = []

        for x in response:
            items.append([x[0], x[1]])

        for y in items:
            log_data = [[y[0], (y[1]).strip().replace("\n", "").replace("\r", "").replace("&nbsp;", "")]]
            df = pandas.DataFrame(log_data, columns=["item_no", "html_description"])
            try:
                pandas.read_csv(log)
            except FileNotFoundError:
                df.to_csv(log, mode='a', header=True, index=False)
            else:
                df.to_csv(log, mode='a', header=False, index=False)


def set_sale_price(query, discount_percentage):
    """takes a sql query and discount percentage and sets PRC_2 and updates lst_modified."""
    response = db.query_db(query)
    if response is not None:
        for x in response:
            item = Product(x[0])
            item.set_sale_price(discount=discount_percentage)


def update_timestamp(sku):
    query = f"""
    UPDATE IM_ITEM
    SET LST_MAINT_DT = GETDATE()
    WHERE ITEM_NO = '{sku}'
    """
    db.query_db(query, commit=True)


def update_product_modifiers():
    """Get a list of all pottery, delete old modifiers, create new"""
    all_pots = get_pottery_for_workshop(mode="")
    counter = 1
    for x in all_pots:
        print(f"Number {counter}/{len(all_pots)}: {x}")
        modifier_id = big_products.get_modifier_id(x)
        if modifier_id is not None:
            big_products.delete_product_modifier(x, modifier_id)
            big_products.add_container_workshop_to_item(x)
        counter += 1


def check_for_bound_product_with_no_parent():
    """prints merged items with no parent or who have multiple parents"""
    for x in get_binding_ids():
        parent = get_parent_product(x)
        if parent is None or type(parent) is list:
            print(f"Binding ID: {x}, Parent: {get_parent_product(x)}")


def get_top_child_product(binding_key):
    """Get Top Performing child product of merged product (by sales in last year window)"""
    from reporting.product_reports import create_top_items_report
    children = create_top_items_report(beginning_date=date_presets.one_year_ago,
                                       ending_date=date_presets.last_year_forecast,
                                       merged=True,
                                       binding_id=binding_key,
                                       number_of_items=1,
                                       return_format=3)
    if children is not None:
        top_child = children[0]
        return top_child


def get_binding_id_issues():
    """prints merged items with no parent or who have multiple parents"""
    result = ""
    binding_ids = get_binding_ids()
    for x in binding_ids:
        pattern = r'B\d{4}'
        if not bool(re.fullmatch(pattern, x)):
            result += f"<p>Binding ID: {x}, does not match pattern.</p>\n"
    for x in binding_ids:
        parent = get_parent_product(x)
        if parent is None or type(parent) is list:
            result += f"<p>Binding ID: {x}, has no parent.</p>\n"
        if type(parent) is list:
            result += f"<p>Binding ID: {x}, has multiple parents: {get_parent_product(x)}</p>\n"

    if result == "":
        result = "<p>No Items</p>"

    return result
