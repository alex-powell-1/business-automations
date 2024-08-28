from setup import creds
from setup.creds import Column, Table
from setup import query_engine
from setup.error_handler import ProcessOutErrorHandler
from datetime import datetime, timedelta
from setup.utilities import format_phone
from traceback import format_exc as tb


class Database:
    db = query_engine.QueryEngine
    error_handler = ProcessOutErrorHandler.error_handler
    logger = ProcessOutErrorHandler.logger

    def create_tables():
        tables = {
            'design_leads': f"""
                                        CREATE TABLE {Table.Middleware.design_leads} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        DATE datetime NOT NULL DEFAULT(current_timestamp),
                                        CUST_NO varchar(50),
                                        FST_NAM varchar(50),
                                        LST_NAM varchar(50),
                                        EMAIL varchar(60), 
                                        PHONE varchar(40),
                                        SKETCH bit DEFAULT(0),
                                        SCALED bit DEFAULT(0),
                                        DIGITAL bit DEFAULT(0),
                                        ON_SITE bit DEFAULT(0), 
                                        DELIVERY bit DEFAULT(0),
                                        INSTALL bit DEFAULT(0),
                                        TIMELINE varchar(50), 
                                        STREET varchar(75),
                                        CITY varchar(50),
                                        STATE varchar(20),
                                        ZIP varchar(10),
                                        COMMENTS varchar(500)
                                        )""",
            'qr': f"""
                                        CREATE TABLE {Table.Middleware.qr} (
                                        QR_CODE varchar(100) NOT NULL PRIMARY KEY,
                                        URL varchar(100),
                                        PUBLICATION varchar(50),
                                        MEDIUM varchar(50),
                                        OFFER varchar(100),
                                        DESCR varchar(255),
                                        COUPON_CODE varchar(20),
                                        COUPON_USE int DEFAULT(0),
                                        VISIT_COUNT int DEFAULT(0),
                                        CREATE_DT datetime NOT NULL DEFAULT(current_timestamp),
                                        LST_SCAN datetime
                                        )""",
            'qr_activ': f"""
                                        CREATE TABLE {Table.Middleware.qr_activity} (
                                        SCAN_DT datetime NOT NULL DEFAULT(current_timestamp) PRIMARY KEY,
                                        CODE varchar(100) NOT NULL FOREIGN KEY REFERENCES SN_QR(QR_CODE),
                                        );""",
            'sms': f"""
                                        CREATE TABLE {Table.Middleware.sms}(
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        DATE datetime NOT NULL DEFAULT(current_timestamp),
                                        ORIGIN varchar(30),
                                        CAMPAIGN varchar(30),
                                        DIRECTION varchar(15),
                                        TO_PHONE varchar(30),
                                        FROM_PHONE varchar(30),
                                        BODY varchar(500), 
                                        USERNAME varchar(50),
                                        CUST_NO varchar(50),
                                        NAME varchar(80),
                                        CATEGORY varchar(50),
                                        MEDIA varchar(500), 
                                        SID varchar(100),
                                        ERROR_CODE varchar(20),
                                        ERROR_MESSAGE varchar(255)
                                        )""",
            'sms_event': f"""
                                        CREATE TABLE {Table.Middleware.sms_event}(
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        DATE datetime NOT NULL DEFAULT(current_timestamp),
                                        ORIGIN varchar(30),
                                        CAMPAIGN varchar(30),
                                        PHONE varchar(30),
                                        CUST_NO varchar(50),
                                        NAME varchar(80),
                                        CATEGORY varchar(50),
                                        EVENT_TYPE varchar(20),
                                        MESSAGE varchar(255)
                                        )""",
        }
        for table in tables:
            Database.db.query(tables[table])

    class DesignLead:
        def get(yesterday=True):
            if yesterday:
                query = f"""
                SELECT * FROM {Table.Middleware.design_leads}
                WHERE DATE > '{datetime.now().date() - timedelta(days=1)}' AND DATE < '{datetime.now().date()}'
                """
            else:
                query = f"""
                    SELECT * FROM {Table.Middleware.design_leads}
                    """
            return Database.db.query(query)

        def insert(
            date,
            cust_no,
            first_name,
            last_name,
            email,
            phone,
            interested_in,
            timeline,
            street,
            city,
            state,
            zip_code,
            comments,
        ):
            sketch, scaled, digital, on_site, delivery, install = 0, 0, 0, 0, 0, 0
            if interested_in is not None:
                for x in interested_in:
                    if x == 'FREE Sketch-N-Go Service':
                        sketch = 1
                    if x == 'Scaled Drawing':
                        scaled = 1
                    if x == 'Digital Renderings':
                        digital = 1
                    if x == 'On-Site Consultation':
                        on_site = 1
                    if x == 'Delivery & Placement Service':
                        delivery = 1
                    if x == 'Professional Installation':
                        install = 1

            query = f"""
                INSERT INTO {Table.Middleware.design_leads} (DATE, CUST_NO, FST_NAM, LST_NAM, EMAIL, PHONE, SKETCH, SCALED, DIGITAL, 
                ON_SITE, DELIVERY, INSTALL, TIMELINE, STREET, CITY, STATE, ZIP, COMMENTS)
                VALUES ('{date}', {f"'{cust_no}'" if cust_no else 'NULL'}, '{first_name}', '{last_name}', '{email}', '{phone}', {sketch}, 
                {scaled}, {digital}, {on_site}, {delivery}, {install}, '{timeline}', 
                '{street}', '{city}', '{state}', '{zip_code}', '{comments}')
                """
            response = Database.db.query(query)
            if response['code'] == 200:
                Database.logger.success(f'Design Lead {first_name} {last_name} added to Middleware.')
            else:
                error = f'Error adding design lead {first_name} {last_name} to Middleware. \nQuery: {query}\nResponse: {response}'
                Database.error_handler.add_error_v(error=error, origin='insert_design_lead')

    class SMS:
        def get(cust_no=None):
            if cust_no:
                query = f"""
                SELECT * FROM {Table.Middleware.sms}
                WHERE CUST_NO = '{cust_no}'
                """
            else:
                query = f"""
                SELECT * FROM {Table.Middleware.sms}
                """
            return Database.db.query(query)

        def insert(
            origin,
            to_phone,
            from_phone,
            cust_no,
            name,
            category,
            body,
            media,
            sid,
            error_code,
            error_message,
            campaign=None,
            username=None,
        ):
            body = body.replace("'", "''")
            name = name.replace("'", "''")
            to_phone = format_phone(to_phone, mode='counterpoint')
            from_phone = format_phone(from_phone, mode='counterpoint')

            if from_phone == format_phone(creds.twilio_phone_number, mode='counterpoint'):
                direction = 'OUTBOUND'
            else:
                direction = 'INBOUND'

            query = f"""
                INSERT INTO {Table.Middleware.sms} (ORIGIN, CAMPAIGN, DIRECTION, TO_PHONE, FROM_PHONE, CUST_NO, BODY, USERNAME, NAME, CATEGORY, MEDIA, SID, ERROR_CODE, ERROR_MESSAGE)
                VALUES ('{origin}', {f"'{campaign}'" if campaign else 'NULL'}, '{direction}', '{to_phone}', '{from_phone}', 
                {f"'{cust_no}'" if cust_no else 'NULL'}, '{body}', {f"'{username}'" if username else 'NULL'}, '{name}', 
                {f"'{category}'" if category else 'NULL'}, {f"'{media}'" if media else 'NULL'}, {f"'{sid}'" if sid else 'NULL'}, 
                {f"'{error_code}'" if error_code else 'NULL'}, {f"'{error_message}'" if error_message else 'NULL'})
                """
            response = Database.db.query(query)
            if response['code'] == 200:
                if direction == 'OUTBOUND':
                    Database.logger.success(f'SMS sent to {to_phone} added to Database.')
                else:
                    Database.logger.success(f'SMS received from {from_phone} added to Database.')
            else:
                error = f'Error adding SMS sent to {to_phone} to Middleware. \nQuery: {query}\nResponse: {response}'
                Database.error_handler.add_error_v(error=error, origin='insert_sms')

    class Counterpoint:
        class Order:
            def delete(doc_id=None, tkt_no=None):
                if doc_id:
                    query = f"""
                    DELETE FROM {Table.CP.orders}
                    WHERE DOC_ID = {doc_id}"""
                elif tkt_no:
                    query = f"""
                    DELETE FROM {Table.CP.orders}
                    WHERE TKT_NO = '{tkt_no}'"""

                else:
                    return

                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Order {doc_id or tkt_no} deleted from PS_DOC_HDR.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Order {doc_id or tkt_no} not found in PS_DOC_HDR.')
                else:
                    error = f'Error deleting order {doc_id or tkt_no} from PS_DOC_HDR. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

        class Product:
            def set_sale_price(sku, price):
                query = f"""
                UPDATE {Table.CP.item_prices}
                SET PRC_2 = {price}, LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO = '{sku}'				
                """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Sale price set for {sku}.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Set Sale Price: No rows affected for {sku}.')
                else:
                    error = f'Error setting sale price for {sku}. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def remove_sale_price(items):
                if len(items) > 1:
                    where_filter = f'WHERE ITEM_NO IN {tuple(items)}'
                else:
                    where_filter = f"WHERE ITEM_NO = '{items[0]}'"

                    query = f"""
                    UPDATE IM_PRC
                    SET PRC_2 = NULL, LST_MAINT_DT = GETDATE()
                    {where_filter}
            
                    UPDATE IM_ITEM
                    SET IS_ON_SALE = 'N', SALE_DESCR = NULL, LST_MAINT_DT = GETDATE()
                    {where_filter}
                    """
                    # Removing Sale Price, Last Maintenance Date, and Removing from On Sale Category
                    response = Database.db.query(query)

                    if response['code'] == 200:
                        Database.logger.success(f'Sale Price removed successfully from {items}.')
                    elif response['code'] == 201:
                        Database.logger.warn(f'No Rows Affected for {items}')
                    else:
                        Database.error_handler.add_error_v(
                            error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Removal")'
                        )

            def set_sale_status(items, status, description=None):
                if len(items) > 1:
                    where_filter = f' WHERE ITEM_NO IN {tuple(items)}'
                else:
                    where_filter = f" WHERE ITEM_NO = '{items[0]}'"

                query = f"""
                    UPDATE {Table.CP.items}
                    SET IS_ON_SALE = '{status}', LST_MAINT_DT = GETDATE()
                    """
                if description:
                    query += f", SALE_DESCR = '{description}'"
                else:
                    query += ', SALE_DESCR = NULL'

                query += where_filter

                # Updating Sale Price, Sale Flag, Sale Description, Last Maintenance Date
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Sale status updated for {items}.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Sale status not updated for {items}.')
                else:
                    error = f'Error updating sale status for {items}. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(payload):
                """FOR PRODUCTS_UPDATE WEBHOOK ONLY. Normal updates from shopify_catalog.py use sync()"""
                query = f'UPDATE {Table.CP.items} SET '
                # Item Status
                if 'status' in payload:
                    if payload['status'] == 'active':
                        query += f"{Column.CP.Product.web_visible} = 'Y', "
                    else:
                        query += f"{Column.CP.Product.web_visible} = 'N', "
                # Web Title
                if 'title' in payload:
                    title = payload['title'].replace("'", "''")[:80]  # 80 char limit
                    query += f"{Column.CP.Product.web_title} = '{title}', "

                # SEO Data
                if 'meta_title' in payload:
                    meta_title = payload['meta_title'].replace("'", "''")[:80]  # 80 char limit
                    query += f"{Column.CP.Product.meta_title} = '{meta_title}', "
                if 'meta_description' in payload:
                    meta_description = payload['meta_description'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Column.CP.Product.meta_description} = '{meta_description}', "

                # Image Alt Text
                if 'alt_text_1' in payload:
                    alt_text_1 = payload['alt_text_1'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Column.CP.Product.alt_text_1} = '{alt_text_1}', "
                if 'alt_text_2' in payload:
                    alt_text_2 = payload['alt_text_2'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Column.CP.Product.alt_text_2} = '{alt_text_2}', "
                if 'alt_text_3' in payload:
                    alt_text_3 = payload['alt_text_3'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Column.CP.Product.alt_text_3} = '{alt_text_3}', "
                if 'alt_text_4' in payload:
                    alt_text_4 = payload['alt_text_4'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Column.CP.Product.alt_text_4} = '{alt_text_4}', "

                # The following Metafields require an ID to be maintained in the middleware.
                # Check for ID in the respective column. If exists, just update the CP product table.
                # If not, insert the metafield ID into the Middleware and then update the CP product table.

                # Product Status Metafields
                # if 'featured' in payload:
                #     query += f"{Database.Counterpoint.Product.columns['featured']} = {'Y' if payload['featured'] else 'N'}, "

                # if 'in_store_only' in payload:
                #     query += f"{Database.Counterpoint.Product.columns['in_store_only']} = {'Y' if payload['in_store_only'] else 'N'}, "

                # if 'is_preorder_item' in payload:
                #     query += f"{Database.Counterpoint.Product.columns['is_preorder_item']} = {'Y' if payload['is_preorder_item'] else 'N'}, "

                # if 'preorder_message' in payload:
                #     preorder_message = payload['preorder_message'].replace("'", "''")[:160]
                #     query += f"{Database.Counterpoint.Product.columns['preorder_message']} = '{preorder_message}', "

                # if 'preorder_release_date' in payload:
                #     query += f"{Database.Counterpoint.Product.columns['preorder_release_date']} = '{payload['preorder_release_date']}', "

                # # Product Specification Metafields
                # if 'botanical_name' in payload:
                #     query += f"CF_BOTAN_NAM = '{payload['botanical_name']}', "

                # if 'plant_type' in payload:
                #     query += f"CF_PLANT_TYP = '{payload['plant_type']}', "

                # if 'light_requirements' in payload:
                #     query += f"CF_LIGHT_REQ = '{payload['light_requirements']}', "

                # if 'size' in payload:
                #     query += f"CF_SIZE = '{payload['size']}', "

                # if 'features' in payload:
                #     query += f"CF_FEATURES = '{payload['features']}', "

                # if 'bloom_season' in payload:
                #     query += f"CF_BLOOM_SEASON = '{payload['bloom_season']}', "

                # if 'bloom_color' in payload:
                #     query += f"CF_BLOOM_COLOR = '{payload['bloom_color']}', "

                # if 'color' in payload:
                #     query += f"CF_COLOR = '{payload['color']}', "

                if query[-2:] == ', ':
                    query = query[:-2]

                query += f" WHERE ITEM_NO = '{payload['item_no']}'"

                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Product {payload["item_no"]} updated in Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Product {payload["item_no"]} not found in Middleware.')
                else:
                    error = f'Error updating product {payload["item_no"]} in Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            class HTMLDescription:
                table = 'EC_ITEM_DESCR'

                def get(item_no):
                    query = f"""
                    SELECT HTML_DESCR FROM {Database.Counterpoint.Product.HTMLDescription.table} 
                    WHERE ITEM_NO = '{item_no}'
                    """
                    return Database.db.query(query)

                def update(item_no, description):
                    description = description.replace("'", "''")
                    query = f"""
                    UPDATE {Database.Counterpoint.Product.HTMLDescription.table}
                    SET HTML_DESCR = '{description}'
                    WHERE ITEM_NO = '{item_no}'
                    """
                    response = Database.db.query(query)
                    if response['code'] == 200:
                        Database.logger.success(f'HTML Description updated for item {item_no}.')
                    elif response['code'] == 201:
                        Database.Counterpoint.Product.HTMLDescription.insert(item_no, description)
                    else:
                        error = f'Error updating HTML Description for item {item_no}. \nQuery: {query}\nResponse: {response}'
                        Database.error_handler.add_error_v(error=error)
                        raise Exception(error)

                def insert(item_no, description):
                    description = description.replace("'", "''")

                    query = f"""
                    INSERT INTO {Database.Counterpoint.Product.HTMLDescription.table} (ITEM_NO, HTML_DESCR, LST_MAINT_DT, 
                    LST_MAINT_USR_ID)
                    VALUES ('{item_no}', '{description}', GETDATE(), 'AP')
                    """
                    response = Database.db.query(query)
                    if response['code'] == 200:
                        Database.logger.success(f'INSERT: HTML Description for {item_no}.')
                    else:
                        error = f'Error adding HTML Description for item {item_no}. \nQuery: {query}\nResponse: {response}'
                        Database.error_handler.add_error_v(error=error)
                        raise Exception(error)

            class Media:
                class Video:
                    def get():
                        query = f"""
                        SELECT ITEM_NO, {Column.CP.Product.videos} 
                        FROM {Table.CP.items}
                        WHERE {Column.CP.Product.videos} IS NOT NULL AND
                        {Column.CP.Product.web_enabled} = 'Y'
                        """
                        response = Database.db.query(query)
                        all_videos = [[x[0], x[1]] for x in response] if response else []
                        temp_videos = []

                        if all_videos:
                            for entry in all_videos:
                                if ',' in entry[1]:
                                    multi_video_list = entry[1].replace(' ', '').split(',')

                                    for video in multi_video_list:
                                        temp_videos.append([entry[0], video])

                                else:
                                    temp_videos.append(entry)

                        return temp_videos

            class Discount:
                def update(rule):
                    for i in rule.items:
                        # Add Sale Item Flag and Sale Description to Items
                        query = f"""
                        UPDATE {Table.CP.items}
                        SET IS_ON_SALE = 'Y', SALE_DESCR = '{rule.badge_text}', LST_MAINT_DT = GETDATE()
                        WHERE ITEM_NO = '{i}'
                        """
                        # Updating Sale Price, Last Maintenance Date, and Adding to On Sale Category
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(f'Item: {i} Sale Status and Description Added Successfully.')
                        elif response['code'] == 201:
                            Database.logger.warn(f'No Rows Affected for Item: {i}')
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Addition")'
                            )

        class Customer:
            def get(last_sync=datetime(1970, 1, 1), customer_no=None, customer_list=None):
                if customer_no:
                    customer_filter = f"AND CP.CUST_NO = '{customer_no}'"
                elif customer_list:
                    customer_filter = f'AND CP.CUST_NO IN {tuple(customer_list)}'
                else:
                    customer_filter = ''

                query = f"""
                SELECT CP.CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, MW.LOY_ACCOUNT, ADRS_1, ADRS_2, CITY, STATE, ZIP_COD, CNTRY,
                MW.SHOP_CUST_ID, MW.META_CUST_NO, CATEG_COD, MW.META_CATEG, PROF_COD_2, MW.META_BIR_MTH, PROF_COD_3, MW.META_SPS_BIR_MTH, 
                PROF_ALPHA_1, MW.WH_PRC_TIER, {creds.sms_subscribe_status}, MW.ID 
                FROM {Table.CP.customers} CP
                FULL OUTER JOIN {Table.Shopify.customers} MW on CP.CUST_NO = MW.cust_no
                WHERE IS_ECOMM_CUST = 'Y' AND CP.LST_MAINT_DT > '{last_sync}' and CUST_NAM_TYP = 'P' {customer_filter}
                """
                return Database.db.query(query)

            def update_timestamps(customer_list):
                if len(customer_list) == 1:
                    customer_list = f"('{customer_list[0]}')"
                else:
                    customer_list = str(tuple(customer_list))
                query = f"""
                UPDATE {Table.CP.customers}
                SET LST_MAINT_DT = GETDATE()
                WHERE CUST_NO IN {customer_list}"""

                response = Database.db.query(query)

                if response['code'] == 200:
                    Database.logger.success('Customer timestamps updated.')
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error updating customer timestamps.\n\nQuery: {query}\n\nResponse: {response}',
                        origin='update_customer_timestamps',
                    )
                    raise Exception(response['message'])

            class Address:
                def get(cust_no):
                    query = f"""
                    SELECT FST_NAM, LST_NAM, ADRS_1, ADRS_2, CITY, STATE, ZIP_COD, CNTRY, PHONE_1
                    FROM {Table.CP.customer_ship_addresses}
                    WHERE CUST_NO = '{cust_no}'"""
                    return Database.db.query(query)

        class Promotion:
            def get(group_code=None, ids_only=False):
                if group_code:
                    promotions = [group_code]
                else:
                    # Get list of promotions from IM_PRC_GRP
                    response = Database.db.query('SELECT GRP_COD FROM IM_PRC_GRP')
                    promotions = [x[0] for x in response] if response else []
                    if ids_only:
                        return promotions

                if promotions:
                    # Get promotion details from IM_PRC_GRP and IM_PRC_GRP_RUL
                    result = []
                    for promo in promotions:
                        query = f"""
                        SELECT TOP 1 GRP.GRP_TYP, GRP.GRP_COD, GRP.GRP_SEQ_NO, GRP.DESCR, GRP.CUST_FILT, GRP.BEG_DAT, 
                        GRP.END_DAT, GRP.LST_MAINT_DT, GRP.ENABLED, GRP.MIX_MATCH_COD, MW.SHOP_ID
                        FROM IM_PRC_GRP GRP FULL OUTER JOIN {Table.Middleware.discounts} MW ON GRP.GRP_COD = MW.GRP_COD
                        WHERE GRP.GRP_COD = '{promo}' and GRP.GRP_TYP = 'P'
                        """
                        response = Database.db.query(query=query)
                        try:
                            promotion = [x for x in response[0]] if response else None
                        except Exception as e:
                            Database.error_handler.add_error_v(
                                error=f'Error getting promotion details for {promo}.\n\nResponse: {response}\n\nError: {e}',
                                origin='get_promotion',
                                traceback=tb(),
                            )
                            promotion = None
                        if promotion:
                            result.append(promotion)
                    return result

            class PriceRule:
                def get(group_code):
                    query = f"""
                    SELECT RUL.GRP_TYP, RUL.GRP_COD, RUL.RUL_SEQ_NO, RUL.DESCR, RUL.CUST_FILT, RUL.ITEM_FILT, 
                    RUL.SAL_FILT, RUL.IS_CUSTOM, RUL.USE_BOGO_TWOFER, RUL.REQ_FULL_GRP_FOR_BOGO_TWOFER, 
                    MW.SHOP_ID, GRP.ENABLED, MW.ENABLED, MW.ID
                    FROM IM_PRC_RUL RUL
					INNER JOIN IM_PRC_GRP GRP on GRP.GRP_COD = RUL.GRP_COD
                    FULL OUTER JOIN SN_PROMO MW on rul.GRP_COD = MW.GRP_COD
                    WHERE RUL.GRP_COD = '{group_code}'
                    """
                    response = Database.db.query(query)
                    return [rule for rule in response] if response else []

    class Shopify:
        def rebuild_tables(self):
            def create_tables():
                tables = {
                    'categories': f"""
                                            CREATE TABLE {Table.Middleware.collections} (
                                            CATEG_ID int IDENTITY(1,1) PRIMARY KEY,
                                            COLLECTION_ID bigint,
                                            MENU_ID bigint,
                                            CP_CATEG_ID bigint NOT NULL,
                                            CP_PARENT_ID bigint,
                                            CATEG_NAME nvarchar(255) NOT NULL,
                                            SORT_ORDER int,
                                            DESCRIPTION text,
                                            IS_VISIBLE BIT NOT NULL DEFAULT(1),
                                            IMG_SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'products': f"""
                                            CREATE TABLE {Table.Middleware.products} (                                        
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO varchar(50) NOT NULL,
                                            BINDING_ID varchar(10),
                                            IS_PARENT BIT,
                                            PRODUCT_ID bigint NOT NULL,
                                            VARIANT_ID bigint,
                                            INVENTORY_ID bigint,
                                            VARIANT_NAME nvarchar(255),
                                            OPTION_ID bigint,
                                            OPTION_VALUE_ID bigint,
                                            CATEG_ID varchar(255),
                                            CF_BOTAN_NAM bigint, 
                                            CF_CLIM_ZON bigint, 
                                            CF_PLANT_TYP bigint, 
                                            CF_TYP bigint, 
                                            CF_HEIGHT bigint, 
                                            CF_WIDTH bigint, 
                                            CF_SUN_EXP bigint, 
                                            CF_BLOOM_TIM bigint,
                                            CF_FLOW_COL bigint,
                                            CF_POLLIN bigint, 
                                            CF_GROWTH_RT bigint, 
                                            CF_DEER_RES bigint, 
                                            CF_SOIL_TYP bigint,
                                            CF_COLOR bigint,
                                            CF_SIZE bigint,
                                            CF_BLOOM_SEAS bigint,
                                            CF_LIGHT_REQ bigint,
                                            CF_FEATURES bigint,
                                            CF_CLIM_ZON bigint,
                                            CF_CLIM_ZON_LST bigint,
                                            CF_IS_PREORDER bigint,
                                            CF_PREORDER_DT bigint,
                                            CF_PREORDER_MSG bigint,
                                            CF_IS_FEATURED bigint,
                                            CF_IS_IN_STORE_ONLY bigint,
                                            CF_IS_ON_SALE bigint, 
                                            CF_SALE_DESCR bigint,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'images': f"""
                                            CREATE TABLE {Table.Middleware.images} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            IMAGE_NAME nvarchar(255) NOT NULL,
                                            ITEM_NO varchar(50),
                                            FILE_PATH nvarchar(255) NOT NULL,
                                            PRODUCT_ID bigint,
                                            IMAGE_ID bigint,
                                            THUMBNAIL BIT DEFAULT(0),
                                            IMAGE_NUMBER int DEFAULT(1),
                                            SORT_ORDER int,
                                            IS_BINDING_IMAGE BIT NOT NULL,
                                            BINDING_ID varchar(50),
                                            IS_VARIANT_IMAGE BIT DEFAULT(0),
                                            DESCR nvarchar(255),
                                            SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'videos': f"""
                                            CREATE TABLE {Table.Middleware.images} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO varchar(50),
                                            URL nvarchar(255),
                                            VIDEO_NAME nvarchar(255),
                                            FILE_PATH nvarchar(255),
                                            PRODUCT_ID bigint,
                                            VIDEO_ID bigint,
                                            SORT_ORDER int,
                                            BINDING_ID varchar(50),
                                            DESCR nvarchar(255),
                                            SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'customers': f"""
                                            CREATE TABLE {Table.Middleware.customers} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            CUST_NO varchar(50) NOT NULL,
                                            SHOP_CUST_ID bigint,
                                            META_CUST_NO bigint,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'orders': f"""
                                            CREATE TABLE {Table.Middleware.orders} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            ORDER_NO int NOT NULL,
                                            DOC_ID bigint,
                                            STATUS bit DEFAULT(0)
                                            )""",
                    'gift': f"""
                                            CREATE TABLE {Table.Middleware.gift_certificates} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            GFC_NO varchar(30) NOT NULL,
                                            BC_GFC_ID int NOT NULL,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            )""",
                    'promo': f""" 
                                            CREATE TABLE {Table.Middleware.discounts}(
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            GRP_COD nvarchar(50) NOT NULL,
                                            RUL_SEQ_NO int,
                                            SHOP_ID bigint,
                                            ENABLED bit DEFAULT(0),
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );""",
                    'metafields': f""" 
                                            CREATE TABLE {Table.Middleware.metafields}(
                                            META_ID bigint NOT NULL, 
                                            NAME varchar(50) NOT NULL, 
                                            DESCR varchar(255),
                                            NAME_SPACE varchar(50), 
                                            META_KEY varchar(50), 
                                            TYPE varchar(50), 
                                            PINNED_POS bit, 
                                            OWNER_TYPE varchar(50),
                                            VALID_NAME varchar(50),
                                            VALID_VALUE varchar(255),
                                            LST_MAINT_DT DATETIME DEFAULT(current_timestamp))""",
                }

                for table in tables:
                    Database.db.query(tables[table])

            # Drop Tables
            def drop_tables():
                tables = [
                    creds.bc_customer_table,
                    creds.bc_image_table,
                    creds.bc_product_table,
                    creds.bc_brands_table,
                    creds.bc_category_table,
                    creds.bc_gift_cert_table,
                    creds.bc_order_table,
                ]

                def drop_table(table_name):
                    Database.db.query(f'DROP TABLE {table_name}')

                for table in tables:
                    drop_table(table)

            # Recreate Tables
            drop_tables()
            create_tables()

        class Customer:
            def get(customer_no=None):
                if customer_no:
                    query = f"""
                            SELECT * FROM {Table.Middleware.customers}
                            WHERE CUST_NO = '{customer_no}'
                            """
                else:
                    query = f"""
                            SELECT * FROM {Table.Middleware.customers}
                            """
                return Database.db.query(query)

            def get_id(cp_cust_no):
                query = f"""
                        SELECT SHOP_CUST_ID FROM {Table.Middleware.customers}
                        WHERE CUST_NO = '{cp_cust_no}'
                        """
                response = Database.db.query(query)
                return response[0][0] if response else None

            def exists(shopify_cust_no):
                query = f"""
                        SELECT * FROM {Table.Middleware.customers}
                        WHERE SHOP_CUST_ID = {shopify_cust_no}
                        """
                return Database.db.query(query)

            def insert(
                cp_cust_no,
                shopify_cust_no,
                loyalty_point_id=None,
                meta_cust_no_id=None,
                meta_category_id=None,
                meta_birth_month_id=None,
                meta_spouse_birth_month_id=None,
                meta_wholesale_price_tier_id=None,
            ):
                query = f"""
                        INSERT INTO {Table.Middleware.customers} (CUST_NO, SHOP_CUST_ID, 
                        LOY_ACCOUNT, META_CUST_NO, META_CATEG, META_BIR_MTH, META_SPS_BIR_MTH, WH_PRC_TIER)
                        VALUES ('{cp_cust_no}', '{shopify_cust_no}', 
                        {loyalty_point_id if loyalty_point_id else "NULL"}, 
                        {meta_cust_no_id if meta_cust_no_id else "NULL"}, 
                        {meta_category_id if meta_category_id else "NULL"}, 
                        {meta_birth_month_id if meta_birth_month_id else "NULL"}, 
                        {meta_spouse_birth_month_id if meta_spouse_birth_month_id else "NULL"}, 
                        {meta_wholesale_price_tier_id if meta_wholesale_price_tier_id else "NULL"})
                        """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {cp_cust_no} added to Middleware.')
                else:
                    error = (
                        f'Error adding customer {cp_cust_no} to Middleware. \nQuery: {query}\nResponse: {response}'
                    )
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(
                cp_cust_no,
                shopify_cust_no,
                loyalty_point_id=None,
                meta_cust_no_id=None,
                meta_category_id=None,
                meta_birth_month_id=None,
                meta_spouse_birth_month_id=None,
                meta_wholesale_price_tier_id=None,
            ):
                query = f"""
                        UPDATE {Table.Middleware.customers}
                        SET SHOP_CUST_ID = {shopify_cust_no}, 
                        LOY_ACCOUNT = {loyalty_point_id if loyalty_point_id else "NULL"},
                        META_CUST_NO = {meta_cust_no_id if meta_cust_no_id else "NULL"},
                        META_CATEG = {meta_category_id if meta_category_id else "NULL"},
                        META_BIR_MTH = {meta_birth_month_id if meta_birth_month_id else "NULL"},
                        META_SPS_BIR_MTH = {meta_spouse_birth_month_id if meta_spouse_birth_month_id else "NULL"},
                        WH_PRC_TIER = {meta_wholesale_price_tier_id if meta_wholesale_price_tier_id else "NULL"},
                        LST_MAINT_DT = GETDATE()
                        WHERE CUST_NO = '{cp_cust_no}'
                        """

                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {cp_cust_no} updated in Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Customer {cp_cust_no} not found in Middleware.')
                else:
                    error = f'Error updating customer {cp_cust_no} in Middleware. \nQuery: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def sync(customer):
                if not customer.cp_cust_no:
                    if customer.mw_id:
                        Database.Shopify.Customer.delete(customer)
                else:
                    if customer.mw_id:
                        Database.Shopify.Customer.update(
                            cp_cust_no=customer.cp_cust_no,
                            shopify_cust_no=customer.shopify_cust_no,
                            loyalty_point_id=customer.loyalty_point_id,
                            meta_cust_no_id=customer.meta_cust_no_id,
                            meta_category_id=customer.meta_category_id,
                            meta_birth_month_id=customer.meta_birth_month_id,
                            meta_spouse_birth_month_id=customer.meta_spouse_birth_month_id,
                            meta_wholesale_price_tier_id=customer.meta_wholesale_price_tier_id,
                        )
                    else:
                        Database.Shopify.Customer.insert(
                            cp_cust_no=customer.cp_cust_no,
                            shopify_cust_no=customer.shopify_cust_no,
                            loyalty_point_id=customer.loyalty_point_id,
                            meta_cust_no_id=customer.meta_cust_no_id,
                            meta_category_id=customer.meta_category_id,
                            meta_birth_month_id=customer.meta_birth_month_id,
                            meta_spouse_birth_month_id=customer.meta_spouse_birth_month_id,
                            meta_wholesale_price_tier_id=customer.meta_wholesale_price_tier_id,
                        )

            def delete(shopify_cust_no):
                if not shopify_cust_no:
                    return
                query = f'DELETE FROM {Table.Middleware.customers} WHERE SHOP_CUST_ID = {shopify_cust_no}'
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {shopify_cust_no} deleted from Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Customer {shopify_cust_no} not found in Middleware.')
                else:
                    error = f'Error deleting customer {shopify_cust_no} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

        class Order:
            pass

        class Collection:
            def get_cp_categ_id(collection_id):
                query = f"""
                        SELECT CP_CATEG_ID FROM {Table.Middleware.collections}
                        WHERE COLLECTION_ID = {collection_id}
                        """
                response = Database.db.query(query)
                try:
                    return response[0][0]
                except:
                    return None

            def insert(category):
                query = f"""
                INSERT INTO {Table.Middleware.collections}(COLLECTION_ID, MENU_ID, CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, 
                SORT_ORDER, DESCRIPTION, IS_VISIBLE, IMG_SIZE, LST_MAINT_DT)
                VALUES({category.collection_id if category.collection_id else 'NULL'}, 
                {category.menu_id if category.menu_id else 'NULL'}, {category.cp_categ_id}, 
                {category.cp_parent_id}, '{category.name}', {category.sort_order}, 
                '{category.description.replace("'", "''")}', {1 if category.is_visible else 0}, 
                {category.image_size if category.image_size else 'NULL'},
                '{category.lst_maint_dt:%Y-%m-%d %H:%M:%S}')
                """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Category {category.name} added to Middleware.')
                else:
                    error = f'Error adding category {category.name} to Middleware. \nQuery: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(category):
                query = f"""
                UPDATE {Table.Middleware.collections}
                SET COLLECTION_ID = {category.collection_id if category.collection_id else 'NULL'}, 
                MENU_ID = {category.menu_id if category.menu_id else 'NULL'},
                CP_PARENT_ID = {category.cp_parent_id}, CATEG_NAME = '{category.name}',
                SORT_ORDER = {category.sort_order}, DESCRIPTION = '{category.description}', 
                IS_VISIBLE = {1 if category.is_visible else 0},
                IMG_SIZE = {category.image_size if category.image_size else 'NULL'},
                LST_MAINT_DT = '{category.lst_maint_dt:%Y-%m-%d %H:%M:%S}'
                WHERE CP_CATEG_ID = {category.cp_categ_id}
                """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Category {category.name} updated in Middleware.')
                else:
                    error = f'Error updating category {category.name} in Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def delete(collection_id=None, cp_categ_id=None):
                if collection_id is not None:
                    cp_categ_id = Database.Shopify.Collection.get_cp_categ_id(collection_id)

                if cp_categ_id is None:
                    raise Exception('No CP_CATEG_ID provided for deletion.')

                query = f"""
                        DELETE FROM {Table.Middleware.collections}
                        WHERE CP_CATEG_ID = {cp_categ_id}
                        """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Category {cp_categ_id} deleted from Middleware.')
                else:
                    error = f'Error deleting category {cp_categ_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def backfill_html_description(collection_id, description):
                cp_categ_id = Database.Shopify.Collection.get_cp_categ_id(collection_id)
                query = f"""
                        UPDATE EC_CATEG
                        SET HTML_DESCR = '{description}'
                        WHERE CATEG_ID = '{cp_categ_id}'
                        """
                response = Database.db.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

        class Product:
            def get_by_category(cp_category=None, cp_subcategory=None):
                query = f"""SELECT ITEM_NO FROM {Table.CP.items} 
                WHERE {Column.CP.Product.web_enabled} = 'Y' AND 
                ({Column.CP.Product.binding_id} IS NULL OR
                {Column.CP.Product.is_parent} = 'Y') AND 
                """
                if cp_category and cp_subcategory:
                    query += f" CATEG_COD = '{cp_category}' AND SUBCAT_COD = '{cp_subcategory}'"
                else:
                    if cp_category:
                        query += f" CATEG_COD = '{cp_category}'"
                    if cp_subcategory:
                        query += f" SUBCAT_COD = '{cp_subcategory}'"

                response = Database.db.query(query)
                try:
                    sku_list = [x[0] for x in response] if response else None
                except:
                    sku_list = None

                if sku_list:
                    product_list = []
                    for sku in sku_list:
                        product_list.append(Database.Shopify.Product.get_id(item_no=sku))
                    return product_list

            def get_id(item_no=None, binding_id=None, image_id=None, video_id=None, all=False):
                """Get product ID from SQL using image ID. If not found, return None."""
                if all:
                    query = f"""SELECT PRODUCT_ID FROM {Table.Middleware.products} """
                    prod_id_res = Database.db.query(query)
                    if prod_id_res is not None:
                        return [x[0] for x in prod_id_res]

                product_query = None

                if item_no:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Table.Middleware.products} WHERE ITEM_NO = '{item_no}'"
                    )
                if image_id:
                    product_query = f'SELECT PRODUCT_ID FROM {Table.Middleware.images} WHERE IMAGE_ID = {image_id}'
                if video_id:
                    product_query = f'SELECT PRODUCT_ID FROM {Table.Middleware.videos} WHERE VIDEO_ID = {video_id}'
                if binding_id:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Table.Middleware.products} WHERE BINDING_ID = '{binding_id}'"
                    )

                if product_query:
                    prod_id_res = Database.db.query(product_query)
                    if prod_id_res is not None:
                        return prod_id_res[0][0]

                else:
                    Database.logger.warn('No product ID found for the given parameters.')
                    return None

            def get_parent_item_no(product_id):
                if product_id:
                    query = f"""
                            SELECT ITEM_NO FROM {Table.Middleware.products}
                            WHERE PRODUCT_ID = {product_id} AND (BINDING_ID IS NULL OR IS_PARENT = 1)
                            """
                else:
                    Database.logger.warn('No product ID provided for parent item number lookup.')
                    return
                response = Database.db.query(query)
                if response is not None:
                    try:
                        return response[0][0]
                    except:
                        return None

            def get_sku(product_id):
                if product_id:
                    query = f"""
                            SELECT ITEM_NO FROM {Table.Middleware.products}
                            WHERE PRODUCT_ID = {product_id}
                            """
                else:
                    Database.logger.warn('No product ID provided for SKU lookup.')
                    return
                response = Database.db.query(query)
                if response is not None:
                    try:
                        return response[0][0]
                    except:
                        return None

            def get_binding_id(product_id):
                if product_id:
                    query = f"""
                            SELECT BINDING_ID FROM {Table.Middleware.products}
                            WHERE PRODUCT_ID = {product_id}
                            """
                else:
                    Database.logger.warn('No product ID provided for binding ID lookup.')
                    return
                response = Database.db.query(query)
                if response is not None:
                    try:
                        return response[0][0]
                    except KeyError:
                        return None

            def sync(product):
                for variant in product.variants:
                    if variant.mw_db_id:
                        Database.Shopify.Product.Variant.update(product=product, variant=variant)
                    else:
                        Database.Shopify.Product.Variant.insert(product=product, variant=variant)

                if product.media:
                    for m in product.media:
                        if m.db_id is None:
                            if m.type == 'IMAGE':
                                Database.Shopify.Product.Media.Image.insert(m)
                            elif m.type == 'EXTERNAL_VIDEO':
                                Database.Shopify.Product.Media.Video.insert(m)
                        else:
                            if m.type == 'IMAGE':
                                Database.Shopify.Product.Media.Image.update(m)
                            elif m.type == 'EXTERNAL_VIDEO':
                                Database.Shopify.Product.Media.Video.update(m)

            def insert(product):
                for variant in product.variants:
                    Database.Shopify.Product.Variant.insert(product, variant)

            def delete(product_id):
                if product_id:
                    query = f'DELETE FROM {Table.Middleware.products} WHERE PRODUCT_ID = {product_id}'
                else:
                    Database.logger.warn('No product ID provided for deletion.')
                    return

                response = Database.db.query(query)

                if response['code'] == 200:
                    Database.logger.success(f'Product {product_id} deleted from Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Product {product_id} not found in Middleware.')
                else:
                    error = f'Error deleting product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

                Database.Shopify.Product.Media.Image.delete(product_id=product_id)
                Database.Shopify.Product.Media.Video.delete(product_id=product_id)

            class Variant:
                def get_id(sku):
                    if sku:
                        query = f"""
                            SELECT VARIANT_ID FROM {creds.shopify_product_table}
                            WHERE ITEM_NO = '{sku}'
                            """
                    else:
                        Database.logger.warn('No SKU provided for variant ID lookup.')
                        return
                    response = Database.db.query(query)
                    if response is not None:
                        return response[0][0]

                def get_option_id(sku):
                    if sku:
                        query = f"""
                            SELECT OPTION_ID FROM {creds.shopify_product_table}
                            WHERE ITEM_NO = '{sku}'
                            """
                    else:
                        Database.logger.warn('No SKU provided for option ID lookup.')
                        return
                    response = Database.db.query(query)
                    if response is not None:
                        return response[0][0]

                def get_option_value_id(sku):
                    if sku:
                        query = f"""
                            SELECT OPTION_VALUE_ID FROM {creds.shopify_product_table}
                            WHERE ITEM_NO = '{sku}'
                            """
                    else:
                        Database.logger.warn('No SKU provided for option value ID lookup.')
                        return
                    response = Database.db.query(query)
                    if response is not None:
                        return response[0][0]

                def insert(product, variant):
                    if product.shopify_collections:
                        collection_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        collection_string = None

                    insert_query = f"""
                        INSERT INTO {Table.Middleware.products} (ITEM_NO, BINDING_ID, IS_PARENT, 
                        PRODUCT_ID, VARIANT_ID, INVENTORY_ID, VARIANT_NAME, OPTION_ID, OPTION_VALUE_ID, CATEG_ID, 
                        CF_BOTAN_NAM, CF_PLANT_TYP, CF_HEIGHT, CF_WIDTH, CF_CLIM_ZON, CF_CLIM_ZON_LST,
                        CF_COLOR, CF_SIZE, CF_BLOOM_SEAS, CF_BLOOM_COLOR, CF_LIGHT_REQ, CF_FEATURES, CF_IS_PREORDER, 
                        CF_PREORDER_DT, CF_PREORDER_MSG, CF_IS_FEATURED, CF_IN_STORE_ONLY, CF_IS_ON_SALE, CF_SALE_DESCR
                        )
                         
                        VALUES ('{variant.sku}', {f"'{product.binding_id}'" if product.binding_id else 'NULL'}, 
                        {1 if variant.is_parent else 0}, {product.product_id if product.product_id else "NULL"}, 
                        {variant.variant_id if variant.variant_id else "NULL"}, 
                        {variant.inventory_id if variant.inventory_id else "NULL"}, 
                        {f"'{variant.variant_name}'" if variant.variant_name else "NULL"}, 
                        {variant.option_id if variant.option_id else "NULL"}, 
                        {variant.option_value_id if variant.option_value_id else "NULL"}, 
                        {f"'{collection_string}'" if collection_string else "NULL"},
                        {product.meta_botanical_name['id'] if product.meta_botanical_name['id'] else "NULL"},
                        {product.meta_plant_type['id'] if product.meta_plant_type['id'] else "NULL"},
                        {product.meta_height['id'] if product.meta_height['id'] else "NULL"},
                        {product.meta_width['id'] if product.meta_width['id'] else "NULL"},
                        {product.meta_climate_zone['id'] if product.meta_climate_zone['id'] else "NULL"},
                        {product.meta_climate_zone_list['id'] if product.meta_climate_zone_list['id'] else "NULL"},
                        {product.meta_colors['id'] if product.meta_colors['id'] else "NULL"},
                        {product.meta_size['id'] if product.meta_size['id'] else "NULL"},
                        {product.meta_bloom_season['id'] if product.meta_bloom_season['id'] else "NULL"},
                        {product.meta_bloom_color['id'] if product.meta_bloom_color['id'] else "NULL"},
                        {product.meta_light_requirements['id'] if product.meta_light_requirements['id'] else "NULL"},
                        {product.meta_features['id'] if product.meta_features['id'] else "NULL"},
                        {product.meta_is_preorder['id'] if product.meta_is_preorder['id'] else "NULL"},
                        {product.meta_preorder_release_date['id'] if product.meta_preorder_release_date['id'] else "NULL"},
                        {product.meta_preorder_message['id'] if product.meta_preorder_message['id'] else "NULL"},
                        {product.meta_is_featured['id'] if product.meta_is_featured['id'] else "NULL"},
                        {product.meta_in_store_only['id'] if product.meta_in_store_only['id'] else "NULL"},
                        {product.meta_is_on_sale['id'] if product.meta_is_on_sale['id'] else "NULL"},
                        {product.meta_sale_description['id'] if product.meta_sale_description['id'] else "NULL"}
                        )
                        """
                    response = Database.db.query(insert_query)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - INSERT Variant {product.sku}'
                        )
                    else:
                        error = f'Query: {insert_query}\n\nResponse: {response}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.insert(SKU: {variant.sku})'
                        )
                        raise Exception(error)

                def update(product, variant):
                    if product.shopify_collections:
                        collection_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        collection_string = None

                    update_query = f"""
                        UPDATE {Table.Middleware.products} 
                        SET ITEM_NO = '{variant.sku}', 
                        BINDING_ID = {f"'{product.binding_id}'" if product.binding_id else 'NULL'}, 
                        IS_PARENT = {1 if variant.is_parent else 0}, 
                        PRODUCT_ID = {product.product_id if product.product_id else 'NULL'}, 
                        VARIANT_ID = {variant.variant_id if variant.variant_id else 'NULL'}, 
                        INVENTORY_ID = {variant.inventory_id if variant.inventory_id else 'NULL'}, 
                        VARIANT_NAME = {f"'{variant.variant_name}'" if variant.variant_name else "NULL"}, 
                        OPTION_ID = {variant.option_id if variant.option_id else "NULL"}, 
                        OPTION_VALUE_ID = {variant.option_value_id if variant.option_value_id else "NULL"},  
                        CATEG_ID = {f"'{collection_string}'" if collection_string else "NULL"}, 
                        CF_BOTAN_NAM = {product.meta_botanical_name['id'] if product.meta_botanical_name['id'] else "NULL"},
                        CF_PLANT_TYP = {product.meta_plant_type['id'] if product.meta_plant_type['id'] else "NULL"},
                        CF_HEIGHT = {product.meta_height['id'] if product.meta_height['id'] else "NULL"},
                        CF_WIDTH = {product.meta_width['id'] if product.meta_width['id'] else "NULL"},
                        CF_CLIM_ZON = {product.meta_climate_zone['id'] if product.meta_climate_zone['id'] else "NULL"},
                        CF_CLIM_ZON_LST = {product.meta_climate_zone_list['id'] if product.meta_climate_zone_list['id'] else "NULL"},
                        CF_COLOR = {product.meta_colors['id'] if product.meta_colors['id'] else "NULL"},
                        CF_SIZE = {product.meta_size['id'] if product.meta_size['id'] else "NULL"},
                        CF_BLOOM_SEAS = {product.meta_bloom_season['id'] if product.meta_bloom_season['id'] else "NULL"},
                        CF_BLOOM_COLOR = {product.meta_bloom_color['id'] if product.meta_bloom_color['id'] else "NULL"},
                        CF_LIGHT_REQ = {product.meta_light_requirements['id'] if product.meta_light_requirements['id'] else "NULL"},
                        CF_FEATURES = {product.meta_features['id'] if product.meta_features['id'] else "NULL"},
                        CF_IS_PREORDER = {product.meta_is_preorder['id'] if product.meta_is_preorder['id'] else "NULL"},
                        CF_PREORDER_DT = {product.meta_preorder_release_date['id'] if product.meta_preorder_release_date['id'] else "NULL"},
                        CF_PREORDER_MSG = {product.meta_preorder_message['id'] if product.meta_preorder_message['id'] else "NULL"},
                        CF_IS_FEATURED = {product.meta_is_featured['id'] if product.meta_is_featured['id'] else "NULL"},
                        CF_IN_STORE_ONLY = {product.meta_in_store_only['id'] if product.meta_in_store_only['id'] else "NULL"},
                        CF_IS_ON_SALE = {product.meta_is_on_sale['id'] if product.meta_is_on_sale['id'] else "NULL"},
                        CF_SALE_DESCR = {product.meta_sale_description['id'] if product.meta_sale_description['id'] else "NULL"},
                        LST_MAINT_DT = GETDATE() 
                        WHERE ID = {variant.mw_db_id}
                        """
                    response = Database.db.query(update_query)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - UPDATE Variant'
                        )
                    elif response['code'] == 201:
                        Database.logger.warn(
                            f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - UPDATE Variant: No Rows Affected'
                        )
                    else:
                        error = f'Query: {update_query}\n\nResponse: {response}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Variant.update(SKU: {variant.sku})'
                        )
                        raise Exception(error)

                def delete(variant_id):
                    if variant_id:
                        query = f'DELETE FROM {Table.Middleware.products} WHERE VARIANT_ID = {variant_id}'
                    else:
                        Database.logger.warn('No variant ID provided for deletion.')
                        return
                    response = Database.db.query(query)

                    if response['code'] == 200:
                        Database.logger.success(f'Variant {variant_id} deleted from Middleware.')
                    else:
                        error = f'Error deleting variant {variant_id} from Middleware. \n Query: {query}\nResponse: {response}'
                        Database.error_handler.add_error_v(error=error)
                        raise Exception(error)

                class Media:
                    class Image:
                        def get(item_no):
                            """Return all image ids for a product."""
                            if item_no:
                                query = f"""
                                SELECT IMAGE_ID FROM {Table.Middleware.images}
                                WHERE ITEM_NO = '{item_no}'
                                """
                            else:
                                Database.logger.warn('No SKU provided for image ID lookup.')
                                return
                            response = Database.db.query(query)
                            if response:
                                return [x[0] for x in response] if response else None

            class Media:
                def delete(product_id):
                    Database.Shopify.Product.Media.Image.delete(product_id=product_id)
                    Database.Shopify.Product.Media.Video.delete(product_id=product_id)

                class Image:
                    table = Table.Middleware.images

                    def get(image_id, column=None):
                        if column is None:
                            column = '*'

                        if image_id:
                            where_filter = f'WHERE IMAGE_ID = {image_id}'

                        query = f"""
                        SELECT {column} FROM {Database.Shopify.Product.Media.Image.table}
                        {where_filter}
                        """
                        response = Database.db.query(query)
                        if column == '*':
                            return response
                        else:
                            try:
                                return response[0][0]
                            except:
                                return None

                    def get_image_id(file_name):
                        if file_name:
                            query = (
                                f"SELECT IMAGE_ID FROM {Table.Middleware.images} WHERE IMAGE_NAME = '{file_name}'"
                            )
                        else:
                            Database.logger.warn('No file name provided for image ID lookup.')
                            return
                        try:
                            img_id_res = Database.db.query(query)
                            return img_id_res[0][0]
                        except:
                            return None

                    def insert(image):
                        img_insert = f"""
                        INSERT INTO {Table.Middleware.images} (IMAGE_NAME, ITEM_NO, FILE_PATH,
                        PRODUCT_ID, IMAGE_ID, THUMBNAIL, IMAGE_NUMBER, SORT_ORDER,
                        IS_BINDING_IMAGE, BINDING_ID, IS_VARIANT_IMAGE, DESCR, SIZE)
                        VALUES (
                        '{image.name}', {f"'{image.sku}'" if image.sku != '' else 'NULL'},
                        '{image.file_path}', {image.product_id}, {image.shopify_id}, '{1 if image.is_thumbnail else 0}', 
                        '{image.number}', '{image.sort_order}', '{image.is_binding_image}',
                        {f"'{image.binding_id}'" if image.binding_id else 'NULL'}, '{image.is_variant_image}',
                        {f"'{image.description.replace("'", "''")}'" if image.description != '' else 'NULL'},
                        {image.size})"""
                        insert_img_response = Database.db.query(img_insert)
                        if insert_img_response['code'] == 200:
                            Database.logger.success(f'SQL INSERT Image {image.name}: Success')
                        else:
                            error = f'Error inserting image {image.name} into Middleware. \nQuery: {img_insert}\nResponse: {insert_img_response}'
                            Database.error_handler.add_error_v(
                                error=error, origin=f'Database.Shopify.Product.Media.Image.insert({image.name})'
                            )
                            raise Exception(error)

                    def update(image):
                        q = f"""
                        UPDATE {Table.Middleware.images}
                        SET IMAGE_NAME = '{image.name}', ITEM_NO = '{image.sku}', FILE_PATH = '{image.file_path}',
                        PRODUCT_ID = '{image.product_id}', IMAGE_ID = '{image.shopify_id}',
                        THUMBNAIL = '{1 if image.is_thumbnail else 0}', IMAGE_NUMBER = '{image.number}',
                        SORT_ORDER = '{image.sort_order}', IS_BINDING_IMAGE = '{image.is_binding_image}',
                        BINDING_ID = {f"'{image.binding_id}'" if image.binding_id else 'NULL'},
                        IS_VARIANT_IMAGE = '{image.is_variant_image}',
                        DESCR = {f"'{image.description.replace("'", "''")}'" if
                                    image.description != '' else 'NULL'}, SIZE = '{image.size}'
                        WHERE ID = {image.db_id}"""

                        res = Database.db.query(q)
                        if res['code'] == 200:
                            Database.logger.success(f'SQL UPDATE Image {image.name}: Success')
                        elif res['code'] == 201:
                            Database.logger.warn(f'SQL UPDATE Image {image.name}: Not found')
                        else:
                            error = (
                                f'Error updating image {image.name} in Middleware. \nQuery: {q}\nResponse: {res}'
                            )
                            Database.error_handler.add_error_v(
                                error=error, origin=f'Database.Shopify.Product.Media.Image.update({image.name})'
                            )
                            raise Exception(error)

                    def delete(image=None, image_id=None, product_id=None):
                        prod_id = None
                        if image_id:
                            # Delete Single Image from Image ID
                            prod_id = Database.Shopify.Product.Media.Image.get(
                                image_id=image_id, column='PRODUCT_ID'
                            )
                            sort_order = Database.Shopify.Product.Media.Image.get(
                                image_id=image_id, column='SORT_ORDER'
                            )
                            q = f"DELETE FROM {Table.Middleware.images} WHERE IMAGE_ID = '{image_id}'"

                        elif image:
                            # Delete Single Image from Image Object
                            prod_id = image.product_id
                            sort_order = image.sort_order
                            if image.shopify_id is None:
                                image.shopify_id = Database.Shopify.Product.Media.Image.get_image_id(
                                    filename=image.name
                                )
                            q = f"DELETE FROM {Table.Middleware.images} WHERE IMAGE_ID = '{image.shopify_id}'"

                        elif product_id:
                            # Delete All Images from Product ID
                            q = f"DELETE FROM {Table.Middleware.images} WHERE PRODUCT_ID = '{product_id}'"

                        else:
                            Database.logger.warn('No image or image_id provided for deletion.')
                            return

                        res = Database.db.query(q)
                        if res['code'] == 200:
                            Database.logger.success(f'{res['affected rows']} images deleted from Middleware.')
                            if (image_id or image) and prod_id:
                                # Decrement sort order of remaining images
                                query = f"""
                                UPDATE {Table.Middleware.images}
                                SET SORT_ORDER = SORT_ORDER - 1
                                WHERE PRODUCT_ID = {prod_id} AND SORT_ORDER > {sort_order}
                                """
                                response = Database.db.query(query)
                                if response['code'] == 200:
                                    Database.logger.success('Decrement Photos: Success')
                                elif response['code'] == 201:
                                    Database.logger.warn('Decrement Photos: No Rows Affected')
                                else:
                                    error = f'Error decrementing sort order of images in Middleware. \nQuery: {query}\nResponse: {response}'
                                    Database.error_handler.add_error_v(
                                        error=error,
                                        origin=f'Database.Shopify.Product.Media.Image.delete(query:\n{q})',
                                    )
                                    raise Exception(error)
                        elif res['code'] == 201:
                            Database.logger.warn(f'IMAGE DELETE: Not found\n\nQuery: {q}\n')
                        else:
                            if image:
                                error = f'Error deleting image {image.name} in Middleware. \nQuery: {q}\nResponse: {res}'
                            elif image_id:
                                error = f'Error deleting image with ID {image_id} in Middleware. \nQuery: {q}\nResponse: {res}'
                            elif product_id:
                                error = f'Error deleting images for product {product_id} in Middleware. \nQuery: {q}\nResponse: {res}'
                            Database.error_handler.add_error_v(
                                error=error, origin=f'Database.Shopify.Product.Media.Image.delete(query:\n{q})'
                            )
                            raise Exception(error)

                class Video:
                    table = Table.Middleware.videos

                    def get(product_id=None, video_id=None, url=None, sku=None, column=None):
                        if column is None:
                            column = '*'

                        if product_id:
                            where_filter = f'WHERE PRODUCT_ID = {product_id}'
                        elif video_id:
                            where_filter = f'WHERE VIDEO_ID = {video_id}'
                        elif url and sku:
                            where_filter = f"WHERE URL = '{url}' AND ITEM_NO = '{sku}'"

                        query = f"""
                        SELECT {column} FROM {Database.Shopify.Product.Media.Video.table}
                        {where_filter}
                        """

                        response = Database.db.query(query)
                        if product_id:
                            return response
                        else:
                            try:
                                return response[0][0]
                            except:
                                return None

                    def insert(video):
                        query = f"""
                        INSERT INTO {Database.Shopify.Product.Media.Video.table} (ITEM_NO, URL, VIDEO_NAME, FILE_PATH, 
                        PRODUCT_ID, VIDEO_ID, SORT_ORDER, BINDING_ID, DESCR, SIZE)
                        VALUES (
                        {f"'{video.sku}'" if video.sku else 'NULL'},
                        {f"'{video.url}'" if video.url else 'NULL'},
                        {f"'{video.name}'" if video.name else 'NULL'}, 
                        {f"'{video.file_path}'" if video.file_path else 'NULL'}, 
                        {video.product_id}, 
                        {video.shopify_id}, 
                        {video.sort_order}, {f"'{video.binding_id}'" if video.binding_id else 'NULL'}, 
                        {f"'{video.description}'" if video.description else 'NULL'}, {video.size if video.size else 'NULL'})
                        """
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(f'Video {video.shopify_id} added to Middleware.')
                        else:
                            error = f'Error adding video {video.shopify_id} to Middleware. \nQuery: {query}\nResponse: {response}'
                            Database.error_handler.add_error_v(error=error)
                            raise Exception(error)

                    def update(video):
                        query = f"""
                        UPDATE {Database.Shopify.Product.Media.Video.table}
                        SET 
                        ITEM_NO = {f"'{video.sku}'" if video.sku else 'NULL'},
                        URL = {f"'{video.url}'" if video.url else 'NULL'},
                        VIDEO_NAME = {f"'{video.name}'" if video.name else 'NULL'}, 
                        FILE_PATH = {f"'{video.file_path}'" if video.file_path else 'NULL'}, 
                        PRODUCT_ID = {video.product_id}, 
                        VIDEO_ID = {video.shopify_id}, 
                        SORT_ORDER = {video.sort_order}, 
                        BINDING_ID = {f"'{video.binding_id}'" if video.binding_id else 'NULL'}, 
                        DESCR = {f"'{video.description}'" if video.description else 'NULL'}, 
                        SIZE = {video.size if video.size else 'NULL'}
                        WHERE ID = {video.db_id}
                        """
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(f'Video {video.shopify_id} updated in Middleware.')
                        elif response['code'] == 201:
                            Database.logger.warn(f'UPDATE: Video {video.shopify_id} not found.\n\nQuery: {query}')
                        else:
                            error = f'Error updating video {video.shopify_id} in Middleware. \nQuery: {query}\nResponse: {response}'
                            Database.error_handler.add_error_v(error=error)
                            raise Exception(error)

                    def delete(video=None, video_id=None, url=None, sku=None, product_id=None):
                        """Delete video from Middleware and decrement the sort order of remaining item videos."""
                        sort_order = None
                        if video:  # Video Object
                            sort_order = video.sort_order
                            product_id = video.product_id
                            where_filter = f'WHERE VIDEO_ID = {video.shopify_id}'
                        else:
                            if video_id:
                                product_id = Database.Shopify.Product.get_id(video_id=video_id)
                                sort_order = Database.Shopify.Product.Media.Video.get(
                                    video_id=video_id, column='SORT_ORDER'
                                )
                                where_filter = f'WHERE VIDEO_ID = {video_id}'
                            elif url and sku:
                                product_id = Database.Shopify.Product.get_id(item_no=sku)
                                sort_order = Database.Shopify.Product.Media.Video.get(
                                    url=url, sku=sku, column='SORT_ORDER'
                                )
                                where_filter = f"WHERE URL = '{url}' AND ITEM_NO = '{sku}'"
                            elif product_id:
                                where_filter = f'WHERE PRODUCT_ID = {product_id}'
                            else:
                                raise Exception('No video_id or product_id provided for deletion.')

                        query = f'DELETE FROM {Database.Shopify.Product.Media.Video.table} {where_filter}'
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            if video_id:
                                Database.logger.success(f'Video {video_id} deleted from Middleware.')
                            elif url and sku:
                                Database.logger.success(f'Video {url} for product {sku} deleted from Middleware.')

                            Database.logger.success(f'{response['affected rows']} videos deleted from Middleware.')

                            if product_id and sort_order:
                                # Decrement sort order of remaining videos
                                query = f"""
                                UPDATE {Database.Shopify.Product.Media.Video.table}
                                SET SORT_ORDER = SORT_ORDER - 1
                                WHERE PRODUCT_ID = {product_id} AND SORT_ORDER > {sort_order}
                                """
                                decrement_response = Database.db.query(query)
                                if decrement_response['code'] == 200:
                                    Database.logger.success('Sort order decremented for remaining videos.')
                                elif decrement_response['code'] == 201:
                                    Database.logger.warn('No rows affected for sort order decrement.')
                                else:
                                    error = f'Error decrementing sort order for remaining videos. \nQuery: {query}\nResponse: {decrement_response}'
                                    Database.error_handler.add_error_v(error=error)
                                    raise Exception(error)

                        elif response['code'] == 201:
                            if video_id:
                                Database.logger.warn(f'DELETE: Video {video_id} not found.')
                            elif product_id:
                                Database.logger.warn(
                                    f'Videos for product {product_id} not found in Middleware.\n\nQuery: {query}\n'
                                )
                            elif url and sku:
                                Database.logger.warn(
                                    f'Video {url} for product {sku} not found in Middleware.\n\nQuery: {query}\n'
                                )
                        else:
                            if video_id:
                                error = f'Error deleting video {video_id} from Middleware. \n Query: {query}\nResponse: {response}'
                            elif product_id:
                                error = f'Error deleting videos for product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
                            elif url and sku:
                                error = f'Error deleting video {url} for product {sku} from Middleware. \n Query: {query}\nResponse: {response}'
                            Database.error_handler.add_error_v(error=error)
                            raise Exception(error)

            class Metafield:
                def delete(product_id):
                    for column in creds.shopify['metafields'].values():
                        query = f"""
                        UPDATE {creds.shopify_product_table}
                        SET {column} = NULL
                        WHERE PRODUCT_ID = {product_id}
                        """
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(f'Metafield {column} deleted from product {product_id}.')
                        elif response['code'] == 201:
                            Database.logger.warn(f'No rows affected for product {product_id} in Middleware.')
                        else:
                            raise Exception(response['message'])

        class Metafield_Definition:
            def get(definition_id=None):
                if definition_id:
                    where_filter = f'WHERE META_ID = {definition_id}'
                else:
                    where_filter = ''

                query = f'SELECT * FROM {Table.Middleware.metafields} {where_filter}'
                response = Database.db.query(query)
                if response is not None:
                    result = {}
                    for row in response:
                        result[row[1]] = {
                            'META_ID': row[0],
                            'NAME': row[1],
                            'DESCR': row[2],
                            'NAME_SPACE': row[3],
                            'META_KEY': row[4],
                            'TYPE': row[5],
                            'PINNED_POS': row[6],
                            'OWNER_TYPE': row[7],
                            'VALIDATIONS': [
                                {'NAME': row[8], 'TYPE': row[9], 'VALUE': row[10]},
                                {'NAME': row[11], 'TYPE': row[12], 'VALUE': row[13]},
                                {'NAME': row[14], 'TYPE': row[15], 'VALUE': row[16]},
                                {'NAME': row[17], 'TYPE': row[18], 'VALUE': row[19]},
                                {'NAME': row[20], 'TYPE': row[21], 'VALUE': row[22]},
                            ],
                            'PIN': row[23],
                        }
                    return result

            def insert(values):
                number_of_validations = len(values['VALIDATIONS'])
                if values['DESCR']:
                    values['DESCR'] = values['DESCR'].replace("'", "''")

                if number_of_validations > 0:
                    validation_columns = ', ' + ', '.join(
                        [
                            f'VALID_{i+1}_NAME, VALID_{i+1}_VALUE, VALID_{i+1}_TYPE'
                            for i in range(number_of_validations)
                        ]
                    )

                    validation_values = ', ' + ', '.join(
                        [
                            f"'{values['VALIDATIONS'][i]['NAME']}', '{values['VALIDATIONS'][i]['VALUE']}', '{values['VALIDATIONS'][i]['TYPE']}'"
                            for i in range(number_of_validations)
                        ]
                    )
                else:
                    validation_columns = ''
                    validation_values = ''

                query = f"""
                        INSERT INTO {Table.Middleware.metafields} (META_ID, NAME, DESCR, NAME_SPACE, META_KEY, 
                        TYPE, PIN, PINNED_POS, OWNER_TYPE {validation_columns})
                        VALUES({values['META_ID']}, '{values['NAME']}', '{values['DESCR']}', 
                        '{values['NAME_SPACE']}', '{values['META_KEY']}', '{values['TYPE']}',
                        {values['PIN']}, {values['PINNED_POS']}, '{values['OWNER_TYPE']}' {validation_values})
                        """

                response = Database.db.query(query)
                if response['code'] != 200:
                    error = f'Error inserting metafield definition {values["META_ID"]}. \nQuery: {query}\nResponse: {response}'
                    raise Exception(error)

            def update(values):
                query = f"""
                        UPDATE {Table.Middleware.metafields}
                        SET META_ID = {values['META_ID']},
                        NAME =  '{values['NAME']}',
                        DESCR = '{values['DESCR']}',
                        NAME_SPACE = '{values['NAME_SPACE']}',
                        META_KEY = '{values['META_KEY']}',
                        TYPE = '{values['TYPE']}',
                        PINNED_POS = {values['PINNED_POS']},
                        OWNER_TYPE = '{values['OWNER_TYPE']}',
                        {', '.join([f"VALID_{i+1}_NAME = '{values['VALIDATIONS'][i]['NAME']}', VALID_{i+1}_VALUE = '{values['VALIDATIONS'][i]['VALUE']}', VALID_{i+1}_TYPE = '{values['VALIDATIONS'][i]['TYPE']}'" for i in range(len(values['VALIDATIONS']))])},
                        LST_MAINT_DT = GETDATE()
                        WHERE META_ID = {values['META_ID']}

                        """
                response = Database.db.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def delete(definition_id=None):
                if definition_id:
                    where_filter = f'WHERE META_ID = {definition_id}'
                else:
                    where_filter = ''
                query = f'DELETE FROM {Table.Middleware.metafields} {where_filter}'
                response = Database.db.query(query)
                if response['code'] not in [200, 201]:
                    raise Exception(response['message'])

        class Webhook:
            def get(id='', ids_only=False):
                if id:
                    query = f'SELECT * FROM {Table.Middleware.webhooks} WHERE HOOK_ID = {id}'
                    response = Database.db.query(query)
                    if response is not None:
                        return response
                else:
                    query = f'SELECT * FROM {Table.Middleware.webhooks}'
                    response = Database.db.query(query)
                    if response is not None:
                        if ids_only:
                            return [hook['id'] for hook in response]
                        return response

            def insert(webhook_data):
                query = f"""
                        INSERT INTO {Table.Middleware.webhooks} (HOOK_ID, TOPIC, DESTINATION, FORMAT, DOMAIN)
                        VALUES ({webhook_data['HOOK_ID']}, '{webhook_data['TOPIC']}', '{webhook_data['DESTINATION']}', '{webhook_data['FORMAT']}', '{webhook_data['DOMAIN']}')
                        """
                response = Database.db.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def update(webhook_data):
                query = f"""
                        UPDATE {Table.Middleware.webhooks}
                        SET TOPIC = '{webhook_data['TOPIC']}', 
                        DESTINATION = '{webhook_data['DESTINATION']}', 
                        FORMAT = '{webhook_data['FORMAT']}',
                        DOMAIN = '{webhook_data['DOMAIN']}'
                        WHERE HOOK_ID = {webhook_data['HOOK_ID']}
                        """
                response = Database.db.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def delete(hook_id=None, all=False):
                if all:
                    response = Database.db.query(f'DELETE FROM {Table.Middleware.webhooks}')
                    if response['code'] != 200:
                        raise Exception(response['message'])
                    else:
                        return 'All webhooks deleted'
                elif hook_id:
                    response = Database.db.query(
                        f'DELETE FROM {Table.Middleware.webhooks} WHERE HOOK_ID = {hook_id}'
                    )
                    if response['code'] != 200:
                        raise Exception(response['message'])
                    else:
                        return f'Webhook {hook_id} deleted'

        class Discount:
            def get(group_code=None):
                if group_code:
                    query = f"SELECT SHOP_ID FROM {Table.Middleware.discounts} WHERE GRP_COD = '{group_code}'"
                else:
                    query = f'SELECT GRP_COD FROM {Table.Middleware.discounts}'
                response = Database.db.query(query)
                return [x[0] for x in response] if response else None

            def sync(price_rule):
                if price_rule.db_id:
                    Database.Shopify.Discount.update(price_rule)
                else:
                    Database.Shopify.Discount.insert(price_rule)

            def insert(rule):
                """Insert a new discount rule into the Middleware."""
                query = f"""
                INSERT INTO SN_PROMO(GRP_COD, RUL_SEQ_NO, SHOP_ID, ENABLED)
                VALUES('{rule.grp_cod}', {rule.seq_no},{rule.shopify_id}, {1 if rule.is_enabled_cp else 0})
                """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(
                        f'Promotion {rule.grp_cod}-Rule: {rule.seq_no} inserted successfully into Middleware.'
                    )
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                        origin='Middleware Promotion Insertion',
                    )

            def update(rule):
                query = f"""
                UPDATE SN_PROMO
                SET RUL_SEQ_NO = {rule.seq_no},
                SHOP_ID = {rule.shopify_id}, 
                ENABLED = {1 if rule.is_enabled_cp else 0}, 
                LST_MAINT_DT = GETDATE()
                WHERE GRP_COD = '{rule.grp_cod}'
                """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(
                        f'Promotion {rule.grp_cod}-Rule: {rule.seq_no} updated successfully in Middleware.'
                    )
                elif response['code'] == 201:
                    Database.logger.warn(
                        f'Promotion {rule.grp_cod}-Rule: {rule.seq_no} not found for update in Middleware.'
                    )
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                        origin='Middleware Promotion Update',
                    )

            def delete(shopify_id):
                if not shopify_id:
                    Database.logger.warn('No Shopify ID provided for deletion.')
                    return
                query = f'DELETE FROM SN_PROMO WHERE SHOP_ID = {shopify_id}'
                response = Database.db.query(query)

                if response['code'] == 200:
                    Database.logger.success(f'DELETE: Promotion {shopify_id} deleted successfully from Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'DELETE: Promotion {shopify_id} not found in Middleware.')
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                        origin='Middleware Promotion Deletion',
                    )

            class Line:
                def get(shopify_id):
                    if not shopify_id:
                        Database.logger.warn('No Shopify ID provided for line item lookup.')
                        return
                    query = f'SELECT ITEM_NO FROM {Table.Middleware.discount_lines} WHERE SHOP_ID = {shopify_id}'
                    response = Database.db.query(query)
                    return [x[0] for x in response] if response else None

                def insert(items, shopify_promo_id):
                    """Insert Items affected by BOGO promos into middleware."""
                    if not items:
                        Database.logger.warn('No items provided for insertion.')
                        return
                    for i in items:
                        query = f"""
                        INSERT INTO {Table.Middleware.discount_lines} (SHOP_ID, ITEM_NO)
                        VALUES ({shopify_promo_id}, '{i}')"""
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(f'Promotion {i} inserted successfully into Middleware.')
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                                origin='Middleware Promotion Line Item Insertion',
                            )

                def delete(shopify_id=None, item_no_list=None):
                    if not shopify_id and not item_no_list:
                        Database.logger.warn('MW Discount Line Delete: Must provide promo id or item number list.')
                        return
                    if shopify_id:
                        # Delete all line items for a specific promotion
                        query = f'DELETE FROM {Table.Middleware.discount_lines} WHERE SHOP_ID = {shopify_id}'
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(
                                f'DELETE: Promotion {shopify_id} deleted successfully from Middleware.'
                            )
                        elif response['code'] == 201:
                            Database.logger.warn(f'DELETE: Promotion {shopify_id} not found in Middleware.')
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                                origin='Middleware Promotion Line Item Deletion',
                            )
                        return
                    elif item_no_list:
                        # Delete all line items for a list of item numbers
                        if len(item_no_list) == 1:
                            where_filter = f'ITEM_NO = {item_no_list[0]}'
                        else:
                            where_filter = f'ITEM_NO IN {tuple(item_no_list)}'
                        query = f'DELETE FROM {Table.Middleware.discount_lines} WHERE {where_filter}'
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            Database.logger.success(
                                f'DELETE: Promotion {item_no_list} deleted successfully from Middleware.'
                            )
                        elif response['code'] == 201:
                            Database.logger.warn(
                                f'DELETE: No Promotion lines for {item_no_list} found in Middleware.'
                            )
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                                origin='Middleware Promotion Line Item Deletion',
                            )

        class Gift_Certificate:
            pass


if __name__ == '__main__':
    print(Database.Shopify.Discount.get('TEST'))
