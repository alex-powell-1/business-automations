from setup import creds
from setup import query_engine
from setup.error_handler import ProcessOutErrorHandler
from datetime import datetime, timedelta
from setup.utilities import format_phone


class Database:
    db = query_engine.QueryEngine
    error_handler = ProcessOutErrorHandler.error_handler
    logger = ProcessOutErrorHandler.logger

    def create_tables():
        tables = {
            'design_leads': f"""
                                        CREATE TABLE {creds.design_leads_table} (
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
                                        CREATE TABLE {creds.qr_table} (
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
                                        CREATE TABLE {creds.qr_activity_table} (
                                        SCAN_DT datetime NOT NULL DEFAULT(current_timestamp) PRIMARY KEY,
                                        CODE varchar(100) NOT NULL FOREIGN KEY REFERENCES SN_QR(QR_CODE),
                                        );""",
            'sms': f"""
                                        CREATE TABLE {creds.sms_table}(
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
                                        CREATE TABLE {creds.sms_event_table}(
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
                SELECT * FROM {creds.design_leads_table}
                WHERE DATE > '{datetime.now().date() - timedelta(days=1)}' AND DATE < '{datetime.now().date()}'
                """
            else:
                query = f"""
                    SELECT * FROM {creds.design_leads_table}
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
                INSERT INTO {creds.design_leads_table} (DATE, CUST_NO, FST_NAM, LST_NAM, EMAIL, PHONE, SKETCH, SCALED, DIGITAL, 
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
                SELECT * FROM {creds.sms_table}
                WHERE CUST_NO = '{cust_no}'
                """
            else:
                query = f"""
                SELECT * FROM {creds.sms_table}
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
                INSERT INTO {creds.sms_table} (ORIGIN, CAMPAIGN, DIRECTION, TO_PHONE, FROM_PHONE, CUST_NO, BODY, USERNAME, NAME, CATEGORY, MEDIA, SID, ERROR_CODE, ERROR_MESSAGE)
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
        class Product:
            table = creds.cp_item_table
            columns = {
                'item_no': creds.column_product_item_no,
                'web_enabled': creds.column_product_web_enabled,
                'web_visible': creds.column_product_web_visible,
                'binding_id': creds.column_product_binding_id,
                'is_parent': creds.column_product_is_parent,
                'variant_name': creds.column_product_variant_name,
                'weight': creds.column_product_weight,
                'brand': creds.column_product_brand,
                'web_title': creds.column_product_web_title,
                'meta_title': creds.column_product_meta_title,
                'meta_description': creds.column_product_meta_description,
                'alt_text_1': creds.column_product_alt_text_1,
                'alt_text_2': creds.column_product_alt_text_2,
                'alt_text_3': creds.column_product_alt_text_3,
                'alt_text_4': creds.column_product_alt_text_4,
                'videos': creds.column_product_videos,
                'featured': creds.column_product_featured,
            }

            def update(payload):
                """FOR PRODUCTS_UPDATE WEBHOOK ONLY. Normal updates from shopify_catalog.py use sync()"""
                query = f'UPDATE {Database.Counterpoint.Product.table} SET '
                if 'status' in payload:
                    if payload['status'] == 'active':
                        query += f"{Database.Counterpoint.Product.columns['web_visible']} = 'Y', "
                    else:
                        query += f"{Database.Counterpoint.Product.columns['web_visible']} = 'N', "
                if 'title' in payload:
                    title = payload['title'].replace("'", "''")[:80]  # 80 char limit
                    query += f"{Database.Counterpoint.Product.columns['web_title']} = '{title}', "
                if 'meta_title' in payload:
                    meta_title = payload['meta_title'].replace("'", "''")[:80]  # 80 char limit
                    query += f"{Database.Counterpoint.Product.columns['meta_title']} = '{meta_title}', "
                if 'meta_description' in payload:
                    meta_description = payload['meta_description'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Database.Counterpoint.Product.columns['meta_description']} = '{meta_description}', "
                if 'alt_text_1' in payload:
                    alt_text_1 = payload['alt_text_1'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Database.Counterpoint.Product.columns['alt_text_1']} = '{alt_text_1}', "
                if 'alt_text_2' in payload:
                    alt_text_2 = payload['alt_text_2'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Database.Counterpoint.Product.columns['alt_text_2']} = '{alt_text_2}', "
                if 'alt_text_3' in payload:
                    alt_text_3 = payload['alt_text_3'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Database.Counterpoint.Product.columns['alt_text_3']} = '{alt_text_3}', "
                if 'alt_text_4' in payload:
                    alt_text_4 = payload['alt_text_4'].replace("'", "''")[:160]  # 160 char limit
                    query += f"{Database.Counterpoint.Product.columns['alt_text_4']} = '{alt_text_4}', "

                if query[-2:] == ', ':
                    query = query[:-2]

                query += f" WHERE ITEM_NO = '{payload['item_no']}'"

                print(f'\n\n\n{query}\n\n\n')
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
                    SET HTML_DESCR = '{description}', LST_MAINT_DT = GETDATE()
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
                        SELECT ITEM_NO, {Database.Counterpoint.Product.columns['videos']} 
                        FROM {Database.Counterpoint.Product.table}
                        WHERE {Database.Counterpoint.Product.columns['videos']} IS NOT NULL AND
                        {Database.Counterpoint.Product.columns['web_enabled']} = 'Y'
                        """
                        response = Database.db.query(query)
                        all_videos = [[x[0], x[1]] for x in response] if response else []
                        if all_videos:
                            for entry in all_videos:
                                if ',' in entry[1]:
                                    multi_video_list = entry[1].replace(' ', '').split(',')
                                    for video in multi_video_list:
                                        all_videos.append([entry[0], video])
                                    all_videos.remove(entry)
                        return all_videos

        class Customer:
            table = creds.cp_customer_table

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
                FROM {Database.Counterpoint.Customer.table} CP
                FULL OUTER JOIN {creds.shopify_customer_table} MW on CP.CUST_NO = MW.cust_no
                WHERE CP.LST_MAINT_DT > '{last_sync}' and CUST_NAM_TYP = 'P' {customer_filter}
                """
                return Database.db.query(query)

            def update_timestamps(customer_list):
                if len(customer_list) == 1:
                    customer_list = f"('{customer_list[0]}')"
                else:
                    customer_list = str(tuple(customer_list))
                query = f"""
                UPDATE {Database.Counterpoint.Customer.table}
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
                    FROM AR_SHIP_ADRS
                    WHERE CUST_NO = '{cust_no}'"""
                    return Database.db.query(query)

    class Shopify:
        def rebuild_tables(self):
            def create_tables():
                tables = {
                    'categories': f"""
                                            CREATE TABLE {Database.Shopify.category_table} (
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
                                            CREATE TABLE {Database.Shopify.Product.table} (                                        
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
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'images': f"""
                                            CREATE TABLE {Database.Shopify.image_table} (
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
                                            CREATE TABLE {Database.Shopify.video_table} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO varchar(50),
                                            URL nvarchar(255),
                                            VIDEO_NAME nvarchar(255),
                                            FILE_PATH nvarchar(255),
                                            PRODUCT_ID bigint,
                                            VIDEO_ID bigint,
                                            VIDEO_NUMBER int DEFAULT(1),
                                            SORT_ORDER int,
                                            BINDING_ID varchar(50),
                                            DESCR nvarchar(255),
                                            SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'customers': f"""
                                            CREATE TABLE {Database.Shopify.customer_table} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            CUST_NO varchar(50) NOT NULL,
                                            SHOP_CUST_ID bigint,
                                            META_CUST_NO bigint,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'orders': f"""
                                            CREATE TABLE {Database.Shopify.order_table} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            ORDER_NO int NOT NULL,
                                            DOC_ID bigint,
                                            STATUS bit DEFAULT(0)
                                            )""",
                    'gift': f"""
                                            CREATE TABLE {Database.Shopify.gift_cert_table} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            GFC_NO varchar(30) NOT NULL,
                                            BC_GFC_ID int NOT NULL,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            )""",
                    'promo': f""" 
                                            CREATE TABLE {Database.Shopify.promo_table}(
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            GRP_COD nvarchar(50) NOT NULL,
                                            RUL_SEQ_NO int,
                                            BC_ID int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );""",
                    'metafields': f""" 
                                            CREATE TABLE {Database.Shopify.metafield_table}(
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
            table = creds.shopify_customer_table

            def get(customer_no=None):
                if customer_no:
                    query = f"""
                            SELECT * FROM {Database.Shopify.Customer.table}
                            WHERE CUST_NO = '{customer_no}'
                            """
                else:
                    query = f"""
                            SELECT * FROM {Database.Shopify.Customer.table}
                            """
                return Database.db.query(query)

            def exists(shopify_cust_no):
                query = f"""
                        SELECT * FROM {Database.Shopify.Customer.table}
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
                        INSERT INTO {Database.Shopify.Customer.table} (CUST_NO, SHOP_CUST_ID, 
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
                        UPDATE {Database.Shopify.Customer.table}
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
                query = f'DELETE FROM {Database.Shopify.Customer.table} WHERE SHOP_CUST_ID = {shopify_cust_no}'
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
            table = creds.shopify_order_table

        class Collection:
            table = creds.shopify_collection_table

            def get_cp_categ_id(collection_id):
                query = f"""
                        SELECT CP_CATEG_ID FROM {creds.shopify_collection_table}
                        WHERE COLLECTION_ID = {collection_id}
                        """
                response = Database.db.query(query)
                try:
                    return response[0][0]
                except:
                    return None

            def insert(category):
                query = f"""
                INSERT INTO {creds.shopify_collection_table}(COLLECTION_ID, MENU_ID, CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, 
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
                UPDATE {creds.shopify_collection_table}
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
                        DELETE FROM {creds.shopify_collection_table}
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
                    print(query)
                    raise Exception(response['message'])

        class Product:
            table = creds.shopify_product_table

            def get_by_category(cp_category=None, cp_subcategory=None):
                query = f"""SELECT ITEM_NO FROM {Database.Counterpoint.Product.table} 
                WHERE {Database.Counterpoint.Product.columns['web_enabled']} = 'Y' AND 
                ({Database.Counterpoint.Product.columns['binding_id']} IS NULL OR
                {Database.Counterpoint.Product.columns['is_parent']} = 'Y') AND 
                """
                if cp_category and cp_subcategory:
                    query += f" CATEG_COD = '{cp_category}' AND SUBCAT_COD = '{cp_subcategory}'"
                else:
                    if cp_category:
                        query += f" CATEG_COD = '{cp_category}'"
                    if cp_subcategory:
                        query += f" SUBCAT_COD = '{cp_subcategory}'"

                sku_list = [x[0] for x in Database.db.query(query)] if Database.db.query(query) else None

                if sku_list:
                    product_list = []
                    for sku in sku_list:
                        product_list.append(Database.Shopify.Product.get_id(item_no=sku))
                    return product_list

            def get_id(item_no=None, binding_id=None, image_id=None):
                """Get product ID from SQL using image ID. If not found, return None."""
                if item_no:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Database.Shopify.Product.table} WHERE ITEM_NO = '{item_no}'"
                    )
                if image_id:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {creds.shopify_image_table} WHERE IMAGE_ID = '{image_id}'"
                    )
                if binding_id:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Database.Shopify.Product.table} WHERE BINDING_ID = '{binding_id}'"
                    )

                if item_no or image_id or binding_id:
                    prod_id_res = Database.db.query(product_query)
                    if prod_id_res is not None:
                        return prod_id_res[0][0]

            def get_parent_item_no(product_id):
                query = f"""
                        SELECT ITEM_NO FROM {Database.Shopify.Product.table}
                        WHERE PRODUCT_ID = {product_id} AND (BINDING_ID IS NULL OR IS_PARENT = 1)
                        """
                response = Database.db.query(query)
                if response is not None:
                    return response[0][0]

            def sync(product):
                for variant in product.variants:
                    if variant.mw_db_id:
                        Database.Shopify.Product.Variant.update(product=product, variant=variant)
                    else:
                        Database.Shopify.Product.Variant.insert(product=product, variant=variant)

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
                query = f'DELETE FROM {Database.Shopify.Product.table} WHERE PRODUCT_ID = {product_id}'
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
                def get_variant_id(sku):
                    query = f"""
                        SELECT VARIANT_ID FROM {creds.shopify_product_table}
                        WHERE ITEM_NO = {sku}
                        """
                    response = Database.db.query(query)
                    if response is not None:
                        return response[0][0]

                def get_option_id(sku):
                    query = f"""
                        SELECT OPTION_ID FROM {creds.shopify_product_table}
                        WHERE ITEM_NO = {sku}
                        """
                    response = Database.db.query(query)
                    if response is not None:
                        return response[0][0]

                def get_option_value_id(sku):
                    query = f"""
                        SELECT OPTION_VALUE_ID FROM {creds.shopify_product_table}
                        WHERE ITEM_NO = {sku}
                        """
                    response = Database.db.query(query)
                    if response is not None:
                        return response[0][0]

                def insert(product, variant):
                    if product.shopify_collections:
                        collection_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        collection_string = None

                    insert_query = f"""
                        INSERT INTO {Database.Shopify.Product.table} (ITEM_NO, BINDING_ID, IS_PARENT, 
                        PRODUCT_ID, VARIANT_ID, INVENTORY_ID, VARIANT_NAME, OPTION_ID, OPTION_VALUE_ID, CATEG_ID, 
                        CF_BOTAN_NAM, CF_PLANT_TYP, CF_HEIGHT, CF_WIDTH, CF_CLIM_ZON, CF_CLIM_ZON_LST,
                        CF_COLOR, CF_SIZE, CF_BLOOM_SEAS, CF_BLOOM_COLOR, CF_LIGHT_REQ, CF_FEATURES, CF_IS_PREORDER, 
                        CF_PREORDER_DT, CF_PREORDER_MSG, CF_IS_FEATURED, CF_IN_STORE_ONLY
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
                        {product.meta_in_store_only['id'] if product.meta_in_store_only['id'] else "NULL"}
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
                        UPDATE {Database.Shopify.Product.table} 
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
                    query = f'DELETE FROM {Database.Shopify.Product.table} WHERE VARIANT_ID = {variant_id}'
                    response = Database.db.query(query)

                    if response['code'] == 200:
                        Database.logger.success(f'Variant {variant_id} deleted from Middleware.')
                    else:
                        error = f'Error deleting variant {variant_id} from Middleware. \n Query: {query}\nResponse: {response}'
                        Database.error_handler.add_error_v(error=error)
                        raise Exception(error)

            class Media:
                def delete(product_id):
                    Database.Shopify.Product.Media.Image.delete(product_id=product_id)
                    Database.Shopify.Product.Media.Video.delete(product_id=product_id)

                class Image:
                    table = creds.shopify_image_table

                    def get_image_id(file_name):
                        img_id_res = Database.db.query(
                            f"SELECT IMAGE_ID FROM {creds.shopify_image_table} WHERE IMAGE_NAME = '{file_name}'"
                        )
                        try:
                            return img_id_res[0][0]
                        except:
                            return None

                    def insert(image):
                        img_insert = f"""
                        INSERT INTO {creds.shopify_image_table} (IMAGE_NAME, ITEM_NO, FILE_PATH,
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
                        UPDATE {creds.shopify_image_table}
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
                        if image_id:
                            # Delete Single Image from Image ID
                            q = f"DELETE FROM {creds.shopify_image_table} WHERE IMAGE_ID = '{image_id}'"
                        elif image:
                            # Delete Single Image from Image Object
                            if image.shopify_id is None:
                                image.shopify_id = Database.Shopify.Product.Media.Image.get_image_id(
                                    filename=image.name
                                )
                            q = f"DELETE FROM {creds.shopify_image_table} WHERE IMAGE_ID = '{image.shopify_id}'"
                        elif product_id:
                            # Delete All Images from Product ID
                            q = f"DELETE FROM {creds.shopify_image_table} WHERE PRODUCT_ID = '{product_id}'"
                        else:
                            Database.logger.warn('No image or image_id provided for deletion.')
                            return
                        res = Database.db.query(q)
                        print(res['code'])
                        if res['code'] == 200:
                            Database.logger.success(f'Query: {q}\nSQL DELETE Image')
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
                    table = creds.shopify_video_table

                    def get(product_id):
                        query = f"""
                        SELECT * FROM {Database.Shopify.Product.Media.Video.table}
                        WHERE PRODUCT_ID = {product_id}
                        """
                        return Database.db.query(query)

                    def insert(video):
                        query = f"""
                        INSERT INTO {Database.Shopify.Product.Media.Video.table} (ITEM_NO, URL, VIDEO_NAME, FILE_PATH, 
                        PRODUCT_ID, VIDEO_ID, VIDEO_NUMBER, SORT_ORDER, BINDING_ID, DESCR, SIZE)
                        VALUES (
                        {f"'{video.sku}'" if video.sku else 'NULL'},
                        {f"'{video.url}'" if video.url else 'NULL'},
                        {f"'{video.name}'" if video.name else 'NULL'}, 
                        {f"'{video.file_path}'" if video.file_path else 'NULL'}, 
                        {video.product_id}, 
                        {video.shopify_id}, 
                        {video.number}, 
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
                        VIDEO_NUMBER = {video.number}, 
                        SORT_ORDER = {video.sort_order}, 
                        BINDING_ID = {f"'{video.binding_id}'" if video.binding_id else 'NULL'}, 
                        DESCR = {f"'{video.description}'" if video.description else 'NULL'}, 
                        SIZE = {video.size if video.size else 'NULL'}
                        WHERE PRODUCT_ID = {video.product_id}
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

                    def delete(video_id=None, product_id=None):
                        if video_id:
                            where_filter = f'WHERE VIDEO_ID = {video_id}'
                        elif product_id:
                            where_filter = f'WHERE PRODUCT_ID = {product_id}'
                        else:
                            raise Exception('No video_id or product_id provided for deletion.')
                        query = f'DELETE FROM {Database.Shopify.Product.Media.Video.table} {where_filter}'
                        response = Database.db.query(query)
                        if response['code'] == 200:
                            if video_id:
                                Database.logger.success(f'Video {video_id} deleted from Middleware.')
                            elif product_id:
                                Database.logger.success(f'Videos for product {product_id} deleted from Middleware.')
                        elif response['code'] == 201:
                            if video_id:
                                Database.logger.warn(f'DELETE: Video {video_id} not found.')
                            elif product_id:
                                Database.logger.warn(
                                    f'Videos for product {product_id} not found in Middleware.\n\nQuery: {query}\n'
                                )
                        else:
                            if video_id:
                                error = f'Error deleting video {video_id} from Middleware. \n Query: {query}\nResponse: {response}'
                            elif product_id:
                                error = f'Error deleting videos for product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
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
            table = creds.shopify_metafield_table

            def get(definition_id=None):
                if definition_id:
                    where_filter = f'WHERE META_ID = {definition_id}'
                else:
                    where_filter = ''

                query = f'SELECT * FROM {creds.shopify_metafield_table} {where_filter}'
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
                print(values)
                number_of_validations = len(values['VALIDATIONS'])
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
                        INSERT INTO {creds.shopify_metafield_table} (META_ID, NAME, DESCR, NAME_SPACE, META_KEY, 
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
                        UPDATE {creds.shopify_metafield_table}
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
                query = f'DELETE FROM {creds.shopify_metafield_table} {where_filter}'
                response = Database.db.query(query)
                if response['code'] not in [200, 201]:
                    raise Exception(response['message'])

        class Webhook:
            table = creds.shopify_webhook_table

            def get(id='', ids_only=False):
                if id:
                    query = f'SELECT * FROM {Database.Shopify.Webhook.table} WHERE HOOK_ID = {id}'
                    response = Database.db.query(query)
                    if response is not None:
                        return response
                else:
                    query = f'SELECT * FROM {Database.Shopify.Webhook.table}'
                    response = Database.db.query(query)
                    if response is not None:
                        if ids_only:
                            return [hook['id'] for hook in response]
                        return response

            def insert(webhook_data):
                query = f"""
                        INSERT INTO {Database.Shopify.Webhook.table} (HOOK_ID, TOPIC, DESTINATION, FORMAT, DOMAIN)
                        VALUES ({webhook_data['HOOK_ID']}, '{webhook_data['TOPIC']}', '{webhook_data['DESTINATION']}', '{webhook_data['FORMAT']}', '{webhook_data['DOMAIN']}')
                        """
                response = Database.db.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def update(webhook_data):
                query = f"""
                        UPDATE {Database.Shopify.Webhook.table}
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
                    response = Database.db.query(f'DELETE FROM {Database.Shopify.Webhook.table}')
                    if response['code'] != 200:
                        raise Exception(response['message'])
                    else:
                        return 'All webhooks deleted'
                elif hook_id:
                    response = Database.db.query(
                        f'DELETE FROM {Database.Shopify.Webhook.table} WHERE HOOK_ID = {hook_id}'
                    )
                    if response['code'] != 200:
                        raise Exception(response['message'])
                    else:
                        return f'Webhook {hook_id} deleted'

        class Promotion:
            table = creds.shopify_promo_table

        class Gift_Certificate:
            table = creds.shopify_gift_cert_table


if __name__ == '__main__':
    video_list = Database.Counterpoint.Product.Media.Video.get()
    for x in video_list:
        print(x)
