from setup import creds
from setup import query_engine
from setup.error_handler import ProcessOutErrorHandler
from datetime import datetime
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
        }
        for table in tables:
            Database.db.query(tables[table])

    class DesignLead:
        def get():
            query = f"""
                SELECT * FROM {creds.design_leads_table}
                """
            return Database.db.query(query)

        def insert(
            date,
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
                INSERT INTO {creds.design_leads_table} (DATE, FST_NAM, LST_NAM, EMAIL, PHONE, SKETCH, SCALED, DIGITAL, 
                ON_SITE, DELIVERY, INSTALL, TIMELINE, STREET, CITY, STATE, ZIP, COMMENTS)
                VALUES ('{date}', '{first_name}', '{last_name}', '{email}', '{phone}', {sketch}, 
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
                '{cust_no}', '{body}', {f"'{username}'" if username else 'NULL'}, '{name}', 
                '{category}', {f"'{media}'" if media else 'NULL'}, {f"'{sid}'" if sid else 'NULL'}, 
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
        class Customer:
            table = creds.ar_cust_table

            def get(last_sync=datetime(1970, 1, 1), customer_no=None, customer_list=None):
                if customer_no:
                    customer_filter = f"AND CP.CUST_NO = '{customer_no}'"
                elif customer_list:
                    customer_filter = f'AND CP.CUST_NO IN {tuple(customer_list)}'
                else:
                    customer_filter = ''

                query = f"""
                SELECT cp.CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, ADRS_1, CITY, STATE, ZIP_COD, CNTRY,
                MW.SHOP_CUST_ID
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
                    'customers': f"""
                                            CREATE TABLE {Database.Shopify.customer_table} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            CUST_NO varchar(50) NOT NULL,
                                            SHOP_CUST_ID int,
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

            def insert(customer):
                query = f"""
                        INSERT INTO {Database.Shopify.Customer.table} (CUST_NO, SHOP_CUST_ID)
                        VALUES ('{customer.cp_cust_no}', {customer.shopify_cust_no})
                        """
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {customer.cust_no} added to Middleware.')
                else:
                    error = f'Error adding customer {customer.cust_no} to Middleware. \nQuery: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(customer):
                query = f"""
                        UPDATE {Database.Shopify.Customer.table}
                        SET SHOP_CUST_ID = {customer.shopify_cust_no}, LST_MAINT_DT = GETDATE()
                        WHERE CUST_NO = '{customer.cp_cust_no}'
                        """

                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {customer.cust_no} updated in Middleware.')
                else:
                    error = f'Error updating customer {customer.cust_no} in Middleware. \nQuery: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def delete(customer):
                query = f'DELETE FROM {Database.Shopify.Customer.table} WHERE CUST_NO = {customer.cp_cust_no}'
                response = Database.db.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {customer.cp_cust_no} deleted from Middleware.')
                else:
                    error = f'Error deleting customer {customer.cp_cust_no} from Middleware. \n Query: {query}\nResponse: {response}'
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

            def sync(product):
                for variant in product.variants:
                    if variant.mw_db_id:
                        Database.Shopify.Product.Variant.update(product=product, variant=variant)
                    else:
                        Database.Shopify.Product.Variant.insert(product=product, variant=variant)

                for image in product.images:
                    if image.db_id is None:
                        Database.Shopify.Product.Image.insert(image)
                    else:
                        Database.Shopify.Product.Image.update(image)

            def insert(product):
                for variant in product.variants:
                    Database.Shopify.Product.Variant.insert(product, variant)

            def delete(product_id):
                query = f'DELETE FROM {Database.Shopify.Product.table} WHERE PRODUCT_ID = {product_id}'
                response = Database.db.query(query)

                if response['code'] == 200:
                    Database.logger.success(f'Product {product_id} deleted from Middleware.')
                else:
                    error = f'Error deleting product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

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
                    '{image.file_path}', {image.product_id}, {image.image_id}, '{1 if image.is_thumbnail else 0}', 
                    '{image.image_number}', '{image.sort_order}', '{image.is_binding_image}',
                    {f"'{image.binding_id}'" if image.binding_id else 'NULL'}, '{image.is_variant_image}',
                    {f"'{image.description.replace("'", "''")}'" if image.description != '' else 'NULL'},
                    {image.size})"""
                    insert_img_response = Database.db.query(img_insert)
                    if insert_img_response['code'] == 200:
                        Database.logger.success(f'SQL INSERT Image {image.name}: Success')
                    else:
                        error = f'Error inserting image {image.name} into Middleware. \nQuery: {img_insert}\nResponse: {insert_img_response}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Image.insert({image.name})'
                        )
                        raise Exception(error)

                def update(image):
                    q = f"""
                    UPDATE {creds.shopify_image_table}
                    SET IMAGE_NAME = '{image.name}', ITEM_NO = '{image.sku}', FILE_PATH = '{image.file_path}',
                    PRODUCT_ID = '{image.product_id}', IMAGE_ID = '{image.image_id}',
                    THUMBNAIL = '{1 if image.is_thumbnail else 0}', IMAGE_NUMBER = '{image.image_number}',
                    SORT_ORDER = '{image.sort_order}', IS_BINDING_IMAGE = '{image.is_binding_image}',
                    BINDING_ID = {f"'{image.binding_id}'" if image.binding_id else 'NULL'},
                    IS_VARIANT_IMAGE = '{image.is_variant_image}',
                    DESCR = {f"'{image.description.replace("'", "''")}'" if
                                image.description != '' else 'NULL'}, SIZE = '{image.size}'
                    WHERE ID = {image.db_id}"""

                    res = Database.db.query(q)
                    if res['code'] == 200:
                        Database.logger.success(f'SQL UPDATE Image {image.name}: Success')
                    else:
                        error = f'Error updating image {image.name} in Middleware. \nQuery: {q}\nResponse: {res}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Image.update({image.name})'
                        )
                        raise Exception(error)

                def delete(image=None, image_id=None, product_id=None):
                    if image_id:
                        # Delete Single Image from Image ID
                        q = f"DELETE FROM {creds.shopify_image_table} WHERE IMAGE_ID = '{image_id}'"
                    elif image:
                        # Delete Single Image from Image Object
                        if image.image_id is None:
                            image.image_id = Database.Shopify.Product.Image.get_image_id(filename=image.name)
                        q = f"DELETE FROM {creds.shopify_image_table} WHERE IMAGE_ID = '{image.image_id}'"
                    elif product_id:
                        # Delete All Images from Product ID
                        q = f"DELETE FROM {creds.shopify_image_table} WHERE PRODUCT_ID = '{product_id}'"
                    else:
                        Database.logger.warn('No image or image_id provided for deletion.')
                        return
                    res = Database.db.query(q)
                    if res['code'] == 200:
                        Database.logger.success(f'Query: {q}\nSQL DELETE Image')
                    else:
                        error = f'Error deleting image {image.name} in Middleware. \nQuery: {q}\nResponse: {res}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Image.delete(query:\n{q})'
                        )
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
                        if response['code'] != 200:
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
                if response['code'] != 200:
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
    print(Database.DesignLead.get())
