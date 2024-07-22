from setup import creds
from setup import query_engine


class Database:
    db = query_engine.QueryEngine()

    def rebuild_tables(self):
        def create_tables():
            tables = {
                'categories': f"""
                                        CREATE TABLE {creds.shopify_category_table} (
                                        CATEG_ID int IDENTITY(1,1) PRIMARY KEY,
                                        COLLECTION_ID bigint,
                                        CP_CATEG_ID bigint NOT NULL,
                                        CP_PARENT_ID bigint,
                                        CATEG_NAME nvarchar(255) NOT NULL,
                                        SORT_ORDER int,
                                        DESCRIPTION text,
                                        IS_VISIBLE BIT NOT NULL DEFAULT(1),
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                'brands': f"""
                                        CREATE TABLE {creds.shopify_brands_table} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        CP_BRAND_ID nvarchar(50) NOT NULL,
                                        BC_BRAND_ID int,
                                        NAME nvarchar(255) NOT NULL,
                                        PAGE_TITLE nvarchar(255) NOT NULL,
                                        META_KEYWORDS nvarchar(255),
                                        META_DESCR nvarchar(255),
                                        SEARCH_KEYWORDS nvarchar(255),
                                        IMAGE_NAME nvarchar(255),
                                        IMAGE_URL nvarchar(255),
                                        IMAGE_FILEPATH nvarchar(255),
                                        IMAGE_SIZE int,
                                        IS_CUSTOMIZED BIT,
                                        CUSTOM_URL nvarchar(255),
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                'products': f"""
                                        CREATE TABLE {creds.shopify_product_table} (                                        
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        ITEM_NO varchar(50) NOT NULL,
                                        BINDING_ID varchar(10),
                                        IS_PARENT BIT,
                                        PRODUCT_ID bigint NOT NULL,
                                        VARIANT_ID bigint,
                                        VARIANT_NAME nvarchar(255),
                                        OPTION_ID bigint,
                                        OPTION_VALUE_ID bigint,
                                        CATEG_ID varchar(255),
                                        CUSTOM_FIELDS varchar(255),
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                'images': f"""
                                        CREATE TABLE {creds.shopify_image_table} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        IMAGE_NAME nvarchar(255) NOT NULL,
                                        ITEM_NO varchar(50),
                                        FILE_PATH nvarchar(255) NOT NULL,
                                        IMAGE_URL nvarchar(255),
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
                                        CREATE TABLE {creds.bc_customer_table} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        CUST_NO varchar(50) NOT NULL,
                                        BC_CUST_ID int,
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                'orders': f"""
                                        CREATE TABLE {creds.bc_order_table} (
                                        ID int IDENTITY(1, 1) PRIMARY KEY,
                                        ORDER_NO int NOT NULL,
                                        DOC_ID bigint,
                                        STATUS bit DEFAULT(0)
                                        )""",
                'gift': f"""
                                        CREATE TABLE {creds.bc_gift_cert_table} (
                                        ID int IDENTITY(1, 1) PRIMARY KEY,
                                        GFC_NO varchar(30) NOT NULL,
                                        BC_GFC_ID int NOT NULL,
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        )""",
                'promo': f""" 
                                        CREATE TABLE {creds.bc_promo_table}(
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        GRP_COD nvarchar(50) NOT NULL,
                                        RUL_SEQ_NO int,
                                        BC_ID int,
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );""",
                'metafields': f""" 
                                        CREATE TABLE {creds.shopify_metafield_table}(
                                        META_ID bigint NOT NULL, 
                                        NAME varchar(50) NOT NULL, 
                                        NAME_SPACE varchar(50), 
                                        META_KEY varchar(50), 
                                        TYPE varchar(50), 
                                        PIN bit, 
                                        OWNER_TYPE varchar(50),
                                        LST_MAINT_DT DATETIME DEFAULT(current_timestamp))""",
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
            }

            for table in tables:
                self.db.query_db(tables[table], commit=True)

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
                self.db.query_db(f'DROP TABLE {table_name}', commit=True)

            for table in tables:
                drop_table(table)

        # Recreate Tables
        drop_tables()
        create_tables()

    class Metafield_Definition:
        def get(definition_id):
            query = f"""
                    SELECT * FROM {creds.shopify_metafield_table}
                    WHERE META_ID = {definition_id}
                    """
            response = Database.db.query_db(query)
            if response is not None:
                return {
                    'META_ID': f'gid://shopify/MetafieldDefinition/{response[0]}',
                    'NAME': response[1],
                    'NAME_SPACE': response[2],
                    'META_KEY': response[3],
                    'TYPE': response[4],
                    'PIN': response[5],
                    'OWNER_TYPE': response[6],
                    'LST_MAINT_DT': response[7],
                }

        def get_all():
            query = f"""
                    SELECT * FROM {creds.shopify_metafield_table}
                    """
            response = Database.db.query_db(query)
            if response is not None:
                result = {}
                for row in response:
                    result[row[1]] = {
                        'META_ID': f'gid://shopify/MetafieldDefinition/{row[0]}',
                        'NAME': row[1],
                        'NAME_SPACE': row[2],
                        'META_KEY': row[3],
                        'TYPE': row[4],
                        'PIN': row[5],
                        'OWNER_TYPE': row[6],
                        'LST_MAINT_DT': row[7],
                    }

                return result

        def insert(values):
            query = f"""
                    INSERT INTO {creds.shopify_metafield_table} (META_ID, NAME, NAME_SPACE, META_KEY, TYPE, PIN, OWNER_TYPE)
                    VALUES {values['META_ID'], values['NAME'], values['NAME_SPACE'], values['META_KEY'], values['TYPE'], values['PIN'], values['OWNER_TYPE']}
                    """
            response = Database.db.query_db(query, commit=True)
            if response['code'] != 200:
                raise Exception(response['message'])

        def update(values):
            query = f"""
                    UPDATE {creds.shopify_metafield_table}
                    SET NAME = {values['NAME']}, NAME_SPACE = {values['NAME_SPACE']}, META_KEY = {values['META_KEY']}, TYPE = {values['TYPE']}, PIN = {values['PIN']}, OWNER_TYPE = {values['OWNER_TYPE']}, LST_MAINT_DT = GETDATE()
                    WHERE META_ID = {values['META_ID']}
                    """
            response = Database.db.query_db(query, commit=True)
            if response['code'] != 200:
                raise Exception(response['message'])

        def delete(definition_id):
            query = f"""
                    DELETE FROM {creds.shopify_metafield_table}
                    WHERE META_ID = {definition_id}
                    """
            response = Database.db.query_db(query, commit=True)
            if response['code'] != 200:
                raise Exception(response['message'])

        def delete_all():
            query = f"""
                    DELETE FROM {creds.shopify_metafield_table}
                    """
            response = Database.db.query_db(query, commit=True)
            if response['code'] != 200:
                raise Exception(response['message'])
