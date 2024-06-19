from setup import creds
from setup import query_engine


class Database:
    db = query_engine.QueryEngine()

    def rebuild_tables(self):
        def create_tables():
            tables = {
                "categories": f"""
                                        CREATE TABLE {creds.bc_category_table} (
                                        CATEG_ID int IDENTITY(1,1) PRIMARY KEY,
                                        BC_CATEG_ID int,
                                        CP_CATEG_ID bigint NOT NULL,
                                        CP_PARENT_ID bigint,
                                        CATEG_NAME nvarchar(255) NOT NULL,
                                        SORT_ORDER int,
                                        DESCRIPTION text,
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                "brands": f"""
                                        CREATE TABLE {creds.bc_brands_table} (
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
                                        IMAGE_LST_MAINT_DT datetime,
                                        IS_CUSTOMIZED BIT,
                                        CUSTOM_URL nvarchar(255),
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                "products": f"""
                                        CREATE TABLE {creds.bc_product_table} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        ITEM_NO varchar(50) NOT NULL,
                                        BINDING_ID varchar(10),
                                        IS_PARENT BIT,
                                        PRODUCT_ID int NOT NULL,
                                        VARIANT_ID int,
                                        VARIANT_NAME nvarchar(255),
                                        OPTION_ID int,
                                        OPTION_VALUE_ID int,
                                        CATEG_ID varchar(100),
                                        CUSTOM_FIELDS varchar(255),
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                "images": f"""
                                        CREATE TABLE {creds.bc_image_table} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        IMAGE_NAME nvarchar(255) NOT NULL,
                                        ITEM_NO varchar(50),
                                        FILE_PATH nvarchar(255) NOT NULL,
                                        IMAGE_URL nvarchar(255),
                                        PRODUCT_ID int,
                                        IMAGE_ID int,
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
                "customers": f"""
                                        CREATE TABLE {creds.bc_customer_table} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        CUST_NO varchar(50) NOT NULL,
                                        BC_CUST_ID int,
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        );
                                        """,
                "orders": f"""
                                        CREATE TABLE {creds.bc_order_table} (
                                        ID int IDENTITY(1, 1) PRIMARY KEY,
                                        ORDER_NO int NOT NULL,
                                        DOC_ID bigint,
                                        STATUS bit DEFAULT(0)
                                        )""",
                "gift": f"""
                                        CREATE TABLE {creds.bc_gift_cert_table} (
                                        ID int IDENTITY(1, 1) PRIMARY KEY,
                                        GFC_NO varchar(30) NOT NULL,
                                        BC_GFC_ID int NOT NULL,
                                        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                        )""",
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
                self.db.query_db(f"DROP TABLE {table_name}", commit=True)

            for table in tables:
                drop_table(table)

        # Recreate Tables
        drop_tables()
        create_tables()
