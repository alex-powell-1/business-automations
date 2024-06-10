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
                "custom_fields": f"""
                                            CREATE TABLE SN_CUSTOM_FIELDS (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO nvarchar(50),
                                            PRODUCT_ID int,
                                            FIELD_1_ID int,
                                            FIELD_1_NAME varchar(60),
                                            FIELD_1_VALUE varchar(255),
                                            FIELD_1_DT datetime DEFAULT(current_timestamp),
                                            FIELD_2_ID int,
                                            FIELD_2_NAME varchar(60),
                                            FIELD_2_VALUE varchar(255),
                                            FIELD_2_DT datetime DEFAULT(current_timestamp),
                                            FIELD_3_ID int,
                                            FIELD_3_NAME varchar(60),
                                            FIELD_3_VALUE varchar(255),
                                            FIELD_3_DT datetime DEFAULT(current_timestamp),
                                            FIELD_4_ID int,
                                            FIELD_4_NAME varchar(60),
                                            FIELD_4_VALUE varchar(255),
                                            FIELD_4_DT datetime DEFAULT(current_timestamp),
                                            FIELD_5_ID int,
                                            FIELD_5_NAME varchar(60),
                                            FIELD_5_VALUE varchar(255),
                                            FIELD_5_DT datetime DEFAULT(current_timestamp),
                                            FIELD_6_ID int,
                                            FIELD_6_NAME varchar(60),
                                            FIELD_6_VALUE varchar(255),
                                            FIELD_6_DT datetime DEFAULT(current_timestamp),
                                            FIELD_7_ID int,
                                            FIELD_7_NAME varchar(60),
                                            FIELD_7_VALUE varchar(255),
                                            FIELD_7_DT datetime DEFAULT(current_timestamp),
                                            FIELD_8_ID int,
                                            FIELD_8_NAME varchar(60),
                                            FIELD_8_VALUE varchar(255),
                                            FIELD_8_DT datetime DEFAULT(current_timestamp),
                                            FIELD_9_ID int,
                                            FIELD_9_NAME varchar(60),
                                            FIELD_9_VALUE varchar(255),
                                            FIELD_9_DT datetime DEFAULT(current_timestamp),
                                            FIELD_10_ID int,
                                            FIELD_10_NAME varchar(60),
                                            FIELD_10_VALUE varchar(255),
                                            FIELD_10_DT datetime DEFAULT(current_timestamp),
                                            FIELD_11_ID int,
                                            FIELD_11_NAME varchar(60),
                                            FIELD_11_VALUE varchar(255),
                                            FIELD_11_DT datetime DEFAULT(current_timestamp),
                                            FIELD_12_ID int,
                                            FIELD_12_NAME varchar(60),
                                            FIELD_12_VALUE varchar(255),
                                            FIELD_12_DT datetime DEFAULT(current_timestamp),
                                            FIELD_13_ID int,
                                            FIELD_13_NAME varchar(60),
                                            FIELD_13_VALUE varchar(255),
                                            FIELD_13_DT datetime DEFAULT(current_timestamp),
                                            FIELD_14_ID int,
                                            FIELD_14_NAME varchar(60),
                                            FIELD_14_VALUE varchar(255),
                                            FIELD_14_DT datetime DEFAULT(current_timestamp),
                                            FIELD_15_ID int,
                                            FIELD_15_NAME varchar(60),
                                            FIELD_15_VALUE varchar(255),
                                            FIELD_15_DT datetime DEFAULT(current_timestamp),
                                            LST_MAINT_DT datetime DEFAULT(current_timestamp),
                                            );""",
                "customers": f"""
                                            CREATE TABLE {creds.bc_customer_table} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            CUST_NO varchar(50) NOT NULL,
                                            BC_CUST_ID int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
            }
            for table in tables:
                self.db.query_db(tables[table], commit=True)

        # Drop Tables
        def drop_tables():
            tables = [
                creds.bc_customer_table,
                creds.bc_custom_fields,
                creds.bc_custom_fields,
                creds.bc_image_table,
                creds.bc_product_table,
                creds.bc_brands_table,
                creds.bc_category_item_table,
                creds.bc_category_table,
            ]

            def drop_table(table_name):
                self.db.query_db(f"DROP TABLE {table_name}", commit=True)

            for table in tables:
                drop_table(table)

        # Recreate Tables
        drop_tables()
        create_tables()
