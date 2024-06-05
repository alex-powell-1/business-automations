import os
import re
from datetime import datetime

import requests
import aiohttp
import asyncio

from PIL import Image, ImageOps
from utilities import handy_tools
from setup import creds
from setup import query_engine
from setup import date_presets
from requests.auth import HTTPDigestAuth

"""
BigCommerce Middleware Integration
Author: Alex Powell
Contributors: Luke Barrier
"""

class Integrator:
    db = query_engine.QueryEngine()

    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.log_file = open("test.txt", "a")

        self.category_tree = None
        self.brands = None
        self.catalog = None

        self.customers = None

    def __str__(self):
        return f"Integration Object\n" \
               f"Last Sync: {self.last_sync}\n" \
               f"{self.catalog}\n" \
               f"{self.category_tree}\n"

    def initialize(self):
        self.category_tree = self.Catalog.CategoryTree(last_sync=self.last_sync)
        self.brands = self.Catalog.Brands(last_sync=self.last_sync)
        self.catalog = self.Catalog(last_sync=self.last_sync)
        self.customers = self.Customers(last_sync=self.last_sync)

    def sync(self):
        self.catalog.sync()
        self.category_tree.build_bc_category_tree()

        self.customers.sync()

    class Database:
        def __init__(self):
            self.db = Integrator.db

        def rebuild_tables(self):
            def drop_table(table_name):
                self.db.query_db(f"DROP TABLE {table_name}", commit=True)

            # Drop Tables
            drop_table(creds.bc_customer_table)
            drop_table(creds.bc_custom_fields)
            drop_table(creds.bc_custom_fields)
            drop_table(creds.bc_image_table)
            drop_table(creds.bc_product_table)
            drop_table(creds.bc_brands_table)
            drop_table(creds.bc_category_item_table)
            drop_table(creds.bc_category_table)

            def create_category_table(table_name):
                query = f"""
                CREATE TABLE {table_name} (
                CATEG_ID int IDENTITY(1,1) PRIMARY KEY,
                BC_CATEG_ID int,
                CP_CATEG_ID bigint NOT NULL,
                CP_PARENT_ID bigint,
                CATEG_NAME nvarchar(255) NOT NULL,
                SORT_ORDER int,
                DESCRIPTION text,
                LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                );
                """
                self.db.query_db(query, commit=True)

            def create_brand_table(table_name):
                query = f"""
                CREATE TABLE {table_name} (
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
                """
                self.db.query_db(query, commit=True)

            def create_category_item_table(table_name):
                query = f"""
                CREATE TABLE {table_name} (
                ID int IDENTITY(1,1) PRIMARY KEY,
                ITEM_NO varchar(50) NOT NULL,
                BC_CATEG_ID int NOT NULL,
                CATEG_ID int FOREIGN KEY REFERENCES {creds.bc_category_table}(CATEG_ID),
                LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                );
                """
                self.db.query_db(query, commit=True)

            def create_product_table(table_name):
                query = f"""
                CREATE TABLE {table_name} (
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
                """
                self.db.query_db(query, commit=True)

            def create_image_table(table_name):
                query = f"""
                    CREATE TABLE {table_name} (
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
                    LST_MOD_DT datetime NOT NULL DEFAULT(current_timestamp),
                    LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                    );
                    """
                self.db.query_db(query, commit=True)

            def create_custom_fields_table(table_name):
                query = f"""
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
                );"""
                self.db.query_db(query, commit=True)

            def create_customer_table(table_name):
                query = f"""
                CREATE TABLE {table_name} (
                ID int IDENTITY(1,1) PRIMARY KEY,
                CUST_NO varchar(50) NOT NULL,
                BC_CUST_ID int,
                LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                );
                """
                self.db.query_db(query, commit=True)

            # Recreate Tables
            create_category_table(creds.bc_category_table)
            create_category_item_table(creds.bc_category_item_table)
            create_brand_table(creds.bc_brands_table)
            create_product_table(creds.bc_product_table)
            create_image_table(creds.bc_image_table)
            create_custom_fields_table(creds.bc_custom_fields)
            create_customer_table(creds.bc_customer_table)

    class Catalog:
        @staticmethod
        def get_all_binding_ids():
            db = query_engine.QueryEngine()
            """Returns a list of unique and validated binding IDs from the IM_ITEM table."""
            response = db.query_db(f"SELECT DISTINCT USR_PROF_ALPHA_16 FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'"
                                   f"AND USR_PROF_ALPHA_16 IS NOT NULL")

            result = []

            def valid(binding_id):
                if re.match(r'B\d{4}', binding_id):
                    return binding_id

            for x in response:
                binding = valid(x[0])
                if binding:
                    result.append(binding)

            return result

        all_binding_ids = get_all_binding_ids()

        def __init__(self, last_sync):
            # self.log_file = log_file
            self.last_sync = last_sync
            self.db = Integrator.db
            # self.update_image_timestamps(self.last_sync)
            # self.process_deletes()
            # lists of products with updated timestamps
            self.products = self.get_products()
            self.binding_ids = set(x['binding_id'] for x in self.products)
            self.product_errors = []
            # Still need to get ALL list from mw and cp

        def __str__(self):
            return (f"Items to Process: {len(self.products)}\n"
                    f"Binding IDs with Updates: {len(self.binding_ids)}\n")

        def get_products(self):
            return [{'sku': '10337', 'binding_id': 'B0006'}]
            # db = query_engine.QueryEngine()
            # query = f"""
            # SELECT ITEM_NO, ISNULL(ITEM.USR_PROF_ALPHA_16, '') as 'Binding ID'
            # FROM IM_ITEM ITEM
            # WHERE ITEM.LST_MAINT_DT > '{self.last_sync: %Y-%m-%d %H:%M:%S}' and ITEM.IS_ECOMM_ITEM = 'Y'
            # """
            # response = db.query_db(query)
            # if response is not None:
            #     result = []
            #     for item in response:
            #         result.append({
            #             'sku': item[0],
            #             'binding_id': item[1]
            #         })
            #     return result

        def sync(self):
            general_errors = []
            print(f"Syncing {len(self.products)} products.")
            while len(self.products) > 0:
                target = self.products.pop()
                print(f"Starting Product: {target['sku']}, Binding: {target['sku']}")
                prod = self.Product(target, last_sync=self.last_sync)
                print(f"Processing Product: {prod.sku}, Binding: {prod.binding_id}, Title: {prod.web_title}")
                if prod.validate_product_inputs():
                    print(f"Product sku:{prod.sku}, binding: {prod.binding_id}: "
                          f"{prod.web_title} PASSED input validation.")
                    try:
                        prod.process()
                    except Exception as e:
                        message = f"General Error with Product {prod.sku}: {e}"
                        print(message)
                        general_errors.append(message)
                    else:
                        if prod.errors:
                            self.product_errors.append(prod)
                else:
                    print(f"Product sku:{prod.sku}, binding: {prod.binding_id}: "
                          f"{prod.web_title} FAILED input validation.")
                    self.product_errors.append(prod)

                # Remove all variants from the queue
                products_to_remove = [y.sku for y in prod.variants]
                for x in products_to_remove:
                    for y in self.products:
                        if y['sku'] == x:
                            print(f"Removing {y}")
                            self.products.remove(y)  # remove all variants from the list
                print(f"Products Remaining: {len(self.products)}\n\n")

            print("-----------------------\n")

            if len(general_errors) > 0:
                print("-----------------------\n")
                print("General Errors:")
                for error in general_errors:
                    print(error)
                print("-----------------------\n")

            print(f"Sync Complete. {len(self.product_errors)} Product Errors.")
            # Print Errors
            if len(self.product_errors) > 0:
                for product in self.product_errors:
                    print(f"Error Processing Product: {product.sku}")
                    if len(product.errors) > 0:
                        print("Error Messages:")
                        for message in product.errors:
                            print(message)
                    print("\n\n")

        @staticmethod
        def process_deletes():
            db = query_engine.QueryEngine()
            all_cp_products = [x[0] for x in db.query_db(f"SELECT ITEM_NO FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'") if x]
            all_mw_products = [x[0] for x in db.query_db(f"SELECT ITEM_NO FROM {creds.bc_product_table}") if x]
            delete_count = 0
            delete_targets = Integrator.Catalog.get_deletion_target(middleware_list=all_mw_products,
                                                                    counterpoint_list=all_cp_products)
            print(f"Delete Targets: {delete_targets}")

            def delete_from_bigcommerce(target):
                print(f"Deleting Product from bigcommerce: {target}. TEST MODE")

            def delete_from_middleware(target):
                print(f"Deleting Product from middleware: {target}. TEST MODE")

            for x in delete_targets:
                print(f"Deleting Product {x}.")
                delete_from_bigcommerce(x)
                delete_from_middleware(x)
                delete_count += 1

            print(f"Deleted {delete_count} brands.")

        @staticmethod
        def update_image_timestamps(last_sync):
            """Takes in a list of SKUs and updates the last maintenance date in IM_ITEM and MW_IMAGES table for each
            product in the list. Updating IM_ITEM will trigger a sync of that item and it will then look for photos
            during the sync process."""
            print("Image Update: Updating product timestamps.")

            def get_updated_photos(date):
                """Get a tuple of two sets:
                    1. all SKUs that have had their photo modified since the input date.
                    2. all file names that have been modified since the input date."""
                sku_result = set()
                file_result = set()
                # Iterate over all files in the directory
                for filename in os.listdir(creds.photo_path):
                    if filename not in ["Thumbs.db", "desktop.ini", ".DS_Store"]:
                        # Get the full path of the file
                        file_path = os.path.join(creds.photo_path, filename)

                        # Get the last modified date of the file
                        modified_date = datetime.fromtimestamp(os.path.getmtime(file_path))

                        # If the file has been modified since the input date, print its name
                        if modified_date > date:
                            sku = filename.split(".")[0].split("^")[0]
                            sku_result.add(sku)
                            file_result.add(filename)

                return sku_result, file_result

            sku_list, file_list = get_updated_photos(last_sync)

            if len(sku_list) > 0:
                db = query_engine.QueryEngine()
                query = (f"UPDATE IM_ITEM "
                         f"SET LST_MAINT_DT = GETDATE() "
                         f"WHERE ITEM_NO in {sku_list} "

                         f"UPDATE {creds.bc_image_table} "
                         f"SET LST_MAINT_DT = GETDATE() "
                         f"WHERE IMAGE_NAME in {file_list} ")
                try:
                    db.query_db(query, commit=True)
                except Exception as e:
                    print(f"Error updating product timestamps: {e}")
                else:
                    print("Product timestamps updated: ", sku_list)
            else:
                print("No new images")

        @staticmethod
        def get_binding_id_from_sku(sku):
            db = query_engine.QueryEngine()
            query = f"""
            SELECT USR_PROF_ALPHA_16
            FROM IM_ITEM
            WHERE ITEM_NO = '{sku}'
            """
            response = db.query_db(query)
            if response is not None:
                return response[0][0]

        @staticmethod
        def get_deletion_target(counterpoint_list, middleware_list):
            return [element for element in middleware_list if element not in counterpoint_list]

        class CategoryTree:
            def __init__(self, last_sync):
                self.db = query_engine.QueryEngine()
                self.last_sync = last_sync
                self.categories = set()
                self.heads = []
                self.get_cp_updates()
                self.create_tree_in_memory()

            def __str__(self):
                def print_category_tree(category, level=0):
                    # Print the category id and name, indented by the category's level in the tree
                    res = (f"{'    ' * level}Category Name: {category.category_name}\n"
                           f"{'    ' * level}---------------------------------------\n"
                           f"{'    ' * level}Counterpoint Category ID: {category.cp_categ_id}\n"
                           f"{'    ' * level}Counterpoint Parent ID: {category.cp_parent_id}\n"
                           f"{'    ' * level}BigCommerce Category ID: {category.bc_categ_id}\n"
                           f"{'    ' * level}BigCommerce Parent ID: {category.bc_parent_id}\n"
                           f"{'    ' * level}Sort Order: {category.sort_order}\n"
                           f"{'    ' * level}Last Maintenance Date: {category.lst_maint_dt}\n\n")
                    # Recursively call this function for each child category
                    for child in category.children:
                        res += print_category_tree(child, level + 1)
                    return res

                # Use the helper function to print the entire tree
                result = ""
                for root in self.heads:
                    result += print_category_tree(root)

                return result

            def get_cp_updates(self):
                query = f"""
                SELECT cp.CATEG_ID, ISNULL(cp.PARENT_ID, 0), cp.DESCR, cp.DISP_SEQ_NO, cp.HTML_DESCR, 
                cp.LST_MAINT_DT, sn.CP_CATEG_ID
                FROM EC_CATEG cp
                FULL OUTER JOIN SN_CATEG sn on cp.CATEG_ID=sn.CP_CATEG_ID
                """
                response = self.db.query_db(query)
                if response:
                    for x in response:
                        cp_categ_id = x[0]
                        if cp_categ_id == '0':
                            continue
                        if cp_categ_id is None:
                            self.delete_category(x[6])
                            continue
                        lst_maint_dt = x[5]
                        sn_cp_categ_id = x[6]
                        if sn_cp_categ_id is None:
                            # Insert new records
                            cp_parent_id = x[1]
                            category_name = x[2]
                            sort_order = x[3]
                            description = x[4]
                            query = f"""
                            INSERT INTO SN_CATEG(CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, 
                            SORT_ORDER, DESCRIPTION, LST_MAINT_DT)
                            VALUES({cp_categ_id}, {cp_parent_id}, '{category_name}',
                            {sort_order}, '{description}', '{lst_maint_dt:%Y-%m-%d %H:%M:%S}')
                            """
                            self.db.query_db(query, commit=True)
                        else:
                            if lst_maint_dt > self.last_sync:
                                # Update existing records
                                cp_parent_id = x[1]
                                category_name = x[2]
                                sort_order = x[3]
                                description = x[4]
                                lst_maint_dt = x[5]
                                query = f"""
                                UPDATE SN_CATEG
                                SET CP_PARENT_ID = {cp_parent_id}, CATEG_NAME = '{category_name}',
                                SORT_ORDER = {sort_order}, DESCRIPTION = '{description}', 
                                LST_MAINT_DT = '{lst_maint_dt:%Y-%m-%d %H:%M:%S}'
                                WHERE CP_CATEG_ID = {sn_cp_categ_id}
                                """
                                self.db.query_db(query, commit=True)

            def create_tree_in_memory(self):
                def get_categories():
                    query = f"""
                    SELECT CATEG_ID, ISNULL(PARENT_ID, 0), DESCR, DISP_SEQ_NO, HTML_DESCR, LST_MAINT_DT
                    FROM EC_CATEG
                    WHERE CATEG_ID != '0'
                    """
                    response = self.db.query_db(query)
                    if response is not None:
                        for ec_cat in response:
                            category = self.Category(cp_categ_id=ec_cat[0],
                                                     cp_parent_id=ec_cat[1],
                                                     category_name=ec_cat[2],
                                                     sort_order=ec_cat[3],
                                                     description=ec_cat[4],
                                                     lst_maint_dt=ec_cat[5])
                            self.categories.add(category)

                get_categories()

                for x in self.categories:
                    for y in self.categories:
                        if y.cp_parent_id == x.cp_categ_id:
                            x.add_child(y)

                self.heads = [x for x in self.categories if x.cp_parent_id == '0']

            def sync(self):
                def build_tree(category):
                    # Get BC Category ID and Parent ID
                    if category.lst_maint_dt > self.last_sync:
                        print(f"Updating: {category.category_name}")
                        if category.bc_categ_id is None:
                            category.get_bc_id()
                        if category.bc_parent_id is None:
                            category.get_bc_parent_id()
                        category.update_category_in_middleware()
                        category.bc_update_category()

                    # Recursively call this function for each child category
                    for child in category.children:
                        build_tree(child)

                for x in self.heads:
                    build_tree(x)

            def delete_category(self, cp_categ_id):

                query = f"""
                SELECT BC_CATEG_ID
                FROM SN_CATEG
                WHERE CP_CATEG_ID = {cp_categ_id}
                """
                response = self.db.query_db(query)
                if response:
                    bc_category_id = response[0][0]
                    print(bc_category_id)
                    if bc_category_id is not None:
                        # Delete Category from BigCommerce
                        print(f"BigCommerce: DELETE {bc_category_id}")
                        url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}"
                               f"/v3/catalog/trees/categories?category_id:in={bc_category_id}")
                        response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                        if 207 >= response.status_code >= 200:
                            print(response.status_code)  # figure what code they are actually returning
                            print(f"Category {bc_category_id} deleted from BigCommerce.")
                            # Delete Category from Middleware
                            print(f"Middleware: DELETE {cp_categ_id}")
                            query = f"""
                                            DELETE FROM SN_CATEG
                                            WHERE CP_CATEG_ID = {cp_categ_id}
                                            """
                            try:
                                self.db.query_db(query, commit=True)
                            except Exception as e:
                                print(f"Error deleting category from middleware: {e}")
                            else:
                                print(f"Category {cp_categ_id} deleted from Middleware.")
                        else:
                            print(f"Error deleting category {bc_category_id} from BigCommerce.")
                            print(response.json())

            class Category:
                def __init__(self, cp_categ_id, cp_parent_id, category_name,
                             bc_categ_id=None, bc_parent_id=None, sort_order=0, description="",
                             lst_maint_dt=datetime(1970, 1, 1)):
                    # Category Properties
                    self.cp_categ_id = cp_categ_id
                    self.cp_parent_id = cp_parent_id
                    self.category_name = category_name
                    self.bc_categ_id = bc_categ_id
                    self.bc_parent_id = bc_parent_id
                    self.sort_order = sort_order
                    self.description = description
                    self.lst_maint_dt = lst_maint_dt
                    self.children = []

                def __str__(self):
                    return f"Category Name: {self.category_name}\n" \
                           f"---------------------------------------\n" \
                           f"Counterpoint Category ID: {self.cp_categ_id}\n" \
                           f"Counterpoint Parent ID: {self.cp_parent_id}\n" \
                           f"BigCommerce Category ID: {self.bc_categ_id}\n" \
                           f"BigCommerce Parent ID: {self.bc_parent_id}\n" \
                           f"Sort Order: {self.sort_order}\n" \
                           f"Last Maintenance Date: {self.lst_maint_dt}\n\n"

                def add_child(self, child):
                    self.children.append(child)

                def get_bc_id(self):
                    query = f"""
                    SELECT BC_CATEG_ID
                    FROM {creds.bc_category_table}
                    WHERE CP_CATEG_ID = {self.cp_categ_id}
                    """
                    response = query_engine.QueryEngine().query_db(query)
                    if response is not None:
                        bc_category_id = response[0][0]
                        if bc_category_id is not None:
                            self.bc_categ_id = response[0][0]
                        else:
                            self.get_bc_parent_id()
                            self.bc_categ_id = self.bc_create_category()

                def get_bc_parent_id(self):
                    query = f"""
                    SELECT BC_CATEG_ID
                    FROM {creds.bc_category_table}
                    WHERE CP_CATEG_ID = (SELECT CP_PARENT_ID 
                                        FROM {creds.bc_category_table} 
                                        WHERE CP_CATEG_ID = {self.cp_categ_id})
                    """
                    response = query_engine.QueryEngine().query_db(query)
                    if response:
                        self.bc_parent_id = response[0][0]
                    else:
                        self.bc_parent_id = 0

                def bc_create_category(self):
                    url = f" https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/trees/categories"
                    payload = [{
                        "name": self.category_name,
                        "url": {
                            "path": f"/{self.category_name}/",
                            "is_customized": False
                        },
                        "parent_id": self.bc_parent_id,
                        "tree_id": 1,
                        "description": self.description,
                        "sort_order": self.sort_order,
                        "page_title": self.category_name,
                        # "meta_keywords": [
                        #   "shower",
                        #   "tub"
                        # ],
                        # "meta_description": "string",
                        # "layout_file": "category.html",
                        # "image_url": "https://cdn8.bigcommerce.com/s-123456/product_images/d/fakeimage.png",
                        "is_visible": True,
                        "search_keywords": "string",
                        "default_product_sort": "use_store_settings"
                    }]

                    response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                    if response.status_code == 201 or response.status_code == 207:
                        print(f"BigCommerce: POST: {self.category_name}: SUCCESS Code: {response.status_code}")
                        category_id = response.json()['data'][0]['category_id']
                        return category_id
                    else:
                        print(f"BigCommerce: POST: {self.category_name}: Failure Code: {response.status_code}")
                        print(response.json())

                def bc_update_category(self):
                    url = f" https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/trees/categories"
                    payload = [{
                        "category_id": self.bc_categ_id,
                        "name": self.category_name,
                        "parent_id": self.bc_parent_id,
                        "tree_id": 1,
                        "page_title": self.category_name,
                        "is_visible": True,
                    }]

                    response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                    if response.status_code == 200:
                        print(
                            f"BigCommerce: UPDATE: {self.category_name} Category: SUCCESS Code: {response.status_code}\n")
                    else:
                        print(f"BigCommerce: UPDATE: {self.category_name} "
                              f"Category: FAILED Status: {response.status_code}"
                              f"Payload: {payload}\n"
                              f"Response: {response.text}\n")

                def write_category_to_middleware(self):
                    query = f"""
                    INSERT INTO SN_CATEG (BC_CATEG_ID, CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, SORT_ORDER, DESCRIPTION)
                    VALUES ({self.bc_categ_id}, {self.cp_categ_id}, {self.cp_parent_id}, 
                    '{self.category_name}', {self.sort_order}, '{self.description}')
                    """
                    try:
                        query_engine.QueryEngine().query_db(query, commit=True)
                    except Exception as e:
                        print(f"Middleware: INSERT {self.category_name}: FAILED")
                        print(e)
                    else:
                        print(f"Middleware: INSERT {self.category_name}: SUCCESS")

                def update_category_in_middleware(self):
                    query = f"""
                    UPDATE SN_CATEG
                    SET BC_CATEG_ID = {self.bc_categ_id}, CP_PARENT_ID = {self.cp_parent_id}, 
                    CATEG_NAME = '{self.category_name}', 
                    SORT_ORDER = {self.sort_order}, DESCRIPTION = '{self.description}'
                    WHERE CP_CATEG_ID = {self.cp_categ_id}
                    """
                    try:
                        query_engine.QueryEngine().query_db(query, commit=True)
                    except Exception as e:
                        print(f"Middleware: UPDATE {self.category_name} Category: FAILED")
                        print(e)
                    else:
                        print(f"Middleware: UPDATE {self.category_name} Category: SUCCESS")

                def delete_category(self):
                    # Delete Category from BigCommerce
                    print(f"BigCommerce: DELETE {self.category_name}")
                    url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                           f"catalog/trees/categories/{self.bc_categ_id}")
                    response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                    if response.status_code == 204:
                        print(f"Category {self.category_name} deleted from BigCommerce.")
                    else:
                        print(f"Error deleting category {self.category_name} from BigCommerce.")
                        print(response.json())

                    # Delete Category from Middleware
                    print(f"Middleware: DELETE {self.category_name}")
                    query = f"""
                    DELETE FROM SN_CATEG
                    WHERE CP_CATEG_ID = {self.cp_categ_id}
                    """
                    try:
                        query_engine.QueryEngine().query_db(query, commit=True)
                    except Exception as e:
                        print(f"Error deleting category from middleware: {e}")
                    else:
                        print(f"Category {self.category_name} deleted from Middleware.")

        class Brands:
            def __init__(self, last_sync):
                self.db = query_engine.QueryEngine()
                self.last_sync = last_sync
                self.cp_brands = set()
                self.mw_brands = set()
                # self.brands will be a set of Brand objects only created if the last_maint_dt is > than last sync
                self.brands: set = set()
                # trip lst_maint_dt for all brands whose photos have been updated
                self.update_brand_timestamps(last_run=self.last_sync)
                # get all brands from CP and MW
                self.get_brands()
                # process deletes
                self.process_deletes()
                # create Brand objects for each brand that has been updated
                self.construct_brands()

            def __str__(self):
                result = ""
                for brand in self.brands:
                    result += (f"{brand.name}\n"
                               f"---------------------------------------\n"
                               f"Last Modified: {brand.last_maint_dt}\n\n")
                return result

            @staticmethod
            def update_brand_timestamps(last_run):
                """Takes in a list of SKUs and updates the last maintenance date in input table for each product in the
                list"""

                def get_updated_brands(date):
                    """Get a tuple of two sets:
                        1. all SKUs that have had their photo modified since the input date.
                        2. all file names that have been modified since the input date."""
                    file_result = set()
                    # Iterate over all files in the directory
                    for filename in os.listdir(creds.brand_photo_path):
                        if filename not in ["Thumbs.db", "desktop.ini", ".DS_Store"]:
                            # Get the full path of the file
                            file_path = os.path.join(creds.brand_photo_path, filename)

                            # Get the last modified date of the file
                            try:
                                modified_date = datetime.fromtimestamp(os.path.getmtime(file_path))
                            except FileNotFoundError:
                                modified_date = datetime(1970, 1, 1)

                            # If the file has been modified since the input date, print its name
                            if modified_date > date:
                                file_result.add(filename)

                    return file_result

                file_list = get_updated_brands(last_run)
                code_tuple = tuple([x.split(".")[0] for x in file_list])
                # If no files have been modified, return
                if len(code_tuple) == 0:
                    return
                else:
                    mw_where_filter = f"WHERE CP_BRAND_ID in {code_tuple}"
                    cp_where_filter = f"WHERE PROF_COD in {code_tuple}"
                    if len(code_tuple) == 1:
                        mw_where_filter = f"WHERE CP_BRAND_ID = '{code_tuple[0]}'"
                        cp_where_filter = f"WHERE PROF_COD = '{code_tuple[0]}'"

                    db = query_engine.QueryEngine()
                    query = (f"UPDATE {creds.bc_brands_table} "
                             f"SET IMAGE_LST_MAINT_DT = GETDATE(), LST_MAINT_DT = GETDATE() "
                             f"{mw_where_filter} "
                             f"UPDATE IM_ITEM_PROF_COD "
                             f"SET LST_MAINT_DT = GETDATE() "
                             f"{cp_where_filter} ")
                    print(query)
                    try:
                        response = db.query_db(query, commit=True)
                    except Exception as e:
                        print(f"Error updating product timestamps: {e}")
                    else:
                        if response["code"] == 200:
                            print("Brand: LST_MAINT_DT UPDATE sent.")
                        else:
                            print(response)

            def construct_brands(self):
                for cp_brand in self.cp_brands:
                    # Filter out brands that are not new or updated
                    if cp_brand[2] > self.last_sync:
                        brand = self.Brand(cp_brand[0], cp_brand[1], cp_brand[2], self.last_sync)
                        self.brands.add(brand)
                    else:
                        print(f"Brand {cp_brand[1]} has not been updated since the last sync.")

            def get_brands(self):
                def get_cp_brands():
                    query = f"""
                    SELECT PROF_COD, DESCR, LST_MAINT_DT
                    FROM IM_ITEM_PROF_COD
                    """
                    response = self.db.query_db(query)
                    if response:
                        for x in response:
                            self.cp_brands.add((x[0], x[1], x[2]))

                def get_mw_brands():
                    query = f"""
                    SELECT CP_BRAND_ID, NAME, BC_BRAND_ID
                    FROM {creds.bc_brands_table}
                    """
                    response = self.db.query_db(query)
                    if response:
                        for x in response:
                            self.mw_brands.add((x[0], x[1], x[2]))

                get_cp_brands()
                get_mw_brands()

            def process_deletes(self):
                delete_count = 0
                mw_brand_ids = [x[0] for x in self.mw_brands]
                cp_brand_ids = [x[0] for x in self.cp_brands]
                delete_targets = Integrator.Catalog.get_deletion_target(middleware_list=mw_brand_ids,
                                                                        counterpoint_list=cp_brand_ids)

                def bc_delete(target):
                    url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                           f"catalog/brands/{target}")
                    response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                    if response.status_code == 204:
                        print(f"BigCommerce: Brand {x} DELETE: SUCCESS. Code: {response.status_code}")
                    elif response.status_code == 404:
                        print(f"BigCommerce: Brand {x} DELETE: Brand Not Found.")
                    else:
                        print(f"BigCommerce: Brand {x} DELETE: FAILED! Status Code: {response.status_code}")
                        print(response.json())

                def delete_from_middleware(target):
                    query = f"""
                    DELETE FROM {creds.bc_brands_table}
                    WHERE CP_BRAND_ID = '{target}'
                    """
                    try:
                        self.db.query_db(query, commit=True)
                    except Exception as e:
                        print(f"Error deleting brand {target} from middleware: {e}")
                    else:
                        print(f"Brand {target} deleted from middleware.")

                def get_bc_brand_id(cp_brand_id):
                    query = f"""
                    SELECT BC_BRAND_ID
                    FROM {creds.bc_brands_table}
                    WHERE CP_BRAND_ID = '{cp_brand_id}'
                    """
                    response = self.db.query_db(query)
                    if response:
                        return response[0][0]
                    else:
                        return None

                for x in delete_targets:
                    bc_brand_id = get_bc_brand_id(x)
                    if bc_brand_id is not None:
                        print(f"Deleting Brand {x}.")
                        bc_delete(bc_brand_id)
                        delete_from_middleware(x)
                        delete_count += 1
                    else:
                        print(f"Brand {x} not found in middleware.")

                print(f"Deleted {delete_count} brands.")

            class Brand:
                def __init__(self, cp_brand_id, description, last_maint_dt, last_sync):
                    self.db = query_engine.QueryEngine()
                    self.cp_brand_id = cp_brand_id
                    print(f"Brand ID: {self.cp_brand_id}")
                    self.bc_brand_id = None
                    self.name = description
                    self.page_title = description
                    self.meta_keywords = ""
                    self.meta_description = ""
                    self.search_keywords = ""
                    self.image_name = ""
                    self.image_url = ""
                    self.image_filepath = f"{creds.brand_photo_path}/{self.cp_brand_id}.jpg"
                    self.image_last_modified = None
                    self.is_custom_url = True
                    self.custom_url = "-".join(str(re.sub('[^A-Za-z0-9 ]+', '', self.name)).split(" "))
                    self.last_maint_dt = last_maint_dt
                    if self.last_maint_dt > last_sync:
                        # setter
                        self.get_brand_details(last_sync)

                def get_brand_details(self, last_sync):
                    query = f"""SELECT *
                    FROM {creds.bc_brands_table}
                    INNER JOIN IM_ITEM_PROF_COD 
                    ON {creds.bc_brands_table}.CP_BRAND_ID = IM_ITEM_PROF_COD.PROF_COD
                    WHERE CP_BRAND_ID = '{self.cp_brand_id}'"""

                    response = self.db.query_db(query)
                    if response is not None:
                        # Brand Found, Update Brand in middleware first with pull from counterpoint
                        # if upon fresh pull, data doesn't exist, then delete, otherwise update and then
                        # run the following code

                        for x in response:
                            self.bc_brand_id = x[2]
                            self.name = x[3]
                            self.page_title = x[4]
                            self.meta_keywords = x[5]
                            self.meta_description = x[6]
                            self.search_keywords = x[7]
                            self.image_name = x[8]
                            self.image_url = x[9]
                            self.image_filepath = x[10]
                            self.image_last_modified = x[11] if x[11] else self.get_image_last_modified()
                            self.is_custom_url = True if x[12] == 1 else False
                            self.custom_url = x[13]
                            self.last_maint_dt = x[14]
                            name = x[16]
                            updated = False

                            if self.name != name:
                                print("Name Mismatch")
                                updated = True
                                self.name = name

                            # Image Exists in DB
                            if self.image_name is not None:
                                # Image Updated
                                if self.image_last_modified > last_sync:
                                    print("Image Updated")
                                    updated = True
                                    self.upload_brand_image()

                            # Image Does Not Exist in DB
                            else:
                                self.image_name = self.get_brand_image()
                                if self.image_name != "":
                                    updated = True
                                    self.image_url = self.upload_brand_image()

                            if updated:
                                print("Updating Brand")
                                self.update()

                            else:
                                print(f"No updates found for brand {self.name}.")

                    # Brand Not Found, Create New Brand
                    else:
                        print("No matching brand found.")
                        self.create()

                def create(self):
                    self.image_url = self.upload_brand_image()

                    def create_bc_brand():
                        def construct_payload():
                            return {
                                "name": self.name,
                                "page_title": self.page_title,
                                "meta_keywords": self.meta_keywords.split(",") if self.meta_keywords else [],
                                "meta_description": self.meta_description,
                                "search_keywords": self.search_keywords if self.search_keywords else "",
                                "image_url": self.image_url,
                                "custom_url": {
                                    "url": f"/{self.custom_url}/",
                                    "is_customized": self.is_custom_url
                                }
                            }

                        url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/brands"
                        payload = construct_payload()
                        response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                        if response.status_code in [200, 207]:
                            print(f"BigCommerce: Brand {self.name} POST: SUCCESS. Code: {response.status_code}")
                            return response.json()["data"]["id"]
                        else:
                            print(f"BigCommerce: Brand {self.name} POST: FAILED! Status Code: {response.status_code}")
                            print(response.json())  # figure out what they are actually returning

                    self.bc_brand_id = create_bc_brand()

                    def insert_to_middleware():
                        print("insert")
                        query = f"""
                        INSERT INTO {creds.bc_brands_table} (CP_BRAND_ID, BC_BRAND_ID, NAME, PAGE_TITLE, META_KEYWORDS, 
                        META_DESCR, SEARCH_KEYWORDS, IMAGE_NAME, IMAGE_URL, IMAGE_FILEPATH, IMAGE_LST_MAINT_DT, IS_CUSTOMIZED, 
                        CUSTOM_URL)
                        VALUES ('{self.cp_brand_id}', 
                        {self.bc_brand_id}, 
                        {f"'{self.name.replace("'", "''")}'"}, 
                        {f"'{self.page_title.replace("'", "''")}'"}, 
                        {f"'{self.meta_keywords.replace("'", "''")}'" if self.meta_keywords else "NULL"}, 
                        {f"'{self.meta_description.replace("'", "''")}'" if self.meta_description else "NULL"}, 
                        {f"'{self.search_keywords.replace("'", "''")}'" if self.search_keywords else "NULL"}, 
                        {f"'{self.image_name}'" if self.image_name != "" else "NULL"},
                        {f"'{self.image_url}'" if self.image_url != "" else "NULL"}, 
                        {f"'{self.image_filepath}'" if self.image_filepath else "NULL"}, 
                        {f"'{self.image_last_modified:%Y-%m-%d %H:%M:%S}'" if self.image_last_modified else "NULL"},
                        {1 if self.is_custom_url else 0},
                        {f"'{self.custom_url}'" if self.custom_url else "NULL"})
                        """
                        try:
                            response = self.db.query_db(query, commit=True)
                        except Exception as e:
                            print(f"MIDDLEWARE: Brand {self.name} INSERT: FAILED.\n")
                            print(e)
                        else:
                            if response["code"] == 200:
                                print("Brand: MIDDLEWARE UPDATE sent.")
                            else:
                                print(response)

                    insert_to_middleware()

                def update(self):
                    def update_bc_brand():
                        url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3"
                               f"/catalog/brands/{self.bc_brand_id}")
                        payload = {
                            "name": self.name,
                            "page_title": self.page_title,
                            "meta_keywords": self.meta_keywords.split(",") if self.meta_keywords else [],
                            "meta_description": self.meta_description if self.meta_description else "",
                            "search_keywords": self.search_keywords if self.search_keywords else "",
                            "image_url": self.image_url,
                            "custom_url": {
                                "url": f"/{self.custom_url}/",
                                "is_customized": self.is_custom_url
                            }
                        }
                        response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                        if response.status_code in [200, 207]:
                            print(f"BigCommerce: Brand {self.name} PUT: SUCCESS. Code: {response.status_code}")
                        else:
                            print(f"BigCommerce: Brand {self.name} PUT: FAILED! Status Code: {response.status_code}")
                            print(response.json())

                    update_bc_brand()

                    def update_middleware():
                        query = f"""
                        UPDATE {creds.bc_brands_table}
                        SET NAME = {f"'{self.name.replace("'", "''")}'"}, 
                        PAGE_TITLE = {f"'{self.page_title.replace("'", "''")}'"}, META_KEYWORDS = 
                        {f"'{self.meta_keywords.replace("'", "''")}'" if self.meta_keywords else "NULL"}, 
                        META_DESCR = {f"'{self.meta_description.replace("'", "''")}'" if self.meta_description else "NULL"}, 
                        SEARCH_KEYWORDS = {f"'{self.search_keywords.replace("'", "''")}'" if self.search_keywords else "NULL"},
                        IMAGE_NAME = {f"'{self.image_name}'" if self.image_name != "" else "NULL"}, 
                        IMAGE_URL = {f"'{self.image_url}'" if self.image_url != "" else "NULL"}, 
                        IMAGE_FILEPATH = {f"'{self.image_filepath}'" if self.image_filepath else "NULL"},
                        IMAGE_LST_MAINT_DT = {f"'{self.image_last_modified:%Y-%m-%d %H:%M:%S}'" if self.image_last_modified else "NULL"},
                        IS_CUSTOMIZED = {1 if self.is_custom_url else 0}, 
                        CUSTOM_URL = {f"'{self.custom_url}'" if self.custom_url else "NULL"}, LST_MAINT_DT = GETDATE()
                        WHERE CP_BRAND_ID = '{self.cp_brand_id}'
                        """
                        print(query)
                        try:
                            response = self.db.query_db(query, commit=True)
                        except Exception as e:
                            print(f"MIDDLEWARE: Brand {self.name} UPDATE: FAILED.\n")
                            print(e)
                        else:
                            if response["code"] == 200:
                                print("Brand: MIDDLEWARE UPDATE sent.")
                            else:
                                print(response)

                    update_middleware()

                def get_brand_image(self):
                    """Get image file name from directory"""
                    for filename in os.listdir(creds.brand_photo_path):
                        if filename.split(".")[0] == self.cp_brand_id:
                            return filename
                    return ""

                def get_image_last_modified(self):
                    """Get last modified date of image file"""
                    try:
                        return datetime.fromtimestamp(os.path.getmtime(self.image_filepath))
                    except FileNotFoundError:
                        return datetime(1970, 1, 1)

                def upload_brand_image(self) -> str:
                    """Upload file to import folder on webDAV server and turn public url"""
                    try:
                        data = open(self.image_filepath, 'rb')
                    except FileNotFoundError:
                        return ""

                    self.image_name = f"{self.cp_brand_id}.jpg"

                    url = f"{creds.web_dav_product_photos}/{self.image_name}"

                    try:
                        requests.put(url, data=data, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw))
                    except Exception as e:
                        print(f"Error uploading image: {e}")
                    else:
                        # return public url of image
                        return f"{creds.public_web_dav_photos}/{self.image_name}"

        class Product:
            def __init__(self, product_data, last_sync):
                # if product_data['binding_id'] == "":
                #     print("INITIALIZING PRODUCT CLASS FOR ITEM: ", product_data['sku'])
                # else:
                #     print("INITIALIZING PRODUCT CLASS FOR ITEM: ", product_data['binding_id'])
                self.db = query_engine.QueryEngine()

                self.sku = product_data['sku']
                self.binding_id = product_data['binding_id']

                self.last_sync = last_sync

                # Determine if Bound
                self.is_bound = True if self.binding_id != "" else False

                # For Bound Items
                self.total_variants: int = 0
                # self.variants will be list of variant products
                self.variants: list = []
                # self.parent will be a list of parent products. If length of list > 1, product validation will fail
                self.parent: list = []

                # A list of image objects
                self.images: list = []

                # Product Information
                self.product_id = ""
                self.web_title: str = ""
                self.default_price = 0.0
                self.cost = 0.0
                self.sale_price = 0.0
                self.weight = 0.1
                self.width = 0.1
                self.height = 0.1
                self.depth = 0.1
                self.buffered_quantity = 0
                self.is_price_hidden = False
                self.brand = ""
                self.html_description = ""
                self.search_keywords = ""
                self.meta_title = ""
                self.meta_description = ""
                self.visible: bool = False
                self.featured: bool = False
                self.sort_order = 0
                self.gift_wrap: bool = False
                self.in_store_only: bool = False
                self.is_preorder_only = False
                self.is_free_shipping = False
                self.preorder_release_date = f"{datetime(1970, 1, 1):%Y-%m-%d}"
                self.preorder_message = ""
                self.alt_text_1 = ""
                self.alt_text_2 = ""
                self.alt_text_3 = ""
                self.alt_text_4 = ""
                self.custom_url = ""
                self.is_custom_url = True

                # Custom Fields
                self.custom_botanical_name = ""
                self.custom_climate_zone = ""
                self.custom_plant_type = ""
                self.custom_type = ""
                self.custom_height = ""
                self.custom_width = ""
                self.custom_sun_exposure = ""
                self.custom_bloom_time = ""
                self.custom_bloom_color = ""
                self.custom_attracts_pollinators = ""
                self.custom_growth_rate = ""
                self.custom_deer_resistant = ""
                self.custom_soil_type = ""
                self.custom_color = ""
                self.custom_size = ""
                self.custom_field_response = []  # Will be list of dictionaries from BC API
                self.custom_field_ids = ""

                # E-Commerce Categories
                self.ecommerce_categories = []

                # Property Getter
                self.get_product_details()

                # Validate Product
                self.validation_retries = 10

                self.errors = []

            def __str__(self):
                result = ""
                line = "-" * 25 + "\n\n"
                result += f"Printing Bound Product Details for: {self.binding_id}\n"
                for k, v in self.__dict__.items():
                    result += f"{k}: {v}\n"
                result += line
                result += "Printing Child Product Details\n"
                if len(self.variants) > 1:
                    variant_index = 1
                    for variant in self.variants:
                        result += f"Variant: {variant_index}\n"
                        result += line
                        for k, v in variant.__dict__.items():
                            result += f"{k}: {v}\n"
                        for image in variant.images:
                            result += f"Image: {image.image_name}\n"
                            result += f"Thumbnail: {image.is_thumbnail}\n"
                            result += f"Variant Image: {image.is_variant_image}\n"
                            result += f"Sort Order: {image.sort_order}\n"
                        result += line
                        variant_index += 1
                return result

            def get_product_details(self):
                def get_bound_product_details():
                    # clear children list
                    self.variants = []
                    query = f"""
                    SELECT ITEM_NO
                    FROM IM_ITEM
                    WHERE USR_PROF_ALPHA_16 = '{self.binding_id}' and IS_ECOMM_ITEM = 'Y'
                    ORDER BY PRC_1
                    """
                    # Get children and append to child list in order of price
                    response = self.db.query_db(query)
                    if response is not None:
                        # Create Product objects for each child and add object to bound parent list
                        for item in response:
                            self.variants.append(self.Variant(item[0], last_run_date=self.last_sync))

                    # Set parent
                    self.parent = [x for x in self.variants if x.is_parent]

                    # Set total children
                    self.total_variants = len(self.variants)

                    # Inherit Product Information from Parent Item
                    for bound in self.variants:
                        if bound.is_parent:
                            self.product_id = bound.product_id
                            self.web_title = bound.web_title
                            self.default_price = bound.price_1
                            self.cost = bound.cost
                            self.sale_price = bound.price_2
                            self.in_store_only = bound.in_store_only
                            self.brand = bound.brand
                            self.sort_order = bound.sort_order
                            self.html_description = bound.html_description
                            self.search_keywords = bound.search_keywords
                            self.meta_title = bound.meta_title
                            self.meta_description = bound.meta_description
                            self.visible = bound.visible
                            self.featured = bound.featured
                            self.gift_wrap = bound.gift_wrap
                            self.custom_botanical_name = bound.custom_botanical_name
                            self.custom_climate_zone = bound.custom_climate_zone
                            self.custom_plant_type = bound.custom_plant_type
                            self.custom_type = bound.custom_type
                            self.custom_height = bound.custom_height
                            self.custom_width = bound.custom_width
                            self.custom_sun_exposure = bound.custom_sun_exposure
                            self.custom_bloom_time = bound.custom_bloom_time
                            self.custom_bloom_color = bound.custom_bloom_color
                            self.custom_attracts_pollinators = bound.custom_attracts_pollinators
                            self.custom_growth_rate = bound.custom_growth_rate
                            self.custom_deer_resistant = bound.custom_deer_resistant
                            self.custom_soil_type = bound.custom_soil_type
                            self.custom_color = bound.custom_color
                            self.custom_size = bound.custom_size
                            self.ecommerce_categories = bound.ecommerce_categories
                            self.custom_url = bound.custom_url
                            self.is_custom_url = bound.is_custom_url
                            self.custom_field_ids = bound.custom_field_ids

                    def get_binding_id_images():
                        binding_images = []
                        photo_path = creds.photo_path
                        list_of_files = os.listdir(photo_path)
                        if list_of_files is not None:
                            for x in list_of_files:
                                if x.split(".")[0].split("^")[0] == self.binding_id:
                                    binding_images.append(x)
                        total_binding_images = len(binding_images)
                        if total_binding_images > 0:
                            # print(f"Found {total_binding_images} binding images for Binding ID: {self.binding_id}")
                            for image in binding_images:
                                binding_img = self.Image(image, last_run_time=self.last_sync)
                                if binding_img.last_modified_dt > self.last_sync and binding_img.validate():
                                    self.images.append(binding_img)

                        else:
                            print(f"No binding images found for Binding ID: {self.binding_id}")

                    # Add Binding ID Images to image list
                    get_binding_id_images()

                    # Add Variant Images to image list and establish which image is the variant thumbnail
                    for variant in self.variants:
                        variant_image_count = 0
                        for variant_image in variant.images:
                            if variant_image_count == 0:
                                variant_image.is_variant_image = True
                            self.images.append(variant_image)
                            variant_image_count += 1

                def get_single_product_details():
                    self.variants.append(self.Variant(self.sku, self.last_sync))
                    single = self.variants[0]
                    self.product_id = single.product_id
                    self.web_title = single.web_title
                    self.default_price = single.price_1
                    self.cost = single.cost
                    self.sale_price = single.price_2
                    self.weight = single.weight
                    self.width = single.width
                    self.height = single.height
                    self.depth = single.depth
                    self.brand = single.brand
                    self.in_store_only = single.in_store_only
                    self.sort_order = single.sort_order
                    self.buffered_quantity = single.quantity_available - single.buffer
                    if self.buffered_quantity < 0:
                        self.buffered_quantity = 0
                    self.html_description = single.html_description
                    self.search_keywords = single.search_keywords
                    self.meta_title = single.meta_title
                    self.meta_description = single.meta_description
                    self.visible = single.visible
                    self.featured = single.featured
                    self.gift_wrap = single.gift_wrap
                    self.custom_botanical_name = single.custom_botanical_name
                    self.custom_climate_zone = single.custom_climate_zone
                    self.custom_plant_type = single.custom_plant_type
                    self.custom_type = single.custom_type
                    self.custom_height = single.custom_height
                    self.custom_width = single.custom_width
                    self.custom_sun_exposure = single.custom_sun_exposure
                    self.custom_bloom_time = single.custom_bloom_time
                    self.custom_bloom_color = single.custom_bloom_color
                    self.custom_attracts_pollinators = single.custom_attracts_pollinators
                    self.custom_growth_rate = single.custom_growth_rate
                    self.custom_deer_resistant = single.custom_deer_resistant
                    self.custom_soil_type = single.custom_soil_type
                    self.custom_color = single.custom_color
                    self.custom_size = single.custom_size
                    self.ecommerce_categories = single.ecommerce_categories
                    self.images = single.images
                    self.custom_url = single.custom_url
                    self.is_custom_url = single.is_custom_url
                    self.custom_field_ids = single.custom_field_ids

                if self.is_bound:
                    get_bound_product_details()
                else:
                    get_single_product_details()

                def get_bc_categories():
                    result = []
                    if self.ecommerce_categories is not None:
                        for category in self.ecommerce_categories:
                            categ_query = f"""
                            SELECT BC_CATEG_ID 
                            FROM SN_CATEG
                            WHERE CP_CATEG_ID = '{category}'
                            """
                            db = query_engine.QueryEngine()
                            cat_response = db.query_db(categ_query)
                            if cat_response is not None:
                                result.append(cat_response[0][0])
                        return result
                    else:
                        return []

                self.ecommerce_categories = get_bc_categories()

                # Now all images are in self.images list and are in order by binding img first then variant img

                sort_order = 0
                for x in self.images:
                    if sort_order == 0:
                        x.is_thumbnail = True
                    x.sort_order = sort_order
                    sort_order += 1

            def validate_product_inputs(self):
                check_web_title = True
                check_for_missing_categories = True
                check_html_description = False
                min_description_length = 20
                check_missing_images = True
                check_for_missing_brand = True

                def set_parent(status: bool = True) -> None:
                    """Target lowest price item in family to set as parent."""
                    # Reestablish parent relationship
                    flag = 'Y' if status else 'N'

                    target_item = min(self.variants, key=lambda x: x.price_1).sku

                    query = f"""
                    UPDATE IM_ITEM
                    SET IS_ADM_TKT = '{flag}', LST_MAINT_DT = GETDATE()
                    WHERE ITEM_NO = '{target_item}'
                    """
                    self.db.query_db(query, commit=True)
                    print(f"Parent status set to {flag} for {target_item}")
                    return self.get_product_details()

                # Bound Product Validation
                if self.is_bound:
                    # print(f"Product {self.binding_id} is a bound product. Validation starting...")
                    if self.validation_retries > 0:
                        # Test for missing binding ID. Potentially add corrective action
                        # (i.e. generate binding ID or remove product
                        # and rebuild as a new single product)
                        if self.binding_id == "":
                            message = f"Product {self.binding_id} has no binding ID. Validation failed."
                            self.errors.append(message)
                            print(message)
                            return False

                        # Test for valid Binding ID Schema (ex. B0001)
                        pattern = r'B\d{4}'
                        if not bool(re.fullmatch(pattern, self.binding_id)):
                            message = f"Product {self.binding_id} has an invalid binding ID. Validation failed."
                            self.errors.append(message)
                            print(message)
                            return False

                        # Test for parent product problems
                        if len(self.parent) != 2:
                            # Test for missing parent
                            if len(self.parent) == 0:
                                message = f"Product {self.binding_id} has no parent. Will reestablish parent."
                                print(message)
                                set_parent()
                                self.validation_retries -= 1
                                if self.validation_retries > 1:
                                    return self.validate_product_inputs()
                                else:
                                    message = f"Product {self.binding_id} has no parent. Validation failed."
                                    self.errors.append(message)
                                    print(message)
                                    return False

                            # Test for multiple parents
                            if len(self.parent) > 1:
                                print(f"Product {self.binding_id} has multiple parents. Will reestablish parent.")
                                self.remove_parent()
                                set_parent()
                                self.validation_retries -= 1
                                if self.validation_retries > 1:
                                    return self.validate_product_inputs()
                                else:
                                    message = f"Product {self.binding_id} has multiple parents. Validation failed."
                                    self.errors.append(message)
                                    print(message)
                                    return False

                        if check_web_title:
                            # Test for missing web title
                            if self.web_title == "":
                                message = f"Product {self.binding_id} is missing a web title. Validation failed."
                                self.errors.append(message)
                                print(message)
                                return False

                        # Test for missing html description
                        if check_html_description:
                            if len(self.html_description) < min_description_length:
                                message = f"Product {self.binding_id} is missing an html description. Validation failed."
                                self.errors.append(message)
                                print(message)
                                return False

                        # Test for missing E-Commerce Categories
                        if len(self.ecommerce_categories) == 0:
                            message = f"Product {self.binding_id} is missing E-Commerce Categories. Validation failed."
                            self.errors.append(message)
                            print(message)
                            return False

                        # Test for missing variant names
                        for child in self.variants:
                            if child.variant_name == "":
                                message = f"Product {child.sku} is missing a variant name. Validation failed."
                                self.errors.append(message)
                                print(message)
                                return False

                # ALL PRODUCTS

                if check_for_missing_categories:
                    # Test for missing E-Commerce Categories
                    if len(self.ecommerce_categories) == 0:
                        message = f"Product {self.sku} is missing E-Commerce Categories. Validation failed."
                        self.errors.append(message)
                        print(message)
                        return False
                if check_for_missing_brand:
                    # Test for missing brand
                    if self.brand == "":
                        message = f"Product {self.sku} is missing a brand. Validation failed."
                        self.errors.append(message)
                        print(message)
                        return False

                # Test for missing cost
                if self.cost == 0:
                    message = f"Product {self.sku} is missing a cost. Validation passed for now :)."
                    self.errors.append(message)
                    print(message)

                # Test for missing price 1
                if self.default_price == 0:
                    message = f"Product {self.sku} is missing a price 1. Validation failed."
                    self.errors.append(message)
                    print(message)
                    return False

                if check_html_description:
                    # Test for missing html description
                    if len(self.html_description) < min_description_length:
                        message = f"Product {self.sku} is missing an html description. Validation failed."
                        self.errors.append(message)
                        print(message)
                        return False

                if check_web_title:
                    # Single Product Validation
                    # Test for missing web title
                    if self.web_title == "":
                        message = f"Product {self.sku} is missing a web title. Validation failed."
                        self.errors.append(message)
                        print(message)
                        return False

                if check_missing_images:
                    # Test for missing product images
                    if len(self.images) == 0:
                        message = f"Product {self.binding_id} is missing images. Will turn visibility to off."
                        print(message)
                        self.visible = False

                # Need validations for character counts on all fields
                # print(f"Product {self.sku} has passed validation.")
                # Validation has Passed.
                return True

            def process(self):
                def create():
                    def bc_create_product():
                        """Create product in BigCommerce. For this implementation, this is a single product with no
                        variants"""
                        url = f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products'
                        payload = self.construct_product_payload()
                        bc_response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)

                        if bc_response.status_code in [200, 207]:
                            print(f"BigCommerce POST {self.sku}: SUCCESS. Code: {bc_response.status_code}")

                            # from utilities import handy_tools
                            # print(handy_tools.pretty_print(bc_response))

                            # assign PRODUCT_ID, VARIANT_ID, and CATEG_ID to product and insert into middleware
                            self.product_id = bc_response.json()["data"]["id"]
                            self.custom_field_response = bc_response.json()["data"]["custom_fields"]

                            for x in range(0, len(self.variants)):
                                self.variants[x].binding_id = self.binding_id
                                self.variants[x].product_id = self.product_id
                                self.variants[x].variant_id = bc_response.json()["data"]["variants"][x]["id"]

                            for x in range(0, len(self.images)):
                                self.images[x].binding_id = self.binding_id
                                self.images[x].product_id = self.product_id
                                self.images[x].image_id = bc_response.json()["data"]["images"][x]["id"]

                            return True

                        elif bc_response.status_code == 409:
                            message = f"Product {self.sku} already exists in BigCommerce."
                            self.errors.append(message)
                            self.errors.append(bc_response.content)
                            print(message)
                            return False

                        elif bc_response.status_code == 422:
                            message = f"Product {self.sku} failed to create in BigCommerce. Invalid Fields: Code 422"
                            self.errors.append(message)
                            self.errors.append(bc_response.content)
                            print(message)
                            return False

                        else:
                            error_message = (f"BigCommerce POST {self.sku}: "
                                             f"FAILED! Status Code: {bc_response.status_code}")
                            self.errors.append(error_message)
                            self.errors.append(bc_response.content)
                            print(error_message)
                            return False

                    def middleware_insert_product():
                        def stringify_categories():
                            return ",".join(str(category) for category in self.ecommerce_categories)

                        db = query_engine.QueryEngine()

                        custom_field_ids = []
                        custom_field_string = None

                        if self.custom_field_response:
                            for entry in self.custom_field_response:
                                custom_field_ids.append(entry["id"])
                            custom_field_string = ",".join(str(cust_field) for cust_field in custom_field_ids)

                        success = True

                        for variant in self.variants:
                            if not variant.is_parent:
                                custom_field_string = None
                            insert_query = (f"INSERT INTO {creds.bc_product_table} (ITEM_NO, BINDING_ID, IS_PARENT, "
                                            f"PRODUCT_ID, VARIANT_ID, CATEG_ID, CUSTOM_FIELDS) VALUES ('{variant.sku}', "
                                            f"{f"'{self.binding_id}'" if self.binding_id != '' else 'NULL'}, "
                                            f"{1 if variant.is_parent else 0}, {self.product_id}, "
                                            f"{variant.variant_id if variant.variant_id != '' else "NULL"}, "
                                            f"'{stringify_categories()}', "
                                            f"{f"'{custom_field_string}'" if custom_field_string else "NULL"})")

                            # INSERT INTO SQL
                            try:
                                insert_product_response = db.query_db(insert_query, commit=True)
                            except Exception as e:
                                insert_prod_message = f"Middleware INSERT product {self.sku}: FAILED {e}"
                                print(insert_prod_message)
                                self.errors.append(insert_prod_message)
                                success = False
                            else:
                                if insert_product_response["code"] == 200:
                                    # print(f"SKU: {variant.sku} Binding: {self.binding_id} "
                                    #       f"Product : inserted into middleware.")
                                    pass

                                else:
                                    message = (f"Middleware INSERT product {self.sku}: "
                                               f"Non 200 response: {insert_product_response}")
                                    print(message)
                                    self.errors.append(message)
                                    self.errors.append(insert_product_response)
                                    success = False

                        if success:
                            print(f"Product {self.sku} Binding: {self.binding_id} inserted into middleware.")

                        else:
                            # Rollback
                            # If any of the variants failed to write to the middleware, delete associated skus from
                            # middleware and BigCommerce
                            create_rollback_query = (f"DELETE FROM {creds.bc_product_table} "
                                                     f"WHERE PRODUCT_ID = '{self.product_id}'")
                            db.query_db(create_rollback_query, commit=True)
                            # Delete product from BigCommerce
                            url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                   f'catalog/products/{self.product_id}')
                            delete_response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                            if delete_response.status_code == 204:
                                message = f"Product {self.sku} deleted from BigCommerce."
                                print(message)
                                self.errors.append(message)
                            else:
                                message = f"Error deleting product {self.sku} from BigCommerce."
                                print(message)
                                self.errors.append(message)
                                self.errors.append(delete_response.content)

                        return success

                    def middleware_insert_images():
                        success = True
                        for image in self.images:
                            query = (f"INSERT INTO {creds.bc_image_table} (IMAGE_NAME, ITEM_NO, FILE_PATH, IMAGE_URL, "
                                     f"PRODUCT_ID, IMAGE_ID, THUMBNAIL, IMAGE_NUMBER, SORT_ORDER, IS_BINDING_IMAGE, "
                                     f"BINDING_ID, IS_VARIANT_IMAGE, DESCR, LST_MOD_DT) VALUES ('{image.image_name}', "
                                     f"{f"'{image.sku}'" if image.sku != '' else 'NULL'}, '{image.file_path}', "
                                     f"'{image.image_url}', '{image.product_id}', '{image.image_id}', "
                                     f"'{1 if image.is_thumbnail else 0}', '{image.image_number}', "
                                     f"'{image.sort_order}', '{image.is_binding_image}', "
                                     f"{f"'{image.binding_id}'" if image.binding_id != '' else 'NULL'}, "
                                     f"'{image.is_variant_image}', "

                                     f"{f"'{image.description.replace("'", "''")}'" if
                                     image.description != '' else 'NULL'}, "

                                     f"'{image.last_modified_dt:%Y-%m-%d %H:%M:%S}')")
                            try:
                                insert_image_response = query_engine.QueryEngine().query_db(query, commit=True)
                            except Exception as e:
                                message = f"Middleware INSERT image {image.image_name}: FAILED {e}"
                                print(message)
                                self.errors.append(message)
                                success = False
                            else:
                                if insert_image_response["code"] == 200:
                                    # print(f"Image: {image.image_name}: inserted into middleware.")
                                    pass
                                else:
                                    message = (f"Middleware INSERT image {image.image_name}: "
                                               f"Non 200 response: {insert_image_response}")
                                    print(message)
                                    self.errors.append(message)
                                    self.errors.append(insert_image_response)
                                    success = False
                        if success:
                            print(f"Product {self.sku} Binding: {self.binding_id} Images: inserted into middleware.")
                        if not success:
                            # Rollback
                            # If any of the images failed to write to the middleware, delete associated images from
                            # middleware and BigCommerce
                            query = f"DELETE FROM {creds.bc_image_table} WHERE PRODUCT_ID = '{self.product_id}'"
                            query_engine.QueryEngine().query_db(query, commit=True)
                            # Delete product from BigCommerce
                            url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                   f'catalog/products/{self.product_id}')
                            delete_response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                            if delete_response.status_code == 204:
                                message = f"Product {self.sku} deleted from BigCommerce."
                                print(message)
                                self.errors.append(message)
                            else:
                                message = f"Error deleting product {self.sku} from BigCommerce."
                                print(message)
                                self.errors.append(message)
                                self.errors.append(delete_response.content)

                        return success

                    if bc_create_product():
                        if middleware_insert_product():
                            middleware_insert_images()
                            return

                def update():
                    """Will update existing product. Will clear out custom field data and reinsert."""
                    # Step 1: Process Deleting Variants
                    # Get a list of variants in DB. Compare to list of variants in self.variants.
                    # If a variant exists in DB but not in self.variants, delete it from DB and BC.

                    # Step 2: Second-State Validation
                    # Validate the product again after the variants have been deleted.
                    # Specifically, check for a bound product turning into a single product.
                    # check for single product becoming bound.
                    # Check for change of parent.
                    # If these product breaking things occur, delete the product from BC and Middleware.
                    # Add them both back into the product queue
                    # OR we could simply FAIL and return an error in log.

                    # Step 3: Create an updated Payload
                    update_payload = self.construct_product_payload(mode="update_product")

                    # Step 4: Delete all Custom Fields
                    def bc_delete_custom_fields(asynchronous=False):
                        def get_custom_fields_from_bc():
                            custom_fields = []
                            cf_url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                                      f"catalog/products/{self.product_id}/custom-fields")
                            cf_response = requests.get(url=cf_url, headers=creds.test_bc_api_headers)
                            if cf_response.status_code == 200:
                                custom_field_data = cf_response.json()["data"]
                                for field in custom_field_data:
                                    custom_fields.append(field["id"])
                            return custom_fields

                        id_list = self.custom_field_ids.split(",")

                        if not id_list:
                            return

                        if asynchronous:
                            async def bc_delete_custom_fields_async():
                                async with aiohttp.ClientSession() as session:
                                    for field_id in id_list:
                                        async_url = f"""https://api.bigcommerce.com/stores/{creds.test_big_store_hash}
                                        /v3/catalog/products/{self.product_id}/custom-fields/{field_id}"""
                                        async with session.get(url=async_url, headers=creds.test_header) as resp:
                                            text_response = await resp.text()
                                            print(text_response)

                            asyncio.run(bc_delete_custom_fields_async())
                        else:
                            # Synchronous Version
                            success = True
                            success_list = []
                            db = query_engine.QueryEngine()
                            # Delete Each Custom Field
                            for number in id_list:
                                url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                                       f"catalog/products/{self.product_id}/custom-fields/{number}")

                                cf_remove_response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                                if cf_remove_response.status_code == 204:
                                    success_list.append(number)
                                else:
                                    success = False
                                    message = f"BigCommerce: Error deleting custom fields for Product {self.sku}."
                                    print(message)
                                    self.errors.append(message)
                                    print(cf_remove_response.content)
                            # If all custom fields were deleted successfully, remove CUSTOM_FIELDS from middleware
                            if success:
                                update_cf1_query = f"""UPDATE {creds.bc_product_table}
                                        SET CUSTOM_FIELDS = NULL, LST_MAINT_DT = GETDATE()
                                        WHERE PRODUCT_ID = '{self.product_id}' AND IS_PARENT = 1
                                        """
                                try:
                                    db.query_db(update_cf1_query, commit=True)
                                except Exception as e:
                                    message = f"Middleware REMOVE CUSTOM_FIELDS: {self.sku}: FAILED ", e
                                    print(message)
                                    self.errors.append(message)
                            else:
                                # Partial Success
                                # If this wasn't totally successful, but some were deleted,
                                # update the CUSTOM_FIELDS in middleware
                                if success_list:
                                    success_list_string = ",".join(str(cust_field) for cust_field in success_list)
                                    update_cf2_query = f"""
                                    UPDATE {creds.bc_product_table}
                                    SET CUSTOM_FIELDS = '{success_list_string}', LST_MAINT_DT = GETDATE()
                                    WHERE PRODUCT_ID = '{self.product_id}' AND IS_PARENT = 1
                                    """
                                    try:
                                        db.query_db(update_cf2_query, commit=True)
                                    except Exception as e:
                                        message = f"Middleware CUSTOM_FIELDS ROLLBACK: {self.sku}: FAILED ", e
                                        print(message)
                                        self.errors.append(message)
                                else:
                                    # If no custom fields were deleted, but there was an error,
                                    # don't update the CUSTOM_FIELDS in middleware
                                    pass

                    bc_delete_custom_fields()

                    # Step 5: Add any new variants if needed

                    # Step 6: Update Product on BC with new Payload, which will include new sort order, etc for new
                    # images and variants
                    def bc_update_product():
                        url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                               f'catalog/products/{self.product_id}?include=custom_fields,variants,images')
                        update_response = requests.put(url=url, headers=creds.test_bc_api_headers, json=update_payload)
                        if update_response.status_code in [200, 201, 207]:
                            self.product_id = update_response.json()["data"]["id"]
                            self.custom_field_response = update_response.json()["data"]["custom_fields"]
                            for x in range(0, len(self.variants)):
                                self.variants[x].binding_id = self.binding_id
                                self.variants[x].product_id = self.product_id
                                self.variants[x].variant_id = update_response.json()["data"]["variants"][x]["id"]
                            for x in range(0, len(self.images)):
                                self.images[x].binding_id = self.binding_id
                                self.images[x].product_id = self.product_id
                                self.images[x].image_id = update_response.json()["data"]["images"][x]["id"]
                            if self.custom_field_response:
                                custom_field_list = []
                                for x in self.custom_field_response:
                                    custom_field_list.append(x['id'])
                                self.custom_field_ids = ",".join(str(x) for x in custom_field_list)
                                return True

                        elif update_response.status_code == 404:
                            message = f"Product {self.sku} failed to update in BigCommerce. Product not found."
                        elif update_response.status_code == 409:
                            message = f"Product {self.sku} failed to update in BigCommerce. Product already exists."
                        elif update_response.status_code == 422:
                            message = f"Product {self.sku} failed to update in BigCommerce. Invalid Fields: Code 422"
                        else:
                            message = f"Product {self.sku} failed to update in BigCommerce. Invalid Fields: Code 422"
                        self.errors.append(message)
                        self.errors.append(update_response.content)
                        print(message)
                        return False

                    def middleware_sync_product():
                        def stringify_categories():
                            return ",".join(str(category) for category in self.ecommerce_categories)

                        success = True

                        db = query_engine.QueryEngine()

                        custom_field_ids = []
                        custom_field_string = None
                        if self.custom_field_response:
                            for entry in self.custom_field_response:
                                custom_field_ids.append(entry["id"])
                            custom_field_string = ",".join(str(cust_field) for cust_field in custom_field_ids)

                        for variant in self.variants:
                            if not variant.is_parent:
                                custom_field_string = None
                            # ---------------------------- #
                            # NEW PRODUCT INSERTS INTO SQL #
                            # ---------------------------- #
                            if variant.db_id is None:
                                # If variant.db_id is None, this is a new product to be inserted into SQL
                                insert_query = (
                                    f"INSERT INTO {creds.bc_product_table} (ITEM_NO, BINDING_ID, IS_PARENT, "
                                    f"PRODUCT_ID, VARIANT_ID, CATEG_ID) VALUES ('{variant.sku}', "
                                    f"{f"'{self.binding_id}'" if self.binding_id != '' else 'NULL'}, "
                                    f"{1 if variant.is_parent else 0}, {self.product_id}, "
                                    f"{variant.variant_id if variant.variant_id != '' else "NULL"}, "
                                    f"'{stringify_categories()}')")

                                # INSERT INTO SQL
                                try:
                                    insert_product_response = db.query_db(insert_query, commit=True)
                                except Exception as e:
                                    insert_prod_message = f"Middleware INSERT product {self.sku}: FAILED {e}"
                                    print(insert_prod_message)
                                    self.errors.append(insert_prod_message)
                                    success = False
                                else:
                                    if insert_product_response["code"] == 200:
                                        # print(f"SKU: {variant.sku} Binding: {self.binding_id} "
                                        #       f"Product : inserted into middleware.")
                                        pass

                                    else:
                                        message = (f"Middleware INSERT product {self.sku}: "
                                                   f"Non 200 response: {insert_product_response}")
                                        print(message)
                                        self.errors.append(message)
                                        self.errors.append(insert_product_response)
                                        success = False
                            else:
                                # ---------------------------- #
                                # NEW PRODUCT INSERTS INTO SQL #
                                # ---------------------------- #
                                # If variant.db_id is not None, this is an existing product to be updated in SQL

                                update_query = (f"UPDATE {creds.bc_product_table} "
                                                f"SET ITEM_NO = '{variant.sku}', "
                                                f"BINDING_ID = "
                                                f"{f"'{self.binding_id}'" if self.binding_id != '' else 'NULL'}, "
                                                f"IS_PARENT = {1 if variant.is_parent else 0}, "
                                                f"PRODUCT_ID = {self.product_id}, "
                                                f"VARIANT_ID = "
                                                f"{variant.variant_id if variant.variant_id != '' else 'NULL'}, "
                                                f"CATEG_ID = '{stringify_categories()}', "
                                                f"CUSTOM_FIELDS = '{custom_field_string}', "
                                                f"LST_MAINT_DT = GETDATE() "
                                                f"WHERE ID = {variant.db_id}")

                                # UPDATE VARIANT IN SQL
                                try:
                                    update_product_response = db.query_db(update_query, commit=True)
                                except Exception as e:
                                    insert_prod_message = f"Middleware UPDATE product {self.sku}: FAILED {e}"
                                    print(insert_prod_message)
                                    self.errors.append(insert_prod_message)
                                    success = False
                                else:
                                    if update_product_response["code"] == 200:
                                        # print(f"SKU: {variant.sku} Binding: {self.binding_id} "
                                        #       f"Product : inserted into middleware.")
                                        pass

                                    else:
                                        message = (f"Middleware UPDATE product {self.sku}: "
                                                   f"Non 200 response: {update_product_response}")
                                        print(message)
                                        self.errors.append(message)
                                        self.errors.append(update_product_response)
                                        success = False

                        if not success:
                            # Rollback
                            # If any of the variants failed to write to the middleware, delete associated skus from
                            # middleware and BigCommerce
                            product_rollback_query = (f"DELETE FROM {creds.bc_product_table} "
                                                      f"WHERE PRODUCT_ID = '{self.product_id}'")
                            db.query_db(product_rollback_query, commit=True)
                            # Could improve this with a join or SQL trigger...
                            images_rollback_query = (f"DELETE FROM {creds.bc_image_table} "
                                                     f"WHERE PRODUCT_ID = '{self.product_id}'")

                            db.query_db(images_rollback_query, commit=True)

                            # Delete product from BigCommerce
                            url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                   f'catalog/products/{self.product_id}')
                            delete_response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                            if delete_response.status_code == 204:
                                message = f"Product {self.sku} deleted from BigCommerce."
                                print(message)
                                self.errors.append(message)
                            else:
                                message = f"Error deleting product {self.sku} from BigCommerce."
                                print(message)
                                self.errors.append(message)
                                self.errors.append(delete_response.content)

                        return success

                    if bc_update_product():
                        middleware_sync_product()

                    # def middleware_update_product():
                    #     query = f"""
                    #     UPDATE {creds.bc_product_table}
                    #     SET WEB_TITLE = '{self.web_title}',
                    #     HTML_DESCRIPTION = '{self.html_description}'
                    #     WHERE ITEM_NO = '{self.sku}'
                    #     """
                    #     try:
                    #         query_engine.QueryEngine().query_db(query, commit=True)
                    #     except Exception as e:
                    #         print(f"Middleware UPDATE product {self.sku}: FAILED")
                    #         print(e)
                    #     else:
                    #         print(f"Middleware UPDATE product {self.sku}: SUCCESS")
                    #
                    # if bc_update_product() == 200:
                    #     middleware_update_product()

                query = f"""SELECT *
                        FROM {creds.bc_product_table}
                        WHERE ITEM_NO = '{self.sku}'"""
                response = self.db.query_db(query)
                if response is None:
                    # Product Not Found, Create New Product
                    create()
                else:
                    # Product Found, Update Product
                    update()

            def construct_product_payload(self, mode="create"):

                def get_ecomm_categories():
                    """EDIT!!! YOU NEED TO GET the separate categories from EC_CATEG_LIN for the item and then
                    Put them into a comma separated string. Then split the string and get the BC category for
                    each of those items. Finally return the BC category ID for each of those items in a list for
                    payload."""
                    result = []
                    for category in str(self.ecommerce_categories).split(","):
                        query = f"""
                        SELECT BC_CATEG_ID 
                        FROM SN_CATEG MW
                        INNER JOIN EC_CATEG CP 
                        on CP.CATEG_ID = MW.CP_CATEG_ID
                        """
                        response = self.db.query_db(query)
                        print(result)
                        if response is not None:
                            result.append(response[0][0])
                    return result

                def get_brand_id():
                    query = f"""
                    SELECT BC_BRAND_ID
                    FROM {creds.bc_brands_table} BRANDS
                    INNER JOIN IM_ITEM_PROF_COD COD on BRANDS.CP_BRAND_ID = COD.PROF_COD
                    WHERE CP_BRAND_ID = '{self.brand}'
                    """
                    response = self.db.query_db(query)
                    if response is not None:
                        return response[0][0]
                    else:
                        return None

                def construct_custom_fields():
                    result = []

                    if self.custom_botanical_name:
                        result.append({
                            "name": "Botanical Name",
                            "value": self.custom_botanical_name
                        })
                    if self.custom_climate_zone:
                        result.append({
                            "name": "Climate Zone",
                            "value": self.custom_climate_zone
                        })
                    if self.custom_plant_type:
                        result.append({
                            "name": "Plant Type",
                            "value": self.custom_plant_type
                        })
                    if self.custom_type:
                        result.append({
                            "name": "Type",
                            "value": self.custom_type
                        })
                    if self.custom_height:
                        result.append({
                            "name": "Height",
                            "value": self.custom_height
                        })
                    if self.custom_width:
                        result.append({
                            "name": "Width",
                            "value": self.custom_width
                        })
                    if self.custom_sun_exposure:
                        result.append({
                            "name": "Sun Exposure",
                            "value": self.custom_sun_exposure
                        })
                    if self.custom_bloom_time:
                        result.append({
                            "name": "Bloom Time",
                            "value": self.custom_bloom_time
                        })
                    if self.custom_bloom_color:
                        result.append({
                            "name": "Bloom Color",
                            "value": self.custom_bloom_color
                        })
                    if self.custom_attracts_pollinators:
                        result.append({
                            "name": "Attracts Pollinators",
                            "value": self.custom_attracts_pollinators
                        })
                    if self.custom_growth_rate:
                        result.append({
                            "name": "Growth Rate",
                            "value": self.custom_growth_rate
                        })
                    if self.custom_deer_resistant:
                        result.append({
                            "name": "Deer Resistant",
                            "value": self.custom_deer_resistant
                        })
                    if self.custom_soil_type:
                        result.append({
                            "name": "Soil Type",
                            "value": self.custom_soil_type
                        })
                    if self.custom_color:
                        result.append({
                            "name": "Color",
                            "value": self.custom_color
                        })
                    if self.custom_size:
                        result.append({
                            "name": "Size",
                            "value": self.custom_size
                        })
                    return result

                def construct_image_payload():
                    result = []
                    # Child Images
                    for image in self.images:
                        if image.last_modified_dt > self.last_sync and image.validate():
                            image_payload = {
                                "product_id": self.product_id,
                                "is_thumbnail": image.is_thumbnail,
                                "sort_order": image.sort_order,
                                "description": f"""{image.description}""",
                                "image_url": image.image_url,
                            }
                            if image.image_id:
                                image_payload["id"] = image.image_id

                            result.append(image_payload)

                    return result

                def construct_video_payload():
                    return []
                    # result = []
                    # for video in self.videos:
                    #     result.append([
                    #     {
                    #         "title": "Writing Great Documentation",
                    #         "description": "A video about documentation",
                    #         "sort_order": 1,
                    #         "type": "youtube",
                    #         "video_id": "z3fRu9pkuXE",
                    #         "id": 0,
                    #         "product_id": 0,
                    #         "length": "string"
                    #     }
                    # ])

                def construct_variant_payload():
                    result = []
                    if len(self.variants) > 1:
                        id_index = 1
                        for child in self.variants:
                            variant_payload = {
                                "cost_price": child.cost,
                                "price": child.price_1,
                                "image_url": child.variant_image_url,
                                "sale_price": child.price_2,
                                "retail_price": child.price_1,
                                "weight": child.weight,
                                "width": child.width,
                                "height": child.height,
                                "depth": child.depth,
                                "is_free_shipping": child.is_free_shipping,
                                # "fixed_cost_shipping_price": 0.1,
                                "purchasing_disabled": child.purchasing_disabled,
                                "purchasing_disabled_message": child.purchasing_disabled_message,
                                "upc": "string",
                                "inventory_level": child.buffered_quantity,
                                # "inventory_warning_level": 2147483647,
                                # "bin_picking_number": "string",
                                # "mpn": "string",
                                # "gtin": "012345678905",
                                "product_id": child.product_id,
                                "id": child.variant_id,
                                "sku": child.sku,
                                "option_values": [
                                    {
                                        "option_display_name": "Option",
                                        "label": child.variant_name
                                    }
                                ],
                                "calculated_price": 0.1,
                                "calculated_weight": 0.1
                            }
                            result.append(variant_payload)

                    return result

                payload = {
                    "name": self.web_title,
                    "type": "physical",
                    "sku": self.binding_id if self.binding_id != "" else self.sku,
                    "description": f"""{self.html_description}""",
                    "weight": self.weight,
                    "width": self.width,
                    "depth": self.depth,
                    "height": self.height,
                    "price": self.default_price,
                    "cost_price": self.cost,
                    "retail_price": self.default_price,
                    "sale_price": self.sale_price,
                    "map_price": 0,
                    "tax_class_id": 0,
                    "product_tax_code": "string",
                    "categories": self.ecommerce_categories,
                    "brand_id": get_brand_id(),
                    "brand_name": self.brand,
                    "inventory_level": self.buffered_quantity,
                    "inventory_warning_level": 10,
                    "inventory_tracking": "variant" if self.is_bound else "product",
                    # "fixed_cost_shipping_price": 0.1,
                    "is_free_shipping": False,
                    "is_visible": self.visible,
                    "is_featured": self.featured,
                    "sort_order": self.sort_order,
                    "search_keywords": self.search_keywords,
                    "availability": "available" if not self.in_store_only else "disabled",
                    "gift_wrapping_options_type": "none" if not self.gift_wrap else "any",
                    # "gift_wrapping_options_list": [
                    #     0
                    # ],
                    "condition": "New",
                    "is_condition_shown": True,
                    "page_title": self.meta_title,
                    "meta_description": self.meta_description,
                    # "preorder_release_date": self.preorder_release_date,
                    "preorder_message": self.preorder_message,
                    "is_preorder_only": self.is_preorder_only,
                    "is_price_hidden": self.is_price_hidden,
                    "price_hidden_label": "string",

                    # "date_last_imported": "string",

                    "custom_fields": construct_custom_fields(),

                    # "bulk_pricing_rules": [
                    #     {
                    #         "quantity_min": 10,
                    #         "quantity_max": 50,
                    #         "type": "price",
                    #         "amount": 10
                    #     }
                    # ],
                    "images": construct_image_payload(),
                    "videos": construct_video_payload(),
                    "variants": construct_variant_payload(),
                }
                if mode == "update_product":
                    payload["id"] = self.product_id

                if self.custom_url != "":
                    print("adding custom url")
                    payload["custom_url"] = {
                        "url": f"/{self.custom_url}/",
                        "is_customized": self.is_custom_url,
                        "create_redirect": True
                    }

                return payload

            def middleware_create_product(self):
                pass

            def update_bound_product(self):
                self.bc_update_product()
                self.middleware_update_product()

            def bc_delete_product(self):
                url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                       f"catalog/products/products?id:in={self.product_id}")
                response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                if response.status_code == 204:
                    print(f"Product {self.binding_id} deleted successfully. Code: {response.status_code}")
                else:
                    print(f"Error deleting product {self.binding_id}. Code: {response.status_code}")
                    print(response.json())

            def middleware_update_product(self):
                pass

            def bc_update_variant(self):
                payload = self.Variant.construct_variant_payload()
                url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                       f"catalog/products/{self.product_id}/variants")
                response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                if response.status_code == 200:
                    print(f"Product {self.binding_id} updated successfully.")
                else:
                    print(f"Error updating product {self.binding_id}.")
                    print(response.json())

            def remove_parent(self):
                """Remove parent status from all children"""
                query = f"""
                        UPDATE IM_ITEM 
                        SET IS_ADM_TKT = 'N', LST_MAINT_DT = GETDATE()
                        WHERE USR_PROF_ALPHA_16 = '{self.binding_id}'
                        """
                self.db.query_db(query, commit=True)
                print("Parent status removed from all children.")

            @staticmethod
            def get_all_binding_ids():
                binding_ids = set()
                db = query_engine.QueryEngine()
                query = f"""
                SELECT USR_PROF_ALPHA_16
                FROM IM_ITEM
                WHERE USR_PROF_ALPHA_16 IS NOT NULL
                """
                response = db.query_db(query)
                if response is not None:
                    for x in response:
                        binding_ids.add(x[0])
                return list(binding_ids)

            class Variant:

                def __init__(self, sku, last_run_date):
                    self.sku = sku
                    print("INITIALIZING VARIANT CLASS FOR ITEM: ", sku)
                    # print("INITIALIZING VARIANT CLASS FOR ITEM: ", sku)
                    self.last_run_date = last_run_date

                    # Product ID Info
                    product_data = self.get_variant_details()

                    # Product Information
                    self.db_id = product_data['db_id']
                    self.binding_id = product_data['binding_id']
                    self.is_parent = True if product_data['is_parent'] == 'Y' else False
                    self.product_id: int = product_data['product_id']
                    self.variant_id: int = product_data['variant_id']
                    self.web_title: str = product_data['web_title']
                    self.variant_name = product_data['variant_name']
                    self.status = product_data['status']
                    self.price_1 = float(product_data['price_1'])
                    self.cost = float(product_data['cost'])
                    self.price_2 = float(product_data['price_2'])
                    # Inventory Levels
                    self.quantity_available = product_data['quantity_available']
                    self.buffer = product_data['buffer']
                    self.buffered_quantity = self.quantity_available - self.buffer
                    if self.buffered_quantity < 0:
                        self.buffered_quantity = 0
                    self.weight = 0.1
                    self.width = 0.1
                    self.height = 0.1
                    self.depth = 0.1
                    self.in_store_only = product_data['in_store_only']
                    self.sort_order = product_data['sort_order']
                    self.is_price_hidden = False
                    # Purchasing Disabled is for Variants Only
                    self.purchasing_disabled = False
                    self.purchasing_disabled_message = ""
                    # Brand
                    self.brand = product_data['brand']
                    self.html_description = product_data['html_description']
                    self.search_keywords = product_data['search_keywords']
                    self.meta_title = product_data['meta_title']
                    self.meta_description = product_data['meta_description']
                    self.visible: bool = product_data['web_visible']
                    self.featured: bool = product_data['is_featured']
                    self.gift_wrap: bool = product_data['gift_wrap']
                    self.is_free_shipping = False
                    self.is_preorder = False
                    self.preorder_release_date = datetime(1970, 1, 1)
                    self.preorder_message = product_data['preorder_message']
                    self.alt_text_1 = product_data['alt_text_1']
                    self.alt_text_2 = product_data['alt_text_2']
                    self.alt_text_3 = product_data['alt_text_3']
                    self.alt_text_4 = product_data['alt_text_4']

                    # Custom Fields
                    self.custom_botanical_name = product_data['custom_botanical_name']
                    self.custom_climate_zone = product_data['custom_climate_zone']
                    self.custom_plant_type = product_data['custom_plant_type']
                    self.custom_type = product_data['custom_type']
                    self.custom_height = product_data['custom_height']
                    self.custom_width = product_data['custom_width']
                    self.custom_sun_exposure = product_data['custom_sun_exposure']
                    self.custom_bloom_time = product_data['custom_bloom_time']
                    self.custom_bloom_color = product_data['custom_bloom_color']
                    self.custom_attracts_pollinators = product_data['custom_attracts_pollinators']
                    self.custom_growth_rate = product_data['custom_growth_rate']
                    self.custom_deer_resistant = product_data['custom_deer_resistant']
                    self.custom_soil_type = product_data['custom_soil_type']
                    self.custom_color = product_data['custom_color']
                    self.custom_size = product_data['custom_size']
                    self.custom_field_ids = product_data['custom_field_ids']
                    self.custom_url = product_data['custom_url']
                    self.custom_url = "-".join(str(re.sub('[^A-Za-z0-9 ]+', '', self.custom_url)).split(" "))
                    self.is_custom_url = product_data['is_custom_url']

                    # Product Images
                    self.images = []

                    # Dates
                    self.lst_maint_dt = datetime(1970, 1, 1)
                    # E-Commerce Categories
                    self.ecommerce_categories = product_data['categories']
                    # Product Schema (i.e. Bound, Single, Variant.)
                    self.item_schema = ""
                    # Processing Method
                    self.processing_method = ""

                    # Initialize Images
                    self.get_local_product_images()

                    # Initialize Variant Image URL
                    if len(self.images) > 0:
                        self.variant_image_url = self.images[0].image_url
                    else:
                        self.variant_image_url = ""

                def __str__(self):
                    result = ""
                    for k, v in self.__dict__.items():
                        result += f"{k}: {v}\n"
                    return result

                def get_variant_details(self):
                    # Get all products that have been updated since the last sync date

                    """Get a list of all products that have been updated since the last run date.
                    Will check IM_ITEM. IM_PRC, IM_INV, EC_ITEM_DESCR, EC_CATEG_ITEM, and Image tables
                    have an after update Trigger implemented for updating IM_ITEM.LST_MAINT_DT."""
                    query = f""" select ISNULL(ITEM.USR_PROF_ALPHA_16, '') as 'Binding ID(0)', 
                    ITEM.IS_ECOMM_ITEM as 'Web Enabled(1)', ISNULL(ITEM.IS_ADM_TKT, 'N') as 'Is Parent(2)', 
                    ISNULL(BC_PROD.PRODUCT_ID, '') as 'Product ID (3)', 
                    ISNULL(BC_PROD.VARIANT_ID, '') as 'Variant ID(4)', ITEM.USR_CPC_IS_ENABLED as 'Web Visible(5)', 
                    ITEM.USR_ALWAYS_ONLINE as 'ALWAYS ONLINE(6)', ITEM.IS_FOOD_STMP_ITEM as 'GIFT_WRAP(7)', 
                    ITEM.PROF_COD_1 as 'BRAND_CP_COD(8)', ITEM.ECOMM_NEW as 'IS_FEATURED(9)', 
                    ITEM.USR_IN_STORE_ONLY as 'IN_STORE_ONLY(10)', ITEM.USR_PROF_ALPHA_27 as 'SORT ORDER(11)', 
                    ISNULL(ITEM.ADDL_DESCR_1, '') as 'WEB_TITLE(12)', ISNULL(ITEM.ADDL_DESCR_2, '') as 'META_TITLE(13)', 
                    ISNULL(USR_PROF_ALPHA_21, '') as 'META_DESCRIPTION(14)', 
                    ISNULL(ITEM.USR_PROF_ALPHA_17, '') as 'VARIANT NAME(15)', 
                    ITEM.STAT as 'STATUS(16)', ISNULL(ITEM.REG_PRC, 0) as 'REG_PRC(17)', 
                    ISNULL(ITEM.PRC_1, 0) as 'PRC_1(18)', ISNULL(PRC.PRC_2, 0) as 'PRC_2(19)', 
                    CAST(ISNULL(INV.QTY_AVAIL, 0) as INTEGER) as 'QUANTITY_AVAILABLE(20)', CAST(ISNULL(ITEM.PROF_NO_1, 0) as INTEGER) as 'BUFFER(21)', 
                    ITEM.ITEM_TYP as 'ITEM_TYPE(22)', ITEM.LONG_DESCR as 'LONG_DESCR(23)', 
                    ISNULL(ITEM.USR_PROF_ALPHA_26, '') as 'SEARCH_KEYWORDS(24)', ITEM.USR_PROF_ALPHA_19 as 'PREORDER_MESSAGE(25)', 
                    ISNULL(EC_ITEM_DESCR.HTML_DESCR, '') as 'HTML_DESCRIPTION(26)', ISNULL(USR_PROF_ALPHA_22, '') as 'ALT_TEXT_1(27)', 
                    ISNULL(USR_PROF_ALPHA_23, '') as 'ALT_TEXT_2(28)', ISNULL(USR_PROF_ALPHA_24, '') as 'ALT_TEXT_3(29)', 
                    ISNULL(USR_PROF_ALPHA_25, '') as 'ALT_TEXT_4(30)', ISNULL(PROF_ALPHA_1, '') as 'BOTANICAL_NAM(31)', 
                    ISNULL(PROF_ALPHA_2, '') as 'CLIMATE_ZONE(32)', ISNULL(PROF_ALPHA_3, '') as 'PLANT_TYPE(33)', 
                    ISNULL(PROF_ALPHA_4, '') as 'TYPE(34)', ISNULL(PROF_ALPHA_5, '') as 'HEIGHT(35)', 
                    ISNULL(USR_PROF_ALPHA_6, '') as 'WIDTH(36)', ISNULL(USR_PROF_ALPHA_7, '') as 'SUN_EXPOSURE(37)', 
                    ISNULL(USR_PROF_ALPHA_8, '') as 'BLOOM_TIME(38)', ISNULL(USR_PROF_ALPHA_9, '') as 'BLOOM_COLOR(39)', 
                    ISNULL(USR_PROF_ALPHA_10, '') as 'ATTRACTS_POLLINATORS(40)', 
                    ISNULL(USR_PROF_ALPHA_11, '') as 'GROWTH_RATE(41)', 
                    ISNULL(USR_PROF_ALPHA_12, '') as 'DEER_RESISTANT(42)', ISNULL(USR_PROF_ALPHA_13, '') as 'SOIL_TYPE(43)', 
                    ISNULL(USR_PROF_ALPHA_14, '') as 'COLOR(44)', ISNULL(USR_PROF_ALPHA_15, '') as 'SIZE(45)', 
                    ITEM.LST_MAINT_DT as 'LST_MAINT_DT(46)', ISNULL(ITEM.LST_COST, 0) as 'LAST_COST(47)', 
                    ITEM.ITEM_NO as 'ITEM_NO (48)', stuff((
                                                        select ',' + EC_CATEG_ITEM.CATEG_ID
                                                        from EC_CATEG_ITEM
                                                        where EC_CATEG_ITEM.ITEM_NO =ITEM.ITEM_NO
                                                        for xml path('')),1,1,'') as 'categories(49)', 
                    
                    BC_PROD.ID as 'db_id(50)', BC_PROD.CUSTOM_FIELDS as 'custom_field_ids(51)'
                    
                                                        
                    FROM IM_ITEM ITEM
                    LEFT OUTER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
                    LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
                    LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
                    LEFT OUTER JOIN {creds.bc_product_table} BC_PROD ON ITEM.ITEM_NO=BC_PROD.ITEM_NO
                    LEFT OUTER JOIN IM_ITEM_PROF_COD COD ON ITEM.PROF_COD_1 = COD.PROF_COD
                    WHERE ITEM.ITEM_NO = '{self.sku}'"""

                    db = query_engine.QueryEngine()
                    item = db.query_db(query)
                    if item is not None:
                        details = {
                            'sku': item[0][48],
                            'db_id': item[0][50],
                            'binding_id': item[0][0],
                            'is_bound': True if item[0][0] else False,
                            'web_enabled': True if item[0][1] == 'Y' else False,
                            'is_parent': item[0][2],
                            'product_id': item[0][3],
                            'variant_id': item[0][4],
                            'web_visible': True if item[0][5] == 'Y' else False,
                            'always_online': True if item[0][6] == 'Y' else False,
                            'gift_wrap': True if item[0][7] == 'Y' else False,
                            'brand': item[0][8],
                            'is_featured': True if item[0][9] == 'Y' else False,
                            'in_store_only': True if item[0][10] == 'Y' else False,
                            'sort_order': int(item[0][11]) if item[0][11] else 0,
                            'web_title': item[0][12],
                            'meta_title': item[0][13],
                            'meta_description': item[0][14],
                            'variant_name': item[0][15],
                            'status': item[0][16],
                            # Product Pricing
                            'reg_price': item[0][17],
                            'price_1': item[0][18],
                            'price_2': item[0][19],
                            # # Inventory Levels
                            'quantity_available': item[0][20],
                            'buffer': item[0][21],
                            # Additional Details
                            'item_type': item[0][22],
                            'long_description': item[0][23],
                            'search_keywords': item[0][24],
                            'preorder_message': item[0][25],
                            'html_description': item[0][26],
                            'alt_text_1': item[0][27],
                            'alt_text_2': item[0][28],
                            'alt_text_3': item[0][29],
                            'alt_text_4': item[0][30],
                            # Custom Fields
                            'custom_botanical_name': item[0][31],
                            'custom_climate_zone': item[0][32],
                            'custom_plant_type': item[0][33],
                            'custom_type': item[0][34],
                            'custom_height': item[0][35],
                            'custom_width': item[0][36],
                            'custom_sun_exposure': item[0][37],
                            'custom_bloom_time': item[0][38],
                            'custom_bloom_color': item[0][39],
                            'custom_attracts_pollinators': item[0][40],
                            'custom_growth_rate': item[0][41],
                            'custom_deer_resistant': item[0][42],
                            'custom_soil_type': item[0][43],
                            'custom_color': item[0][44],
                            'custom_size': item[0][45],
                            'lst_maint_dt': item[0][46],
                            'cost': item[0][47],
                            'categories': str(item[0][49]).split(',') if item[0][49] else [],
                            'custom_url': '',
                            'is_custom_url': False,
                            'custom_field_ids': item[0][51]
                        }
                        # for x in details:
                        #     print(f"{x}: {details[x]}")
                        return details

                def validate_product(self):
                    print(f"Validating product {self.sku}")
                    # Test for missing variant name
                    if self.variant_name == "":
                        print(f"Product {self.sku} is missing a variant name. Validation failed.")
                        return False
                    # Test for missing price 1
                    if self.price_1 == 0:
                        print(f"Product {self.item_no} is missing a price 1. Validation failed.")
                        return False

                    return True

                def get_last_maintained_dates(self, dates):
                    """Get last maintained dates for product"""
                    for x in dates:
                        if x is not None:
                            if x > self.lst_maint_dt:
                                self.lst_maint_dt = x

                def get_local_product_images(self):
                    """Get local image information for product"""
                    product_images = []
                    photo_path = creds.photo_path
                    list_of_files = os.listdir(photo_path)
                    if list_of_files is not None:
                        for x in list_of_files:
                            if x.split(".")[0].split("^")[0] == self.sku:
                                product_images.append(x)
                    total_images = len(product_images)
                    if total_images > 0:
                        # print(f"Found {total_images} product images for item: {self.sku}")
                        for image in product_images:
                            img = Integrator.Catalog.Product.Image(image_name=image,
                                                                   last_run_time=self.last_run_date)
                            if img.last_modified_dt > self.last_run_date and img.validate():
                                self.images.append(img)

                def get_bc_product_images(self):
                    """Get BigCommerce image information for product's images"""
                    url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                           f'catalog/products/{self.product_id}/images')
                    response = requests.get(url=url, headers=creds.test_bc_api_headers)
                    if response is not None:
                        for x in response.json():
                            # Could use this to back-fill database with image id and sort order info
                            pass

                def construct_image_payload(self):
                    result = []
                    for image in self.images:
                        result.append({
                            "image_file": image.name,
                            "is_thumbnail": image.is_thumbnail,
                            "sort_order": image.sort_order,
                            "description": image.alt_text_1,
                            "image_url": f"{creds.public_web_dav_photos}/{image.name}",
                            "id": 0,
                            "product_id": self.product_id,
                            "date_modified": image.modified_date
                        })
                    return result

                def construct_custom_fields(self):
                    result = []

                    if self.custom_botanical_name:
                        result.append({
                            "id": 1,
                            "name": "Botanical Name",
                            "value": self.custom_botanical_name
                        })
                    if self.custom_climate_zone:
                        result.append({
                            "id": 2,
                            "name": "Climate Zone",
                            "value": self.custom_climate_zone
                        })
                    if self.custom_plant_type:
                        result.append({
                            "id": 3,
                            "name": "Plant Type",
                            "value": self.custom_plant_type
                        })
                    if self.custom_type:
                        result.append({
                            "id": 4,
                            "name": "Type",
                            "value": self.custom_type
                        })
                    if self.custom_height:
                        result.append({
                            "id": 5,
                            "name": "Height",
                            "value": self.custom_height
                        })
                    if self.custom_width:
                        result.append({
                            "id": 6,
                            "name": "Width",
                            "value": self.custom_width
                        })
                    if self.custom_sun_exposure:
                        result.append({
                            "id": 7,
                            "name": "Sun Exposure",
                            "value": self.custom_sun_exposure
                        })
                    if self.custom_bloom_time:
                        result.append({
                            "id": 8,
                            "name": "Bloom Time",
                            "value": self.custom_bloom_time
                        })
                    if self.custom_bloom_color:
                        result.append({
                            "id": 9,
                            "name": "Bloom Color",
                            "value": self.custom_bloom_color
                        })
                    if self.custom_attracts_pollinators:
                        result.append({
                            "id": 10,
                            "name": "Attracts Pollinators",
                            "value": self.custom_attracts_pollinators
                        })
                    if self.custom_growth_rate:
                        result.append({
                            "id": 11,
                            "name": "Growth Rate",
                            "value": self.custom_growth_rate
                        })
                    if self.custom_deer_resistant:
                        result.append({
                            "id": 12,
                            "name": "Deer Resistant",
                            "value": self.custom_deer_resistant
                        })
                    if self.custom_soil_type:
                        result.append({
                            "id": 13,
                            "name": "Soil Type",
                            "value": self.custom_soil_type
                        })
                    if self.custom_color:
                        result.append({
                            "id": 14,
                            "name": "Color",
                            "value": self.custom_color
                        })
                    if self.custom_size:
                        result.append({
                            "id": 15,
                            "name": "Size",
                            "value": self.custom_size
                        })
                    return result

                def construct_product_payload(self):
                    payload = {
                        "name": self.web_title,
                        "type": "physical",
                        "sku": self.sku,
                        "description": self.html_description,
                        "weight": self.weight,
                        "width": self.width,
                        "depth": self.depth,
                        "height": self.height,
                        "price": self.price_1,
                        "cost_price": self.cost,
                        "retail_price": self.price_1,
                        "sale_price": self.price_2,
                        "tax_class_id": 255,
                        "product_tax_code": "string",
                        "categories": [
                            0
                        ],
                        "brand_id": 1000000000,
                        "brand_name": self.brand,
                        "inventory_level": 2147483647,
                        "inventory_warning_level": 2147483647,
                        "inventory_tracking": "none",
                        "fixed_cost_shipping_price": 0.1,
                        "is_free_shipping": False,
                        "is_visible": self.web_visible,
                        "is_featured": self.featured,
                        "search_keywords": self.search_keywords,
                        "availability": "available" if not self.in_store_only else "disabled",
                        "gift_wrapping_options_type": "any",
                        "gift_wrapping_options_list": [
                            0
                        ],
                        "condition": "New",
                        "is_condition_shown": True,
                        "page_title": self.meta_title,
                        "meta_description": self.meta_description,
                        "preorder_release_date": "2019-08-24T14:15:22Z",
                        "preorder_message": self.preorder_message,
                        "is_preorder_only": False,
                        "is_price_hidden": False,
                        "price_hidden_label": "string",
                        # "custom_url": {
                        #   "url": "string",
                        #   "is_customized": True,
                        #   "create_redirect": True
                        # },

                        "date_last_imported": "string",

                        "custom_fields": self.construct_custom_fields(),

                        "bulk_pricing_rules": [
                            {
                                "quantity_min": 10,
                                "quantity_max": 50,
                                "type": "price",
                                "amount": 10
                            }
                        ],
                        "images": self.construct_image_payload(),
                        "videos": [
                            {
                                "title": "Writing Great Documentation",
                                "description": "A video about documentation",
                                "sort_order": 1,
                                "type": "youtube",
                                "video_id": "z3fRu9pkuXE",
                                "id": 0,
                                "product_id": 0,
                                "length": "string"
                            }
                        ],
                        "variants": self.construct_variant_payload(),
                    }
                    return payload

                @staticmethod
                def get_lst_maint_dt(file_path):
                    return datetime.fromtimestamp(os.path.getmtime(file_path)) if os.path.exists(
                        file_path) else datetime(
                        1970, 1, 1)

            class Image:

                def __init__(self, image_name: str, last_run_time, sku="", image_url="", product_id=0, variant_id=0,
                             image_id=0, is_thumbnail=False, sort_order=0, is_binding_image=False, is_binding_id=None,
                             is_variant_image=False, description=""):

                    self.last_run_time = last_run_time
                    self.id = None
                    self.image_name = image_name  # This is the file name
                    self.sku = sku
                    self.file_path = f"{creds.photo_path}/{self.image_name}"
                    self.image_url = image_url
                    self.product_id = product_id
                    self.variant_id = variant_id
                    self.image_id = image_id
                    self.is_thumbnail = is_thumbnail
                    self.image_number = 1
                    self.sort_order = sort_order
                    self.is_binding_image = is_binding_image
                    self.binding_id = is_binding_id
                    self.is_variant_image = is_variant_image
                    self.description = description
                    self.last_modified_dt = datetime.fromtimestamp(os.path.getmtime(self.file_path))
                    self.last_maintained_dt = None
                    self.get_image_details()

                def __str__(self):
                    result = ""
                    for k, v in self.__dict__.items():
                        result += f"{k}: {v}\n"
                    return result

                def get_image_details(self):
                    db = query_engine.QueryEngine()
                    query = f"SELECT * FROM SN_IMAGES WHERE IMAGE_NAME = '{self.image_name}'"
                    response = db.query_db(query)
                    if response is not None:
                        self.id = response[0][0]
                        self.image_name = response[0][1]
                        self.sku = response[0][2]
                        self.file_path = response[0][3]
                        self.image_url = response[0][4]
                        self.product_id = response[0][5]
                        self.image_id = response[0][6]
                        self.is_thumbnail = True if response[0][7] == 1 else False
                        self.image_number = response[0][8]
                        self.sort_order = response[0][9]
                        self.is_binding_image = True if response[0][10] == 1 else False
                        self.binding_id = response[0][11]
                        self.is_variant_image = True if response[0][12] == 1 else False
                        self.description = response[0][13] if response[0][13] else ""
                        self.last_modified_dt = response[0][14]
                        self.last_maintained_dt = response[0][15]
                    else:
                        self.image_url = self.upload_product_image()
                        self.set_image_details()

                def validate(self):
                    try:
                        file_size = os.path.getsize(self.file_path)
                    except FileNotFoundError:
                        print(f"File {self.file_path} not found.")
                        return False
                    # Check for valid file size/format
                    size = (1280, 1280)
                    q = 90
                    exif_orientation = 0x0112
                    if self.image_name.lower().endswith("jpg"):
                        # Resize files larger than 1.8 MB
                        if file_size > 1800000:
                            print(f"Found large file {self.image_name}. Attempting to resize.")
                            try:
                                im = Image.open(self.file_path)
                                im.thumbnail(size, Image.LANCZOS)
                                code = im.getexif().get(exif_orientation, 1)
                                if code and code != 1:
                                    im = ImageOps.exif_transpose(im)
                                im.save(self.file_path, 'JPEG', quality=q)
                                print(f"{self.image_name} resized.")
                            except Exception as e:
                                print(f"Error resizing {self.image_name}: {e}")
                                return False
                            else:
                                print(f"Image {self.image_name} was resized.")

                    # Remove Alpha Layer and Convert PNG to JPG
                    if self.image_name.lower().endswith("png"):
                        print(f"Found PNG file: {self.image_name}. Attempting to reformat.")
                        try:
                            im = Image.open(self.file_path)
                            im.thumbnail(size, Image.LANCZOS)
                            # Preserve Rotational Data
                            code = im.getexif().get(exif_orientation, 1)
                            if code and code != 1:
                                im = ImageOps.exif_transpose(im)
                            print(f"Stripping Alpha Layer.")
                            rgb_im = im.convert('RGB')
                            print(f"Saving new file in JPG format.")
                            new_file_path = f"{self.file_path[:-4]}.jpg"
                            rgb_im.save(new_file_path, 'JPEG', quality=q)
                            im.close()
                            print(f"Removing old PNG file")
                            os.remove(self.file_path)
                            self.file_path = new_file_path
                        except Exception as e:
                            print(f"Error converting {self.image_name}: {e}")
                            return False
                        else:
                            print("Conversion successful.")

                    # replace .JPEG with .JPG
                    if self.image_name.lower().endswith("jpeg"):
                        print(f"Found file ending with .JPEG. Attempting to reformat.")
                        try:
                            print("Start")
                            print(self.file_path)
                            im = Image.open(self.file_path)
                            im.thumbnail(size, Image.LANCZOS)
                            # Preserve Rotational Data
                            code = im.getexif().get(exif_orientation, 1)
                            if code and code != 1:
                                im = ImageOps.exif_transpose(im)
                            print(f"Saving new file in JPG format.")
                            new_image_name = self.image_name.split(".")[0] + ".jpg"
                            new_file_path = f"{creds.photo_path}/{new_image_name}"
                            im.save(new_file_path, 'JPEG', quality=q)
                            im.close()
                            print(f"Removing old JPEG file")
                            os.remove(self.file_path)
                            self.file_path = new_file_path
                            self.image_name = new_image_name
                            print("End", self.file_path)
                        except Exception as e:
                            print(f"Error converting {self.image_name}: {e}")
                            return False
                        else:
                            print("Conversion successful.")
                    if len(self.description) >= 500:
                        print(f"Description for {self.image_name} is too long. Validation failed.")
                        return False

                    # Check for images with words or trailing numbers in the name
                    if "^" in self.image_name and not self.image_name.split(".")[0].split("^")[1].isdigit():
                        print(f"Image {self.image_name} is not valid.")
                        return False

                    # Valid Image
                    # print(f"Image {self.image_name} is valid.")
                    return True

                def set_image_details(self):
                    def get_item_no_from_image_name(image_name):
                        def get_binding_id(item_no):
                            query = f"""
                                   SELECT USR_PROF_ALPHA_16 FROM IM_ITEM
                                   WHERE ITEM_NO = '{item_no}'
                                   """
                            response = query_engine.QueryEngine().query_db(query)
                            if response is not None:
                                return response[0][0] if response[0][0] else ''

                        # Check for binding image
                        if image_name.split(".")[0].split("^")[0] in Integrator.Catalog.all_binding_ids:
                            item_no = ""
                            binding_id = image_name.split(".")[0].split("^")[0]
                            self.is_binding_image = True
                        else:
                            item_no = image_name.split(".")[0].split("^")[0]
                            binding_id = get_binding_id(item_no)

                        return item_no, binding_id

                    def get_image_number():
                        image_number = 1
                        if "^" in self.image_name and self.image_name.split(".")[0].split("^")[1].isdigit():
                            # secondary images
                            for x in range(1, 100):
                                if int(self.image_name.split(".")[0].split("^")[1]) == x:
                                    image_number = x + 1
                                    break
                        return image_number

                    self.sku, self.binding_id = get_item_no_from_image_name(self.image_name)
                    self.image_number = get_image_number()

                    # Image Description Only non-binding images have descriptions at this time. Though,
                    # this could be handled with JSON reference in the future for binding images.
                    self.description = self.get_image_description()

                def get_image_description(self):
                    # currently there are only 4 counterpoint fields for descriptions.
                    if self.image_number < 5:
                        query = f"""
                               SELECT {str(f'USR_PROF_ALPHA_{self.image_number + 21}')} FROM IM_ITEM
                               WHERE ITEM_NO = '{self.sku}'
                               """
                        response = query_engine.QueryEngine().query_db(query)

                        if response is not None:
                            if response[0][0]:
                                return response[0][0]
                            else:
                                return ""
                        else:
                            return ""
                    else:
                        # If image number is greater than 4, it  will not have a description
                        return ""

                def upload_product_image(self) -> str:
                    """Upload file to import folder on webDAV server and turn public url"""
                    data = open(self.file_path, 'rb')
                    new_name = self.image_name.replace("^", "-")
                    url = f"{creds.web_dav_product_photos}/{new_name}"
                    try:
                        requests.put(url, data=data, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw))
                    except Exception as e:
                        print(f"Error uploading image: {e}")
                    else:
                        # return public url of image
                        return f"{creds.public_web_dav_photos}/{new_name}"

                def resize_image(self):
                    size = (1280, 1280)
                    q = 90
                    exif_orientation = 0x0112
                    if self.image_name.endswith("jpg"):
                        im = Image.open(self.file_path)
                        im.thumbnail(size, Image.LANCZOS)
                        code = im.getexif().get(exif_orientation, 1)
                        if code and code != 1:
                            im = ImageOps.exif_transpose(im)
                        im.save(self.file_path, 'JPEG', quality=q)
                        print(f"Resized {self.image_name}")

                def bc_get_image(self):
                    url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/'
                           f'products/{self.product_id}/images/{self.image_id}')
                    response = requests.get(url=url, headers=creds.test_bc_api_headers)
                    return response.content

                def bc_delete_image(self):
                    """Photos can either be variant images or product images. Two flows in this function"""
                    if self.is_variant_image:
                        url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/'
                               f'products/{self.product_id}/variants/{self.variant_id}/images/{self.image_id}')
                        response = requests.delete(url=url, headers=creds.test_bc_api_headers)

                    else:
                        url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/'
                               f'products/{self.product_id}/images/{self.image_id}')
                        response = requests.delete(url=url, headers=creds.test_bc_api_headers)

                    return response.content

                def sql_post_photo(self):
                    pass

                def sql_update_photo(self):
                    pass

                def sql_delete_photo(self):
                    query = f"DELETE FROM {creds.bc_image_table} WHERE IMAGE_NAME = '{self.image_name}'"
                    query_engine.QueryEngine().query_db(query, commit=True)
                    print(f"Photo {self.image_name} deleted from database.")

            class Video:
                """Placeholder for video class"""
                pass

            class Modifier:
                """Placeholder for modifier class"""
                pass

    class Customers:
        def __init__(self, last_sync):
            self.last_sync = last_sync
            self.db = Integrator.db
            self.customers = self.get_customers()

        def get_customers(self):
            query = f"""
            SELECT FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, ADRS_1, CITY, STATE, ZIP_COD, CNTRY
            FROM AR_CUST
            WHERE LST_MAINT_DT > '{self.last_sync}'
            """
            response = self.db.query_db(query)
            if response is not None:
                result = []
                for x in response:
                    if x is not None:
                        result.append(self.Customer(x))
                return result

        def sync(self):
            for customer in self.customers:
                customer.process()
                

        class Customer:
            def __init__(self, cust_result):
                self.cust_no = cust_result[0]
                self.db = Integrator.db
                self.fst_nam = cust_result[1]
                self.lst_nam = cust_result[2]
                self.email = cust_result[3] if cust_result[3] else f"{self.cust_no}@store.com"
                self.phone = cust_result[4]
                self.loyalty_points = cust_result[5]
                self.address = cust_result[6]
                self.city = cust_result[7]
                self.state = cust_result[8]
                self.zip = cust_result[9]
                self.country = cust_result[10]
            
            def hasPhone(self):
                return self.phone is not None
            
            def hasAddress(self):
                return self.address is not None and self.city is not None and self.country is not None

            def sync(self):
                class CSync:
                    def __init__(self):
                        self.db = Integrator.db
                    
                    def create(self, bc_cust_id: int):
                        query = f"""
                        INSERT INTO {creds.bc_customer_table}
                        (CUST_NO, BC_CUST_ID)
                        VALUES ('{self.cust_no}', {bc_cust_id})
                        """

                        self.db.query_db(query, commit=True)

                    def update(self, bc_cust_id: int):
                        query = f"""
                        UPDATE {creds.bc_customer_table}
                        SET BC_CUST_ID = {bc_cust_id}
                        WHERE CUST_NO = '{self.cust_no}'
                        """

                        self.db.query_db(query, commit=True)

                    def delete(self):
                        query = f"""
                        DELETE FROM {creds.bc_customer_table}
                        WHERE CUST_NO = '{self.cust_no}'
                        """

                        self.db.query_db(query, commit=True)

                return CSync()

            def process(self):
                def write_customer_payload(bc_cust_id: int = None):
                    payload = {}
                    if bc_cust_id is not None:
                        payload["id"] = bc_cust_id
                    
                    payload["first_name"] = self.fst_nam
                    payload["last_name"] = self.lst_nam
                    payload["email"] = self.email
                    payload["store_credit_amounts"] = [{"amount": self.loyalty_points}]

                    if self.hasPhone():
                        payload["phone"] = self.phone
                    
                    if self.hasAddress():
                        address = {
                            "first_name": self.fst_nam,
                            "last_name": self.lst_nam,
                            "address1": self.address,
                            "city": self.city,
                            "country_code": country_to_country_code(self.country if self.country is not None else "United States")
                        }

                        if self.state is not None:
                            address["state"] = self.state
                        
                        if self.zip is not None:
                            address["zip"] = self.zip

                        payload["addresses"] = [address]
                    
                    return payload

                def create():
                    print(f"Creating customer {self.cust_no}")
                    url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/customers"
                    payload = write_customer_payload()

                    response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                
                    if response.status_code == 200:
                        print(f"Customer {self.cust_no} created successfully.")
                        self.sync().create(response.json()["data"]["id"])
                    else:
                        print(f"Error creating customer {self.cust_no}.")

                def get_bc_id():
                    query = f"""
                    SELECT BC_CUST_ID FROM {creds.bc_customer_table}
                    WHERE CUST_NO = '{self.cust_no}'
                    """
                    response = self.db.query_db(query)
                    if response is not None:
                        return response[0][0]
                    else:
                        return None

                def update():
                    id = get_bc_id()
                    if id is None:
                        print(f"Customer {self.cust_no} not found in database.")
                    else:
                        print(f"Updating customer {self.cust_no}")
                        url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/customers"
                        payload = write_customer_payload(bc_cust_id=id)

                        response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                    
                        if response.status_code == 200:
                            print(f"Customer {self.cust_no} updated successfully.")
                            self.sync().update(id)
                        else:
                            print(f"Error updating customer {self.cust_no}.")

                def delete():
                    id = get_bc_id()
                    if id is None:
                        print(f"Customer {self.cust_no} not found in database.")
                    else:
                        print(f"Deleting customer {self.cust_no}")
                        url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/customers?id:in={id}"
                        response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                    
                        if response.status_code == 204:
                            print(f"Customer {self.cust_no} deleted successfully.")
                            self.sync().delete()
                        else:
                            print(f"Error deleting customer {self.cust_no}.")

                def get_processing_method():
                    del_query = f"""
                    SELECT CUST_NO FROM AR_CUST
                    WHERE CUST_NO = '{self.cust_no}'
                    """

                    response = self.db.query_db(del_query)
                    if response is None or len(response) == 0:
                        return "delete"

                    query = f"""
                    SELECT BC_CUST_ID FROM {creds.bc_customer_table}
                    WHERE CUST_NO = '{self.cust_no}'
                    """
                    response = self.db.query_db(query)
                    if response is not None:
                        return "update"
                    else:
                        return "create"
                    
                if get_processing_method() == "create":
                    create()
                elif get_processing_method() == "update":
                    update()
                elif get_processing_method() == "delete":
                    delete()

    class Orders:
        pass


def country_to_country_code(country):
    country_codes = {
        "United States": "US",
        "Canada": "CA",
        "Mexico": "MX",
        "United Kingdom": "GB"
    }

    return country_codes[country] if country in country_codes else country

def initialize_integration():
    """Clear Tables and Initialize Integration from Business Start Date"""
    integrator = Integrator(date_presets.business_start_date)
    database = integrator.Database()

    database.rebuild_tables()
    integrator.initialize()

    print(f"Building Categories on BigCommerce: Start Time: {datetime.now()}\n")
    integrator.category_tree.sync()
    print(f"Building Categories on BigCommerce End Time: {datetime.now()}")


def run_integration(last_sync):
    integrator = Integrator(last_sync)
    integrator.initialize()
    integrator.category_tree.sync()


# run_integration(date_presets.twenty_four_hours_ago)
# initialize_integration()

# database = Integrator.Database()
# database.rebuild_tables()
#
# tree = Integrator.Catalog.CategoryTree(last_sync=date_presets.business_start_date)
# tree.sync()
#
# brands = Integrator.Catalog.Brands(last_sync=date_presets.business_start_date)


# photo = Integrator.Catalog.Product.Image("202896.jpg", last_run_time=datetime(2021, 1, 1))

catalog = Integrator.Catalog(last_sync=date_presets.business_start_date)

catalog.sync()

# flag = Integrator.Catalog.Product("201213", last_sync=date_presets.business_start_date)
# print(flag.process())
# brands = Integrator.Catalog.Brands(last_sync=date_presets.five_minutes_ago)

# catalog = Integrator.Catalog(last_sync=date_presets.twenty_four_hours_ago)
# print(catalog)
# catalog.sync()
