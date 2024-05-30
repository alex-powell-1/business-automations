import os
import re
from datetime import datetime

import requests
from PIL import Image, ImageOps

from setup import creds
from setup import query_engine
from setup import date_presets
from requests.auth import HTTPDigestAuth

"""
BigCommerce Middleware Integration
Author: Alex Powell
"""


class Integrator:
    def __init__(self, last_run_date):
        self.last_sync = last_run_date
        self.log_file = open("test.txt", "a")
        self.category_tree = None
        self.brands = None
        self.catalog = None

    def __str__(self):
        return f"Integration Object\n" \
               f"Last Sync: {self.last_sync}\n" \
               f"{self.catalog}\n" \
               f"{self.category_tree}\n"

    def initialize(self):
        self.category_tree = self.Catalog.CategoryTree(last_sync=self.last_sync)
        self.brands = self.Catalog.Brands(last_sync=self.last_sync)
        # self.catalog = self.Catalog(last_run=last_run_date)

    def sync(self):
        self.catalog.sync()
        self.category_tree.build_bc_category_tree()

    class Database:
        def __init__(self):
            self.db = query_engine.QueryEngine()

        def rebuild_tables(self):
            def drop_table(table_name):
                self.db.query_db(f"DROP TABLE {table_name}", commit=True)

            # Drop Tables
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
                IMAGE_URL nvarchar(255),
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
                ITEM_NO varchar(50) NOT NULL PRIMARY KEY,
                BINDING_ID varchar(10),
                IS_PARENT BIT,
                PRODUCT_ID int NOT NULL,
                VARIANT_ID int,
                CATEG_ID int NOT NULL FOREIGN KEY REFERENCES {creds.bc_category_table}(CATEG_ID),
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
                    VARIANT_ID int,
                    IMAGE_ID int,
                    THUMBNAIL BIT DEFAULT(0),
                    IMAGE_NUMBER int DEFAULT(1),
                    SORT_ORDER int,
                    IS_BINDING_IMAGE BIT NOT NULL,
                    BINDING_ID varchar(50),
                    IS_VARIANT_IMAGE BIT DEFAULT(0),
                    DESCR nvarchar(100),
                    LST_MOD_DT datetime NOT NULL DEFAULT(current_timestamp),
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

    class Catalog:
        def __init__(self, last_run):
            # self.log_file = log_file
            self.products = []
            self.binding_ids = []
            self.single_products = []
            # Will update SN.PROD and SN.IMAGES tables LST_MAINT_DT for associated images
            self.update_image_timestamps(last_run=last_run)
            # Will get all products that have been updated since the last sync date
            self.get_products(last_sync_date=last_run)

        def __str__(self):
            return f"Products ({len(self.products)}): {self.products}\n" \
                   f"Binding IDs ({len(self.binding_ids)}): {self.binding_ids}\n" \
                   f"Single Products ({len(self.single_products)}): {self.single_products}\n"

        def get_products(self, last_sync_date):
            # Get all products that have been updated since the last sync date
            def get_updated_products(dt):
                """Get a list of all products that have been updated since the last run date.
                Will check IM_ITEM, IM_PRC, IM_INV,
                EC_ITEM_DESCR, EC_CATEG_ITEM, and Image tables for updates."""
                result = set()
                db = query_engine.QueryEngine()
                query = f"""
                SELECT ITEM.ITEM_NO
                FROM IM_ITEM ITEM
                LEFT OUTER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
                LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
                LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
                LEFT OUTER JOIN EC_CATEG_ITEM ON ITEM.ITEM_NO=EC_CATEG_ITEM.ITEM_NO
                LEFT OUTER JOIN {creds.bc_image_table} ON ITEM.ITEM_NO={creds.bc_image_table}.ITEM_NO
                WHERE ITEM.LST_MAINT_DT > '{dt}' or PRC.LST_MAINT_DT > '{dt}' 
                or INV.LST_MAINT_DT > '{dt}' or EC_ITEM_DESCR.LST_MAINT_DT > '{dt}'
                or EC_CATEG_ITEM.LST_MAINT_DT > '{dt}' or {creds.bc_image_table}.LST_MAINT_DT > '{dt}'
                """
                response = db.query_db(query)
                if response is not None:
                    for x in response:
                        result.add(x[0])
                return list(result)

            def get_binding_ids(product_list):
                binding_ids = set()
                for product in product_list:
                    binding_id = self.get_binding_id_from_sku(product)
                    if binding_id:
                        binding_ids.add(binding_id)
                return list(binding_ids)

            self.products = get_updated_products(last_sync_date)
            self.binding_ids = get_binding_ids(self.products)
            self.single_products = self.products.copy()

        def sync(self):
            def sync_bound_products(binding_ids):
                """Takes in a list of binding ids and creates BoundProduct objects for each one. If the product
                passes input validation, it will compare the product/image/category details against those in the
                middleware database and update."""
                for x in binding_ids:
                    bound_product = self.Product(x)
                    if bound_product.validate_product():
                        bound_product.process()
                    else:
                        print(f"Product {x} failed validation.")

                    # Subtract SKUs from single_product List
                    for variant in bound_product.variants:
                        self.single_products.remove(variant.sku)

            def sync_single_products(single_products):
                """Takes in a list of product SKUs and creates Product objects for each one. If the product passes input
                validation, it will compare the product/image/category details against those in the middleware database
                and update. Returns a list of all products that have been successfully updated."""
                while len(single_products) > 0:
                    product = self.Product(single_products.pop())
                    if product.validate_product():
                        product.process()

            if len(self.binding_ids) > 0:
                print(f"Syncing Bound Products: {len(self.binding_ids)} products")
                sync_bound_products(self.binding_ids)

            if len(self.single_products) > 0:
                print(f"Syncing Single Products: {len(self.binding_ids)} products")
                sync_single_products(self.single_products)

            # self.log_file.close()

        @staticmethod
        def update_image_timestamps(last_run):
            """Takes in a list of SKUs and updates the last maintenance date in input table for each product in the 
            list"""

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

            sku_list, file_list = get_updated_photos(last_run)

            db = query_engine.QueryEngine()
            query = (f"UPDATE {creds.bc_product_table} "
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
            return [element for element in counterpoint_list if element not in middleware_list]

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
                self.brands: set = set()
                self.get_cp_brands()

            def __str__(self):
                def print_brand_tree(brand, level=0):
                    # Print the brand id and name, indented by the brand's level in the tree
                    res = (f"{'    ' * level}Brand Name: {brand.name}\n"
                           f"{'    ' * level}---------------------------------------\n"
                           f"{'    ' * level}Counterpoint Brand ID: {brand.cp_brand_id}\n"
                           f"{'    ' * level}BigCommerce Brand ID: {brand.bc_brand_id}\n"
                           f"{'    ' * level}Page Title: {brand.page_title}\n"
                           f"{'    ' * level}Meta Keywords: {brand.meta_keywords}\n"
                           f"{'    ' * level}Meta Description: {brand.meta_description}\n"
                           f"{'    ' * level}Search Keywords: {brand.search_keywords}\n"
                           f"{'    ' * level}Image URL: {brand.image_url}\n"
                           f"{'    ' * level}Custom URL: {brand.custom_url}\n"
                           f"{'    ' * level}Last Maintenance Date: {brand.lst_maint_dt}\n\n")
                    # Recursively call this function for each child brand
                    for child in brand.children:
                        res += print_brand_tree(child, level + 1)
                    return res

                # Use the helper function to print the entire tree
                result = ""
                for root in self.heads:
                    result += print_brand_tree(root)

                return result

            def get_cp_brands(self):
                query = f"""
                SELECT PROF_COD, DESCR, LST_MAINT_DT
                FROM IM_ITEM_PROF_COD WHERE LST_MAINT_DT > '{self.last_sync}'
                """
                response = self.db.query_db(query)
                if response:
                    for x in response:
                        self.brands.add(self.Brand(cp_brand_id=x[0],
                                                   description=x[1],
                                                   lst_maint_dt=x[2]))

            class Brand:
                def __init__(self, cp_brand_id, description, lst_maint_dt):
                    self.db = query_engine.QueryEngine()
                    self.cp_brand_id = cp_brand_id
                    self.bc_brand_id = None
                    self.name = description
                    self.page_title = description
                    self.meta_keywords = ""
                    self.meta_description = ""
                    self.search_keywords = ""
                    self.image_url = ""
                    self.is_custom_url = True
                    self.custom_url = "-".join(str(self.name).split(" "))
                    # timestamps
                    self.lst_maint_dt = lst_maint_dt
                    # setter
                    self.get_brand_details()

                def get_brand_details(self):
                    query = (f"SELECT * "
                             f"FROM {creds.bc_brands_table} "
                             f"WHERE CP_BRAND_ID = '{self.cp_brand_id}'")

                    response = self.db.query_db(query)
                    print("HERE")
                    if response is not None:
                        for x in response:
                            self.bc_brand_id = x[2]
                            self.name = x[3]
                            self.page_title = x[4]
                            self.meta_keywords = x[5]
                            self.meta_description = x[6]
                            self.search_keywords = x[7]
                            self.image_url = x[8]
                            self.is_custom_url = True if x[9] == 1 else False
                            self.custom_url = x[9]
                    # Brand Not Found, Create New Brand
                    else:
                        print("No matching brand found.")
                        self.create()

                def create(self):
                    def create_bc_brand():
                        def construct_payload():
                            return {
                                "name": self.name,
                                "page_title": self.page_title,
                                "meta_keywords": self.meta_keywords.split(",") if self.meta_keywords else [],
                                "meta_description": self.meta_description,
                                "search_keywords": self.search_keywords if self.search_keywords else "",
                                "image_url": self.image_url,
                                # Causing Error: construct separately and add to payload
                                # "custom_url": {
                                #     "url": self.custom_url,
                                #     "is_customized": self.is_custom_url
                                # }
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

                    def write_to_middleware():
                        query = f"""
                        INSERT INTO {creds.bc_brands_table} (CP_BRAND_ID, BC_BRAND_ID, NAME, PAGE_TITLE, META_KEYWORDS, 
                        META_DESCR, SEARCH_KEYWORDS, IMAGE_URL, IS_CUSTOMIZED, CUSTOM_URL, LST_MAINT_DT)
                        VALUES ('{self.cp_brand_id}', {self.bc_brand_id}, '{self.name}', '{self.page_title}', 
                        {f"'{self.meta_keywords}'" if self.meta_keywords else "NULL"}, 
                        {f"'{self.meta_description}'" if self.meta_description else "NULL"}, 
                        {f"'{self.search_keywords}'" if self.search_keywords else "NULL"}, 
                        {f"'{self.image_url}'" if self.image_url else "NULL"}, {1 if self.is_custom_url else 0}, 
                        {f"'{self.custom_url}'" if self.custom_url else "NULL"}, GETDATE())
                        """
                        try:
                            response = self.db.query_db(query, commit=True)
                            print(response)
                        except Exception as e:
                            print(f"MIDDLEWARE: Brand {self.name} INSERT: SUCCESS.\n")
                            print(e)
                        else:
                            print(f"MIDDLEWARE: Brand {self.name} INSERT: SUCCESS.\n")

                    write_to_middleware()
        class Product:
            def __init__(self, item_no: str, last_run_date):

                print("INITIALIZING PRODUCT CLASS FOR ITEM: ", item_no)
                print()
                self.db = query_engine.QueryEngine()
                self.item_no = item_no
                self.last_run_date = last_run_date

                # Determine if Bound
                if self.item_no in self.get_all_binding_ids():
                    self.binding_id = self.item_no
                    self.is_bound = True
                else:
                    self.binding_id = None
                    self.is_bound = False

                # For Bound Items
                self.total_variants: int = 0
                # self.variants will be list of variant products
                self.variants: list = []
                # self.parent will be a list of parent products. If length of list > 1, product validation will fail
                self.parent: list = []

                # A list of image objects
                self.images: list = []

                # Product Information
                self.product_id: int = 0
                self.web_title: str = ""
                self.default_price = 0
                self.cost = 0
                self.sale_price = 0
                self.weight = 0
                self.width = 0
                self.height = 0
                self.depth = 0
                self.is_price_hidden = False
                self.brand: str = ""
                self.html_description: str = ""
                self.search_keywords: str = ""
                self.meta_title: str = ""
                self.meta_description: str = ""
                self.visible: bool = False
                self.featured: bool = False
                self.gift_wrap: bool = False
                self.search_keywords: str = ""
                self.availability: str = "available"
                self.is_preorder = False
                self.preorder_release_date = datetime(1970, 1, 1)
                self.preorder_message: str = ""
                self.meta_title: str = ""
                self.meta_description: str = ""
                self.alt_text_1: str = ""
                self.alt_text_2: str = ""
                self.alt_text_3: str = ""
                self.alt_text_4: str = ""

                # Custom Fields
                self.custom_botanical_name: str = ""
                self.custom_climate_zone: str = ""
                self.custom_plant_type: str = ""
                self.custom_type: str = ""
                self.custom_height: str = ""
                self.custom_width: str = ""
                self.custom_sun_exposure: str = ""
                self.custom_bloom_time: str = ""
                self.custom_bloom_color: str = ""
                self.custom_attracts_pollinators: str = ""
                self.custom_attracts_butterflies: str = ""
                self.custom_growth_rate: str = ""
                self.custom_deer_resistant: str = ""
                self.custom_soil_type: str = ""
                self.custom_color: str = ""
                self.custom_size: str = ""

                # E-Commerce Categories
                self.ecommerce_categories = []

                # Property Getter
                self.get_product_details()

                # Validate Product
                self.validation_retries = 10
                self.validate_product()

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
                        result += line
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
                            self.variants.append(self.Variant(item[0], self.last_run_date))

                    # Set parent
                    self.parent = [x for x in self.variants if x.is_parent]

                    # Set total children
                    self.total_variants = len(self.variants)

                    # Inherit Product Information from Parent Item
                    for x in self.variants:
                        if x.is_parent:
                            self.product_id = x.product_id
                            self.web_title = x.web_title
                            self.default_price = x.price_1
                            self.cost = x.cost
                            self.sale_price = x.price_2
                            self.brand = x.brand
                            self.html_description = x.html_description
                            self.search_keywords = x.search_keywords
                            self.meta_title = x.meta_title
                            self.meta_description = x.meta_description
                            self.visible = x.web_visible
                            self.featured = x.featured
                            self.gift_wrap = x.gift_wrap
                            self.custom_botanical_name = x.custom_botanical_name
                            self.custom_climate_zone = x.custom_climate_zone
                            self.custom_plant_type = x.custom_plant_type
                            self.custom_type = x.custom_type
                            self.custom_height = x.custom_height
                            self.custom_width = x.custom_width
                            self.custom_sun_exposure = x.custom_sun_exposure
                            self.custom_bloom_time = x.custom_bloom_time
                            self.custom_bloom_color = x.custom_bloom_color
                            self.custom_attracts_pollinators = x.custom_attracts_pollinators
                            self.custom_growth_rate = x.custom_growth_rate
                            self.custom_deer_resistant = x.custom_deer_resistant
                            self.custom_soil_type = x.custom_soil_type
                            self.custom_color = x.custom_color
                            self.custom_size = x.custom_size
                            self.ecommerce_categories = x.ecommerce_categories

                    def get_binding_id_images():
                        photo_path = creds.photo_path
                        list_of_files = os.listdir(photo_path)
                        if list_of_files is not None:
                            for x in list_of_files:
                                if x.split(".")[0].split("^")[0] == self.binding_id:
                                    self.images.append(self.Image(x, last_run_time=self.last_run_date))

                    # Product Images
                    get_binding_id_images()

                    # Variant Images
                    for x in self.variants:
                        for y in x.images:
                            self.images.append(y)

                def get_single_product_details():
                    self.variants.append(self.Variant(self.item_no, self.last_run_date))
                    if len(self.variants) == 1:
                        for x in self.variants[0]:
                            self.product_id = x.product_id
                            self.web_title = x.web_title
                            self.default_price = x.price_1
                            self.cost = x.cost
                            self.sale_price = x.price_2
                            self.brand = x.brand
                            self.html_description = x.html_description
                            self.search_keywords = x.search_keywords
                            self.meta_title = x.meta_title
                            self.meta_description = x.meta_description
                            self.visible = x.web_visible
                            self.featured = x.featured
                            self.gift_wrap = x.gift_wrap
                            self.custom_botanical_name = x.custom_botanical_name
                            self.custom_climate_zone = x.custom_climate_zone
                            self.custom_plant_type = x.custom_plant_type
                            self.custom_type = x.custom_type
                            self.custom_height = x.custom_height
                            self.custom_width = x.custom_width
                            self.custom_sun_exposure = x.custom_sun_exposure
                            self.custom_bloom_time = x.custom_bloom_time
                            self.custom_bloom_color = x.custom_bloom_color
                            self.custom_attracts_pollinators = x.custom_attracts_pollinators
                            self.custom_growth_rate = x.custom_growth_rate
                            self.custom_deer_resistant = x.custom_deer_resistant
                            self.custom_soil_type = x.custom_soil_type
                            self.custom_color = x.custom_color
                            self.custom_size = x.custom_size
                            self.ecommerce_categories = x.ecommerce_categories

                if self.is_bound:
                    get_bound_product_details()
                else:
                    get_single_product_details()

            def validate_product(self):
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
                    print("Reestablishing products.")
                    return self.construct_bound_product()

                # Bound Product Validation
                if self.is_bound:
                    while self.validation_retries > 0:
                        print(f"Validating {self.binding_id}")

                        # Test for missing binding ID. Potentially add corrective action
                        # (i.e. generate binding ID or remove product
                        # and rebuild as a new single product)
                        if self.binding_id == "":
                            print(f"Product {self.binding_id} has no binding ID. Validation failed.")

                            return False

                        # Test for valid Binding ID Schema (ex. B0001)
                        pattern = r'B\d{4}'
                        if not bool(re.fullmatch(pattern, self.binding_id)):
                            print(f"Product {self.binding_id} has an invalid binding ID. Validation failed.")
                            return False

                        # Test for parent product problems
                        if len(self.parent) != 2:
                            # Test for missing parent
                            if len(self.parent) == 0:
                                print(f"Product {self.binding_id} has no parent. Will reestablish parent.")
                                set_parent()
                                self.validation_retries -= 1
                                return self.validate_product()

                            # Test for multiple parents
                            if len(self.parent) > 1:
                                print(f"Product {self.binding_id} has multiple parents. Will reestablish parent.")
                                self.remove_parent()
                                set_parent()
                                self.validation_retries -= 1
                                return self.validate_product()

                        # Test for missing web title
                        if self.web_title == "":
                            print(f"Product {self.binding_id} is missing a web title. Validation failed.")
                            return False

                        # Test for missing html description
                        if self.html_description == "":
                            print(f"Product {self.binding_id} is missing an html description. Validation failed.")
                            return False

                        # Test for missing product images
                        if len(self.images) == 0:
                            print(f"Product {self.binding_id} is missing images. Will turn visibility to off.")
                            self.visible = False

                        # Test for missing E-Commerce Categories
                        if len(self.ecommerce_categories) == 0:
                            print(f"Product {self.binding_id} is missing E-Commerce Categories. Validation failed.")
                            return False

                        # Test for missing variant names
                        for child in self.variants:
                            if child.variant_name == "":
                                print(f"Product {child.sku} is missing a variant name. Validation failed.")
                                return False
                        print(f"Product {self.binding_id} is valid.")
                        print("\n")
                        return True
                else:
                    # Single Product Validation
                    print(f"Validating product {self.item_no}")

                    # Test for missing web title
                    if self.web_title == "":
                        print(f"Product {self.item_no} is missing a web title. Validation failed.")
                        return False

                    # Test for missing html description
                    if self.html_description == "":
                        print(f"Product {self.item_no} is missing an html description. Validation failed.")
                        return False

                    # Test for missing E-Commerce Categories
                    if len(self.ecommerce_categories) == 0:
                        print(f"Product {self.item_no} is missing E-Commerce Categories. Validation failed.")
                        return False

                    # Test for missing brand
                    if self.brand == "":
                        print(f"Product {self.item_no} is missing a brand. Validation failed.")
                        return False

                    # Test for missing cost
                    if self.cost == 0:
                        print(f"Product {self.item_no} is missing a cost. Validation failed.")
                        return False

                    # Test for missing price 1
                    if self.default_price == 0:
                        print(f"Product {self.item_no} is missing a price 1. Validation failed.")
                        return False

                    # Test for missing weight
                    if self.weight == 0:
                        print(f"Product {self.item_no} is missing a weight. Validation failed.")
                        return False

                    # Validation has Passed.
                    return True

            def process(self):
                def get_processing_method() -> str:
                    # Check for delete needed
                    pass

                def construct_product_payload():

                    def get_ecomm_categories():
                        """Take the list of e-commerce categories and return a list of BC category IDs"""
                        result = []
                        for category in self.ecommerce_categories:
                            query = f"""
                            SELECT BC_CATEG_ID
                            FROM SN_CATEG
                            WHERE CP_CATEG_ID = {category}
                            """
                            response = self.db.query_db(query)
                            if response is not None:
                                result.append(response[0][0])

                        return result

                    def construct_custom_fields():
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

                    def construct_image_payload(mode: str = "create"):
                        result = []
                        # Child Images
                        for child in self.variants:
                            for image in child.images:
                                if mode == "create":
                                    result.append({
                                        "product_id": child.product_id,
                                        "image_file": image.name,
                                        "is_thumbnail": image.is_thumbnail,
                                        "sort_order": image.sort_order,
                                        "description": image.description,
                                        "image_url": image.image_url,
                                    })
                                elif mode == "update":
                                    result.append({
                                        "product_id": child.product_id,
                                        "image_file": image.name,
                                        "is_thumbnail": image.is_thumbnail,
                                        "sort_order": image.sort_order,
                                        "description": image.description,
                                        "image_url": image.image_url,
                                        "id": image.id,
                                    })
                        return result

                    def construct_variant_payload():
                        result = []
                        id_index = 1
                        for child in self.variants:
                            result.append({
                                "cost_price": child.cost,
                                "price": child.price_1,
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
                                "id": id_index,
                                "sku": "string",
                                "option_values": [
                                    {
                                        "option_display_name": "Color",
                                        "label": "Beige"
                                    }
                                ],
                                "calculated_price": 0.1,
                                "calculated_weight": 0
                            })
                        return result

                    payload = {
                        "name": self.web_title,
                        "type": "physical",
                        "sku": self.binding_id,
                        "description": self.html_description,
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
                        "categories": get_ecomm_categories(),
                        "brand_id": get_brand_id(),
                        "brand_name": self.brand,
                        "inventory_level": sum(x.buffered_quantity for x in self.variants),
                        "inventory_warning_level": 10,
                        "inventory_tracking": "variant" if self.is_bound else "product",
                        # "fixed_cost_shipping_price": 0.1,
                        "is_free_shipping": False,
                        "is_visible": self.visible,
                        "is_featured": self.featured,
                        "search_keywords": self.search_keywords,
                        "availability": self.availability,
                        "gift_wrapping_options_type": "any",
                        "gift_wrapping_options_list": [
                            0
                        ],
                        "condition": "New",
                        "is_condition_shown": True,
                        "page_title": self.meta_title,
                        "meta_description": self.meta_description,
                        "preorder_release_date": self.preorder_release_date,
                        "preorder_message": self.preorder_message,
                        "is_preorder_only": self.is_preorder,
                        "is_price_hidden": self.is_price_hidden,
                        "price_hidden_label": "string",
                        # "custom_url": {
                        #   "url": "string",
                        #   "is_customized": True,
                        #   "create_redirect": True
                        # },

                        "date_last_imported": "string",

                        "custom_fields": construct_custom_fields(),

                        "bulk_pricing_rules": [
                            {
                                "quantity_min": 10,
                                "quantity_max": 50,
                                "type": "price",
                                "amount": 10
                            }
                        ],
                        "images": construct_image_payload(),
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
                        "variants": construct_variant_payload(),
                    }

                    return payload

                def create():
                    def bc_create_product():
                        """Create product in BigCommerce. For this implementation, this is a single product with no
                        variants"""
                        url = f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products'
                        payload = construct_product_payload()
                        response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                        if response.status_code in [200, 207]:
                            print(f"BigCommerce POST {self.item_no}: SUCCESS. Code: {response.status_code}")
                        else:
                            print(f"BigCommerce POST {self.item_no}: FAILED. Code: {response.status_code}")
                            print(response.content)
                        return response.status_code

                    def middleware_create_product():
                        """NOT DONE"""
                        query = f"""
                        INSERT INTO {creds.bc_product_table}
                        (ITEM_NO, WEB_TITLE, HTML_DESCRIPTION)
                        VALUES ('{self.item_no}', '{self.web_title}', '{self.html_description}'),
                        """
                        try:
                            query_engine.QueryEngine().query_db(query, commit=True)
                        except Exception as e:
                            print(f"Middleware INSERT product {self.item_no}: FAILED")
                            print(e)
                        else:
                            print(f"Middleware INSERT product {self.item_no}: SUCCESS")

                    if bc_create_product() in [200, 207]:
                        middleware_create_product()

                def update():
                    def bc_update_product():
                        url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                               f'catalog/products/{self.product_id}')
                        payload = construct_product_payload()
                        response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                        if response.status_code == 200:
                            print(f"Product {self.item_no} updated successfully.")
                        else:
                            print(f"Error updating product {self.item_no}.")
                            print(response.content)
                        return response.status_code

                    def middleware_update_product():
                        query = f"""
                        UPDATE {creds.bc_product_table}
                        SET WEB_TITLE = '{self.web_title}', 
                        HTML_DESCRIPTION = '{self.html_description}'
                        WHERE ITEM_NO = '{self.item_no}'
                        """
                        try:
                            query_engine.QueryEngine().query_db(query, commit=True)
                        except Exception as e:
                            print(f"Middleware UPDATE product {self.item_no}: FAILED")
                            print(e)
                        else:
                            print(f"Middleware UPDATE product {self.item_no}: SUCCESS")

                    if bc_update_product() == 200:
                        middleware_update_product()

                def delete():
                    def bc_delete_product():
                        # Delete product VARIANT from BigCommerce
                        if self.is_bound and not self.is_parent:
                            # This needs to be in variant class
                            url = (
                                f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                f'catalog/products/{self.product_id}'
                                f'/variants/{self.variant_id}')
                        else:
                            # This will delete single products and bound parent products
                            url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                   f'catalog/products/{self.product_id}')
                        try:
                            response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                        except Exception as e:
                            print(f"Error deleting product {self.item_no}: {e}")
                        else:
                            if response.status_code == 204:
                                print(f"Product {self.item_no} deleted successfully.")
                                return response.json()

                    def middleware_delete_product():
                        # First delete product images
                        for image in self.images:
                            image.delete()
                        # Delete product from product table
                        query = f"DELETE FROM {creds.bc_product_table} WHERE ITEM_NO = '{self.item_no}'"
                        query_engine.QueryEngine().query_db(query, commit=True)

                    bc_delete_product()
                    middleware_delete_product()

                method = get_processing_method()

                if method == "delete":
                    delete()
                else:
                    # Validate Product Inputs
                    if self.validate_product():
                        if method == "create":
                            create()
                        elif method == "update":
                            update()
                    else:
                        print(f"Product {self.item_no} failed validation.")

            def create_bound_product(self):
                self.bc_create_product()
                self.middleware_create_product()

            def bc_create_product(self):
                payload = self.construct_product_payload()
                url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products"
                response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                if response.status_code == 201:
                    print(f"Product {self.binding_id} created successfully.")
                else:
                    print(f"Error creating product {self.binding_id}.")
                    print(response.json())

            def middleware_create_product(self):
                pass

            def update_bound_product(self):
                self.bc_update_product()
                self.middleware_update_product()

            def bc_update_product(self):
                payload = self.construct_product_payload()
                url = (f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                       f"catalog/products/{self.product_id}")
                response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                if response.status_code == 200:
                    print(f"Product {self.binding_id} updated successfully.")
                else:
                    print(f"Error updating product {self.binding_id}.")
                    print(response.json())

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
                def __init__(self, sku: str, last_run_date):
                    print("INITIALIZING VARIANT CLASS FOR ITEM: ", sku)
                    print()
                    self.last_run_date = last_run_date
                    # Product ID Info
                    self.item_no: str = sku
                    self.binding_id: str = ""
                    self.product_id: int = 0
                    self.variant_id: int = 0

                    # Status
                    self.web_enabled: bool = False
                    self.web_visible: bool = False
                    self.purchasing_disabled = False
                    self.purchasing_disabled_message = ""
                    self.is_free_shipping = False
                    self.always_online: bool = False
                    self.gift_wrap: bool = False
                    self.brand_cp_cod = ""
                    self.brand: str = ""
                    self.featured: bool = False
                    self.in_store_only: bool = False
                    self.sort_order: int = 0
                    self.is_bound = False
                    self.is_parent: bool = False
                    self.web_title: str = ""
                    self.variant_name: str = ""
                    self.status: str = ""

                    # Product Pricing
                    self.reg_price: float = 0
                    self.price_1: float = 0
                    self.price_2: float = 0
                    self.cost: float = 0

                    # Inventory Levels
                    self.quantity_available: int = 0
                    self.buffer: int = 0
                    self.buffered_quantity: int = 0

                    # Product Details
                    self.item_type = ""
                    self.weight = 0
                    self.width = 0
                    self.height = 0
                    self.depth = 0
                    self.parent_category = ""
                    self.sub_category = ""
                    self.description: str = ""
                    self.long_description: str = ""
                    self.search_keywords: str = ""
                    self.preorder_message: str = ""
                    self.meta_title: str = ""
                    self.meta_description: str = ""
                    self.html_description: str = ""

                    # Photo Alt Text
                    self.alt_text_1: str = ""
                    self.alt_text_2: str = ""
                    self.alt_text_3: str = ""
                    self.alt_text_4: str = ""

                    # Custom Fields
                    self.custom_botanical_name: str = ""
                    self.custom_climate_zone: str = ""
                    self.custom_plant_type: str = ""
                    self.custom_type: str = ""
                    self.custom_height: str = ""
                    self.custom_width: str = ""
                    self.custom_sun_exposure: str = ""
                    self.custom_bloom_time: str = ""
                    self.custom_bloom_color: str = ""
                    self.custom_attracts_pollinators: str = ""
                    self.custom_growth_rate: str = ""
                    self.custom_deer_resistant: str = ""
                    self.custom_soil_type: str = ""
                    self.custom_color: str = ""
                    self.custom_size: str = ""
                    # Product Images
                    self.images = []
                    # Dates
                    self.lst_maint_dt = datetime(1970, 1, 1)
                    # E-Commerce Categories
                    self.ecommerce_categories = []
                    # Product Schema (i.e. Bound, Single, Variant.)
                    self.item_schema = ""
                    # Processing Method
                    self.processing_method = ""
                    # Initialize Product Details
                    self.get_product_details()

                def __str__(self):
                    result = ""
                    for k, v in self.__dict__.items():
                        result += f"{k}: {v}\n"
                    return result

                def get_product_details(self):
                    db = query_engine.QueryEngine()

                    query = f""" select ITEM.USR_PROF_ALPHA_16, ITEM.IS_ECOMM_ITEM, ITEM.IS_ADM_TKT, 
                    ITEM.USR_CPC_IS_ENABLED, ITEM.USR_ALWAYS_ONLINE, ITEM.IS_FOOD_STMP_ITEM, ITEM.PROF_COD_1, 
                    ITEM.ECOMM_NEW, ITEM.USR_IN_STORE_ONLY, ITEM.USR_PROF_ALPHA_27, ITEM.ADDL_DESCR_1, 
                    ITEM.USR_PROF_ALPHA_17, ITEM.REG_PRC, ITEM.PRC_1, PRC.PRC_2, ISNULL(INV.QTY_AVAIL, 0), 
                    ISNULL(ITEM.PROF_NO_1, 0), ITEM.ITEM_TYP,ITEM.CATEG_COD, ITEM.SUBCAT_COD, ITEM.DESCR, 
                    ITEM.LONG_DESCR, ITEM.USR_PROF_ALPHA_26, ITEM.USR_PROF_ALPHA_19, ITEM.ADDL_DESCR_2, 
                    USR_PROF_ALPHA_21, EC_ITEM_DESCR.HTML_DESCR, ITEM.STAT, USR_PROF_ALPHA_22, USR_PROF_ALPHA_23, 
                    USR_PROF_ALPHA_24, USR_PROF_ALPHA_25, PROF_ALPHA_1, PROF_ALPHA_2, PROF_ALPHA_3, PROF_ALPHA_4, 
                    PROF_ALPHA_5, USR_PROF_ALPHA_6, USR_PROF_ALPHA_7, USR_PROF_ALPHA_8, USR_PROF_ALPHA_9, 
                    USR_PROF_ALPHA_10, USR_PROF_ALPHA_11, USR_PROF_ALPHA_12, USR_PROF_ALPHA_13, USR_PROF_ALPHA_14, 
                    USR_PROF_ALPHA_15, ITEM.LST_MAINT_DT, INV.LST_MAINT_DT, PRC.LST_MAINT_DT,
                    EC_ITEM_DESCR.LST_MAINT_DT, EC_CATEG_ITEM.LST_MAINT_DT, EC_CATEG_ITEM.CATEG_ID, ITEM.LST_COST, 
                    COD.DESCR

                   FROM IM_ITEM ITEM

                   INNER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO

                   LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
                   LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
                   LEFT OUTER JOIN EC_CATEG_ITEM ON ITEM.ITEM_NO=EC_CATEG_ITEM.ITEM_NO
                   LEFT OUTER JOIN EC_CATEG ON EC_CATEG.CATEG_ID=EC_CATEG_ITEM.CATEG_ID
                   INNER JOIN IM_ITEM_PROF_COD COD ON ITEM.PROF_COD_1 = COD.PROF_COD


                   WHERE ITEM.ITEM_NO = '{self.item_no}' and ITEM.IS_ECOMM_ITEM = 'Y'
                   """
                    response = db.query_db(query)
                    if response is not None:
                        self.binding_id: str = response[0][0] if response[0][0] else ""
                        self.is_bound: bool = True if self.binding_id != "" else False

                        # self.product_id: int = 0 This could be in separate table or would need to add columns to
                        # IM_ITEM self.variant_id: int = 0 This could be in separate table or would need to add
                        # columns to IM_ITEM

                        # Status
                        self.web_enabled: bool = True if response[0][1] == 'Y' else False
                        self.is_parent: bool = True if response[0][2] == 'Y' else False
                        self.web_visible: bool = True if response[0][3] == 'Y' else False
                        self.always_online: bool = True if response[0][4] == 'Y' else False
                        self.gift_wrap: bool = True if response[0][5] == 'Y' else False
                        self.brand_cp_cod: str = response[0][6] if response[0][6] else ""
                        self.brand: str = response[0][54] if response[0][54] else ""
                        self.featured: bool = True if response[0][7] == 'Y' else False
                        self.in_store_only: bool = True if response[0][8] == 'Y' else False
                        self.sort_order: int = int(response[0][9]) if response[0][9] else 0
                        self.web_title: str = response[0][10] if response[0][10] else ""
                        self.variant_name: str = response[0][11] if response[0][11] else ""
                        self.status: str = response[0][27] if response[0][27] else ""

                        # Product Pricing
                        self.reg_price: float = response[0][12] if response[0][12] else 0
                        self.price_1: float = response[0][13] if response[0][13] else 0
                        self.price_2: float = response[0][14] if response[0][14] else 0

                        # Inventory Levels
                        self.quantity_available: int = int(response[0][15]) if response[0][15] else 0
                        self.buffer: int = int(response[0][16]) if response[0][16] else 0
                        self.buffered_quantity: int = self.quantity_available - self.buffer

                        # Product Details
                        self.item_type: str = response[0][17] if response[0][17] else ""
                        self.parent_category = response[0][18] if response[0][18] else ""
                        self.sub_category = response[0][19] if response[0][19] else ""
                        self.description: str = response[0][20] if response[0][20] else ""
                        self.long_description: str = response[0][21] if response[0][21] else ""
                        self.search_keywords: str = response[0][22] if response[0][22] else ""
                        self.preorder_message: str = response[0][23] if response[0][23] else ""
                        self.meta_title: str = response[0][24] if response[0][24] else ""
                        self.meta_description: str = response[0][25] if response[0][25] else ""
                        self.html_description: str = response[0][26] if response[0][26] else ""
                        self.alt_text_1: str = response[0][28] if response[0][28] else ""
                        self.alt_text_2: str = response[0][29] if response[0][29] else ""
                        self.alt_text_3: str = response[0][30] if response[0][30] else ""
                        self.alt_text_4: str = response[0][31] if response[0][31] else ""

                        # Custom Fields
                        self.custom_botanical_name: str = response[0][32] if response[0][32] else ""
                        self.custom_climate_zone: str = response[0][33] if response[0][33] else ""
                        self.custom_plant_type: str = response[0][34] if response[0][34] else ""
                        self.custom_type: str = response[0][35] if response[0][35] else ""
                        self.custom_height: str = response[0][36] if response[0][36] else ""
                        self.custom_width: str = response[0][37] if response[0][37] else ""
                        self.custom_sun_exposure: str = response[0][38] if response[0][38] else ""
                        self.custom_bloom_time: str = response[0][39] if response[0][39] else ""
                        self.custom_bloom_color: str = response[0][40] if response[0][40] else ""
                        self.custom_attracts_pollinators: str = response[0][41] if response[0][41] else ""
                        self.custom_growth_rate: str = response[0][42] if response[0][42] else ""
                        self.custom_deer_resistant: str = response[0][43] if response[0][43] else ""
                        self.custom_soil_type: str = response[0][44] if response[0][44] else ""
                        self.custom_color: str = response[0][45] if response[0][45] else ""
                        self.custom_size: str = response[0][46] if response[0][46] else ""
                        # Dates
                        self.get_last_maintained_dates(response[0][47:52])
                        # E-Commerce Categories
                        for x in response:
                            if x is not None:
                                self.ecommerce_categories.append(x[52])
                        # Last Cost
                        self.cost = response[0][53] if response[0][53] else 0

                        # Product Images
                        self.get_local_product_images()

                def validate_product(self):
                    print(f"Validating product {self.item_no}")
                    # Test for missing variant name
                    if self.variant_name == "":
                        print(f"Product {self.item_no} is missing a variant name. Validation failed.")
                        return False
                    # Test for missing price 1
                    if self.price_1 == 0:
                        print(f"Product {self.item_no} is missing a price 1. Validation failed.")
                        return False

                    return True

                def process(self):
                    def get_processing_method() -> str:
                        # Check for delete needed
                        pass

                    def create():
                        def bc_create_variant():
                            """Create product in BigCommerce. For this implementation, this is a single product with
                            no variants"""
                            url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                   f'catalog/products/{self.product_id}/variants')
                            payload = self.construct_variant_payload()
                            response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                            if response.status_code in [200, 207]:
                                print(f"BigCommerce POST {self.item_no}: SUCCESS. Code: {response.status_code}")
                            else:
                                print(f"BigCommerce POST {self.item_no}: FAILED. Code: {response.status_code}")
                                print(response.content)

                            return response.status_code

                        def middleware_create_variant():
                            """NOT DONE"""
                            query = f"""
                                   INSERT INTO {creds.bc_product_table}
                                   (ITEM_NO, BINDING_ID, IS_PARENT, PRODUCT_ID, VARIANT_ID, LST_MAINT_DT
                                   VALUES ('{self.item_no}', '{self.binding_id}', '{self.is_parent}', 
                                   '{self.product_id}', '{self.variant_id}'), '{self.lst_maint_dt}',
                                   """
                            try:
                                query_engine.QueryEngine().query_db(query, commit=True)
                            except Exception as e:
                                print(f"Middleware INSERT product {self.item_no}: FAILED")
                                print(e)
                            else:
                                print(f"Middleware INSERT product {self.item_no}: SUCCESS")

                        if create() in [200, 207]:
                            middleware_create_variant()

                    def update():
                        def bc_update_variant():
                            url = (f'https://api.bigcommerce.com/stores/{store_hash}/v3/'
                                   f'catalog/products/{product_id}/variants/{variant_id}')
                            payload = self.construct_product_payload()
                            response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
                            if response.status_code == 200:
                                print(f"Product {self.item_no} updated successfully.")
                            else:
                                print(f"Error updating product {self.item_no}.")
                                print(response.content)
                            return response.status_code

                        def middleware_update_variant():
                            query = f"""
                                   UPDATE {creds.bc_product_table}
                                   SET WEB_TITLE = '{self.web_title}', DESCRIPTION = '{self.description}', 
                                   HTML_DESCRIPTION = '{self.html_description}'
                                   WHERE ITEM_NO = '{self.item_no}'
                                   """
                            try:
                                query_engine.QueryEngine().query_db(query, commit=True)
                            except Exception as e:
                                print(f"Middleware UPDATE product {self.item_no}: FAILED")
                                print(e)
                            else:
                                print(f"Middleware UPDATE product {self.item_no}: SUCCESS")

                        if bc_update_variant() == 200:
                            middleware_update_variant()

                    def delete():
                        def bc_delete_variant():
                            # Delete product VARIANT from BigCommerce
                            if self.is_bound and not self.is_parent:
                                url = (
                                    f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                    f'catalog/products/{self.product_id}'
                                    f'/variants/{self.variant_id}')
                            else:
                                # This will delete single products and bound parent products
                                url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/'
                                       f'catalog/products/{self.product_id}')
                            try:
                                response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                            except Exception as e:
                                print(f"Error deleting product {self.item_no}: {e}")
                            else:
                                if response.status_code == 204:
                                    print(f"Product {self.item_no} deleted successfully.")
                                    return response.json()

                        def middleware_delete_product():
                            # First delete product images
                            for image in self.images:
                                image.delete()
                            # Delete product from product table
                            query = f"DELETE FROM {creds.bc_product_table} WHERE ITEM_NO = '{self.item_no}'"
                            query_engine.QueryEngine().query_db(query, commit=True)

                        bc_delete_variant()
                        middleware_delete_variant()

                    method = get_processing_method()

                    if method == "delete":
                        delete()
                    elif method == "create":
                        create()
                    elif method == "update":
                        update()

                def construct_variant_payload(self):
                    variants = [{
                        "cost_price": self.cost,
                        "price": self.price_1,
                        "sale_price": self.price_2,
                        "retail_price": self.price_1,
                        "weight": self.weight,
                        "width": self.width,
                        "height": self.height,
                        "depth": self.depth,
                        "is_free_shipping": self.is_free_shipping,
                        # "fixed_cost_shipping_price": 0.1,
                        "purchasing_disabled": self.purchasing_disabled,
                        "purchasing_disabled_message": self.purchasing_disabled_message,
                        # "upc": self.upc,
                        "inventory_level": self.buffered_quantity,
                        # "inventory_warning_level": self.inventory_warning_level,
                        # "bin_picking_number": self.bin_picking_number,
                        "image_url": self.images[0].image_url,
                        # "gtin": self.gtin,
                        # "mpn": self.mpn,
                        "product_id": self.product_id,
                        "sku": self.item_no,
                        "option_values": [
                            {
                                "option_display_name": "Option",
                                "label": self.variant_name,
                            }
                        ]
                    }]
                    return variants

                def get_product_schema(self):
                    if self.is_bound and not self.is_parent:
                        self.item_schema = "variant"

                def get_last_maintained_dates(self, dates):
                    """Get last maintained dates for product"""
                    for x in dates:
                        if x is not None:
                            if x > self.lst_maint_dt:
                                self.lst_maint_dt = x

                def get_local_product_images(self):
                    """Get local image information for product"""
                    photo_path = creds.photo_path
                    list_of_files = os.listdir(photo_path)
                    if list_of_files is not None:
                        for x in list_of_files:
                            if x.split(".")[0].split("^")[0] == self.item_no:
                                self.images.append(
                                    Integrator.Catalog.Product.Image(x, last_run_time=self.last_run_date))

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
                        "sku": self.item_no,
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
                        "availability": "available",
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

                binding_id_list = get_all_binding_ids()

                def __init__(self, image_name: str, last_run_time, item_no="", image_url="", product_id=0, variant_id=0,
                             image_id=0, is_thumbnail=False, sort_order=0, is_binding_image=False, is_binding_id=None,
                             is_variant_image=False, description=None):

                    print("INITIALIZING IMAGE CLASS FOR ITEM: ", image_name)
                    print()

                    self.last_run_time = last_run_time
                    self.id = None
                    self.image_name = image_name  # This is the file name
                    self.item_no = item_no
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

                    if self.last_modified_dt >= self.last_run_time:
                        print(f"Image {self.image_name} has been updated since last run.")
                        # Image has been updated since last run. Check image for valid size and format.
                        if self.validate_image():
                            # Input image file is valid.
                            # Set image properties with this new information.
                            # Write local image information to database.
                            # BigCommerce uploading will be handled by the Product class.
                            self.initialize_image_details(binding_id_list=self.binding_id_list)
                    else:
                        # Set image details for existing photos with no update
                        self.get_image_details_from_db()

                def __str__(self):
                    result = ""
                    for k, v in self.__dict__.items():
                        result += f"{k}: {v}\n"
                    return result

                def validate_image(self):
                    print(f"Validating image {self.image_name}")
                    try:
                        file_size = os.path.getsize(self.file_path)
                    except FileNotFoundError:
                        print(f"File {self.file_path} not found.")
                        return False
                    else:
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
                                    return True

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
                                return True

                        # replace .JPEG with .JPG
                        if self.image_name.lower().endswith("jpeg"):
                            print(f"Found file ending with .JPEG. Attempting to reformat.")
                            try:
                                im = Image.open(self.file_path)
                                im.thumbnail(size, Image.LANCZOS)
                                # Preserve Rotational Data
                                code = im.getexif().get(exif_orientation, 1)
                                if code and code != 1:
                                    im = ImageOps.exif_transpose(im)
                                print(f"Saving new file in JPG format.")
                                new_file_path = f"{self.file_path[:-5]}.jpg"
                                im.save(new_file_path, 'JPEG', quality=q)
                                im.close()
                                print(f"Removing old JPEG file")
                                os.remove(self.file_path)
                                self.file_path = new_file_path
                            except Exception as e:
                                print(f"Error converting {self.image_name}: {e}")
                                return False
                            else:
                                print("Conversion successful.")
                                return True
                    print(f"Image {self.image_name} is valid.")
                    return True

                def get_image_details_from_db(self):
                    db = query_engine.QueryEngine()
                    query = f"SELECT * FROM SN_IMAGES WHERE IMAGE_NAME = '{self.image_name}'"
                    response = db.query_db(query)
                    if response is not None:
                        self.id = response[0][0]
                        self.image_name = response[0][1]
                        self.item_no = response[0][2]
                        self.file_path = response[0][3]
                        self.image_url = response[0][4]
                        self.product_id = response[0][5]
                        self.variant_id = response[0][6]
                        self.image_id = response[0][7]
                        self.is_thumbnail = True if response[0][8] == 1 else False
                        self.image_number = response[0][9]
                        self.sort_order = response[0][10]
                        self.is_binding_image = True if response[0][11] == 1 else False
                        self.binding_id = response[0][12]
                        self.is_variant_image = True if response[0][13] == 1 else False
                        self.description = response[0][14]
                        self.last_maintained_dt = response[0][16]

                def initialize_image_details(self, binding_id_list):
                    def get_item_no_from_image_name(image_name, binding_id_list):
                        def get_binding_id():
                            query = f"""
                                   SELECT USR_PROF_ALPHA_16 FROM IM_ITEM
                                   WHERE ITEM_NO = '{self.item_no}'
                                   """
                            response = query_engine.QueryEngine().query_db(query)
                            if response is not None:
                                return response[0][0] if response[0][0] else None

                        # Check for binding image
                        if image_name.split(".")[0].split("^")[0] in binding_id_list:
                            item_no = None
                            binding_id = image_name.split(".")[0].split("^")[0]
                        else:
                            item_no = image_name.split(".")[0].split("^")[0]
                            binding_id = get_binding_id()

                        return item_no, binding_id

                    def get_image_number():
                        image_number = 1
                        if "^" in self.image_name:
                            # secondary images
                            for x in range(1, 100):
                                if int(self.image_name.split(".")[0].split("^")[1]) == x:
                                    image_number = x + 1
                                    break
                        return image_number

                    self.item_no, self.binding_id = get_item_no_from_image_name(self.image_name, binding_id_list)
                    self.image_number = get_image_number()
                    self.image_url = self.upload_product_image()
                    self.is_binding_image = True if self.binding_id else False

                    # Image Description Only non-binding images have descriptions at this time. Though,
                    # this could be handled with JSON reference in the future for binding images.
                    if not self.is_binding_image:
                        self.description = self.get_image_description()

                    self.write_image_to_db()

                def write_image_to_db(self):
                    query = f""" INSERT INTO SN_IMAGES (IMAGE_NAME, ITEM_NO, FILE_PATH, IMAGE_URL, IMAGE_NUMBER,
                    IS_BINDING_IMAGE, BINDING_ID, DESCR, LST_MOD_DT)
                    
                    VALUES ('{self.image_name}', {f"{self.item_no}" if self.item_no else "NULL"}, '{self.file_path}', 
                    '{self.image_url}', {self.image_number}, {1 if self.is_binding_image else 0}, '{self.binding_id}',
                    {f"'{self.description}'" if self.description else "NULL"}, '{self.last_modified_dt:%Y-%m-%d %H:%M:%S}')
                           """
                    try:
                        query_engine.QueryEngine().query_db(query, commit=True)
                    except Exception as e:
                        print(f"Error writing image to db: {e}")
                    else:
                        print(f"Image {self.image_name} written to db.\n")

                def get_image_description(self):
                    # currently there are only 4 counterpoint fields for descriptions.
                    if self.image_number < 5:
                        query = f"""
                               SELECT USR_PROF_ALPHA_{self.image_number + 21} FROM IM_ITEM
                               WHERE ITEM_NO = '{self.item_no}'
                               """
                        response = query_engine.QueryEngine().query_db(query)
                        if response is not None:
                            return response[0][0]
                        else:
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

                def bc_post_image(self) -> int:
                    if self.is_variant_image:
                        url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/'
                               f'products/{self.product_id}/variants/{self.variant_id}/images')
                        payload = {
                            "product_id": self.product_id,
                            "variant_id": self.variant_id,
                            "is_thumbnail": self.is_thumbnail,
                            "sort_order": self.sort_order,
                            "description": self.description,
                            "image_url": self.image_url
                        }
                        print(f"Posting variant image {self.image_name} to item {self.item_no}.")
                        response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                        if response.status_code == 200:
                            return 0
                        elif response.status_code == 500:
                            print("Image too large, will resize and try again.")
                            self.resize_image()
                            self.bc_post_image()
                        else:
                            print(f"Error posting image: {response.content}")
                            return 0

                    else:
                        url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog'
                               f'products/{self.product_id}/images')
                        payload = {
                            "product_id": self.product_id,
                            "is_thumbnail": self.is_thumbnail,
                            "sort_order": self.sort_order,
                            "description": self.description,
                            "image_url": self.image_url
                        }
                        print(f"Posting image {self.image_name} to item {self.item_no}.")
                        response = requests.post(url=url, headers=creds.test_bc_api_headers, json=payload)
                        if response.status_code == 200:
                            return response.json()["data"]["id"]

                        elif response.status_code == 404:
                            print("Product not found.")
                            return 0
                        else:
                            print(f"Error posting image: {response.content}")
                            return 0

                def bc_update_product_image(self, source_url, description=""):
                    url = (f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/'
                           f'products/{self.product_id}/images/{self.image_id}')
                    payload = {
                        "product_id": self.product_id,
                        "is_thumbnail": self.is_thumbnail,
                        "sort_order": self.sort_order,
                        "description": description,
                        "image_url": source_url
                    }
                    response = requests.put(url=url, headers=creds.test_bc_api_headers, json=payload)
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

# tree = Integrator.CategoryTree(last_sync=date_presets.business_start_date)
# tree.sync()

# photo = Integrator.Catalog.Product.Image("202896.jpg", last_run_time=datetime(2021, 1, 1))

# product = Integrator.Catalog.Product("45", last_run_date=datetime(2020, 5, 30))
# print(product)


brands = Integrator.Catalog.Brands(last_sync=date_presets.business_start_date)
