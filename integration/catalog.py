import asyncio
import os
import re
from datetime import datetime
from setup import date_presets

import time
import aiohttp
import requests
from PIL import Image, ImageOps
from requests.auth import HTTPDigestAuth
import random

from integration.database import Database

from setup import creds
from setup import query_engine
from integration.utilities import pretty_print, get_all_binding_ids, timer


class Catalog:
    all_binding_ids = get_all_binding_ids()

    def __init__(self, last_sync):
        # self.log_file = log_file
        self.last_sync = last_sync
        self.db = Database.db
        self.process_item_deletes()
        self.process_image_deletes_and_adds()
        # lists of products with updated timestamps
        self.products = self.get_products()
        if self.products:
            self.binding_ids = set(x["binding_id"] for x in self.products)
        self.product_errors = []
        # Still need to get ALL list from mw and cp

    def __str__(self):
        return (
            f"Items to Process: {len(self.products)}\n"
            f"Binding IDs with Updates: {len(self.binding_ids)}\n"
        )

    def process_item_deletes(self):
        """Assesses CP and MW for products that have been deleted. Deletes from BC and MW if found."""

        cp_items = self.db.query_db(
            "SELECT ITEM_NO FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'"
        )

        all_cp_products = [x[0] for x in cp_items] if cp_items else []
        mw_items = self.db.query_db(f"SELECT ITEM_NO FROM {creds.bc_product_table}")
        all_mw_products = [x[0] for x in mw_items] if mw_items else []

        delete_count = 0
        delete_targets = Catalog.get_deletion_target(
            middleware_list=all_mw_products, counterpoint_list=all_cp_products
        )

        def delete_from_bigcommerce(target):
            item_query = f"SELECT PRODUCT_ID FROM {creds.bc_product_table} WHERE ITEM_NO = '{target}'"
            response = self.db.query_db(item_query)
            if response is not None:
                product_id = response[0][0]

            if product_id:
                delete_url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products?id={product_id}"

                response = requests.delete(
                    url=delete_url, headers=creds.test_bc_api_headers
                )
                if response.status_code == 204:
                    print(f"Product {target} deleted from BigCommerce.")
                    delete_query = f"DELETE FROM {creds.bc_product_table} WHERE PRODUCT_ID = '{product_id}'"
                    self.db.query_db(delete_query, commit=True)
                else:
                    print(f"Error deleting product {target} from BigCommerce.")
                    print(response.json())

        if delete_targets:
            print(f"Product Delete Targets: {delete_targets}")
            for x in delete_targets:
                print(f"Deleting Product {x}.")
                delete_from_bigcommerce(x)
                delete_count += 1

            print(f"Deleted {delete_count} products.")

    def process_image_deletes_and_adds(self):
        """Assesses Image folder. Deletes images from MW and BC. Updates LST_MAINT_DT in CP if new images have been added."""

        def get_local_images():
            """Get a tuple of two sets:
            1. all SKUs that have had their photo modified since the input date.
            2. all file names that have been modified since the input date."""

            all_files = []
            # Iterate over all files in the directory
            for filename in os.listdir(creds.photo_path):
                if filename not in ["Thumbs.db", "desktop.ini", ".DS_Store"]:
                    # filter out trailing filenames
                    if "^" in filename:
                        if filename.split(".")[0].split("^")[1].isdigit():
                            all_files.append(
                                [
                                    filename,
                                    os.path.getsize(f"{creds.photo_path}/{filename}"),
                                ]
                            )
                    else:
                        all_files.append(
                            [
                                filename,
                                os.path.getsize(f"{creds.photo_path}/{filename}"),
                            ]
                        )

            return all_files

        def get_middleware_images():
            query = f"SELECT IMAGE_NAME, SIZE FROM {creds.bc_image_table}"
            response = self.db.query_db(query)
            return [[x[0], x[1]] for x in response] if response else []

        def delete_image(image_name) -> bool:
            """Takes in an image name and looks for matching image file in middleware. If found, deletes from BC and SQL."""
            print("Entering Delete Image Function of Catalog Class")
            print(f"Deleting {image_name}")
            image_query = f"SELECT PRODUCT_ID, IMAGE_ID, IS_VARIANT_IMAGE FROM {creds.bc_image_table} WHERE IMAGE_NAME = '{image_name}'"
            img_id_res = self.db.query_db(image_query)
            if img_id_res is not None:
                print("Image ID Result: ", img_id_res)
                product_id, image_id, is_variant = (
                    img_id_res[0][0],
                    img_id_res[0][1],
                    img_id_res[0][2],
                )

            if is_variant == 1:
                # Get Variant ID
                item_number = image_name.split(".")[0].split("^")[0]
                variant_query = f"SELECT VARIANT_ID FROM {creds.bc_product_table} WHERE ITEM_NO = '{item_number}'"
                variant_id_res = self.db.query_db(variant_query)
                if variant_id_res is not None:
                    variant_id = variant_id_res[0][0]
                else:
                    print(
                        f"Variant ID not found for {image_name}. Response: {variant_id_res}"
                    )

                if variant_id is not None:
                    url = (
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/"
                        f"products/{product_id}/variants/{variant_id}/images/"
                    )
                    response = requests.post(
                        url=url,
                        headers=creds.test_bc_api_headers,
                        json={"image_url": ""},
                    )
                    if response.status_code == 200:
                        print(
                            f"Primary Variant Image {image_name} deleted from BigCommerce."
                        )
                    else:
                        print(
                            f"Error deleting Primary Variant Image {image_name} from BigCommerce. {response.json()}"
                        )
                        print(response.json())

            delete_img_url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products/{product_id}/images/{image_id}"

            img_del_res = requests.delete(
                url=delete_img_url, headers=creds.test_bc_api_headers, timeout=10
            )

            if img_del_res.status_code == 204:
                print(f"Image {image_name} deleted from BigCommerce.")
            else:
                print(f"Error deleting image {image_name} from BigCommerce.")
                print(img_del_res.json())

            print("Deleting from SQL")

            delete_images_query = (
                f"DELETE FROM {creds.bc_image_table} " f"WHERE IMAGE_ID = '{image_id}'"
            )
            response = self.db.query_db(delete_images_query, commit=True)
            print(f"Image {image_name} deleted from SQL.", response, "\n\n")

        local_images = get_local_images()
        mw_image_list = get_middleware_images()

        # print("Length of Image List: ", len(local_images))
        # print("Length of MW Image List: ", len(mw_image_list))

        delete_targets = Catalog.get_deletion_target(
            counterpoint_list=local_images, middleware_list=mw_image_list
        )

        if delete_targets:
            print("Delete Targets", delete_targets)
            for x in delete_targets:
                print(f"Deleting Image {x[0]}.\n")
                delete_image(x[0])
        else:
            print("No image deletions found.")

        update_list = delete_targets

        addition_targets = Catalog.get_deletion_target(
            counterpoint_list=mw_image_list, middleware_list=local_images
        )

        if addition_targets:
            for x in addition_targets:
                update_list.append(x)
        else:
            print("No image additions found.")

        if update_list:
            sku_list = [x[0].split(".")[0].split("^")[0] for x in update_list]
            binding_list = [x for x in sku_list if x in Catalog.all_binding_ids]

            sku_list = tuple(sku_list)
            if binding_list:
                if len(binding_list) > 1:
                    binding_list = tuple(binding_list)
                    where_filter = f" or USR_PROF_ALPHA_16 in {binding_list}"
                else:
                    where_filter = f" or USR_PROF_ALPHA_16 = '{binding_list[0]}'"
            else:
                where_filter = ""

            query = (
                "UPDATE IM_ITEM "
                "SET LST_MAINT_DT = GETDATE() "
                f"WHERE (ITEM_NO in {sku_list} {where_filter}) and IS_ECOMM_ITEM = 'Y'"
            )

            self.db.query_db(query, commit=True)
            print(
                f"Image Update: LST_MAINT_DT UPDATE sent for {len(sku_list)} products."
            )

    def get_products(self):
        # return [{"sku": "TREEMA20", "binding_id": ""}]
        db = query_engine.QueryEngine()
        query = f"""
        SELECT ITEM_NO, ISNULL(ITEM.USR_PROF_ALPHA_16, '') as 'Binding ID'
        FROM IM_ITEM ITEM
        WHERE ITEM.LST_MAINT_DT > '{self.last_sync: %Y-%m-%d %H:%M:%S}' and
        ITEM.IS_ECOMM_ITEM = 'Y'
        """
        response = db.query_db(query)
        print(f"Response: {response}")
        if response is not None:
            result = []
            for item in response:
                sku = item[0]
                binding_id = item[1]
                if binding_id != "":
                    # Get Parent to Process. This would be a great place to fix multiple
                    # parents...
                    query = f"""
                    SELECT ITEM_NO
                    FROM IM_ITEM
                    WHERE USR_PROF_ALPHA_16 = '{binding_id}' AND IS_ECOMM_ITEM = 'Y' AND IS_ADM_TKT = 'Y'"""
                    response = db.query_db(query)
                    if response is not None:
                        parent_sku = response[0][0]

                    if parent_sku:
                        result.append({"sku": parent_sku, "binding_id": binding_id})
                    else:
                        print(f"Parent SKU not found for {binding_id}.")
                        continue
                else:
                    result.append({"sku": sku, "binding_id": binding_id})

                print(f"Adding Product to Queue: {item[0]} Binding ID: {item[1]}")

            res = []
            [res.append(x) for x in result if x not in res]

            print(f"Result: {res}")
            return res

    def sync(self):
        if not self.products:
            print("No products to sync.")
        else:
            general_errors = []
            print(f"Syncing {len(self.products)} products.")
            while len(self.products) > 0:
                target = self.products.pop()
                print(
                    f"Starting Product: {target['sku']}, Binding: {target['binding_id']}"
                )
                start_time = time.time()
                prod = self.Product(target, last_sync=self.last_sync)
                print(
                    f"Processing Product: {prod.sku}, Binding: {prod.binding_id}, Title: {prod.web_title}"
                )
                if prod.validate_product_inputs():
                    prod.process()

                for error in prod.errors:
                    self.product_errors.append((prod.sku, error))

                # Remove ALL associated variants from the queue, failed or not.
                products_to_remove = [y.sku for y in prod.variants]
                for x in products_to_remove:
                    for y in self.products:
                        if y["sku"] == x:
                            print(f"Removing {y}")
                            self.products.remove(y)  # remove all variants from the list
                print(
                    f"Product {prod.sku} processed in {time.time() - start_time} seconds."
                )
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
                print("Product Errors:")
                for error in self.product_errors:
                    print(error)
                    print("\n\n")

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
        return [
            element for element in middleware_list if element not in counterpoint_list
        ]

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
                res = (
                    f"{'    ' * level}Category Name: {category.category_name}\n"
                    f"{'    ' * level}---------------------------------------\n"
                    f"{'    ' * level}Counterpoint Category ID: {category.cp_categ_id}\n"
                    f"{'    ' * level}Counterpoint Parent ID: {category.cp_parent_id}\n"
                    f"{'    ' * level}BigCommerce Category ID: {category.bc_categ_id}\n"
                    f"{'    ' * level}BigCommerce Parent ID: {category.bc_parent_id}\n"
                    f"{'    ' * level}Sort Order: {category.sort_order}\n"
                    f"{'    ' * level}Last Maintenance Date: {category.lst_maint_dt}\n\n"
                )
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
            query = """
            SELECT cp.CATEG_ID, ISNULL(cp.PARENT_ID, 0), cp.DESCR, cp.DISP_SEQ_NO, cp.HTML_DESCR, 
            cp.LST_MAINT_DT, sn.CP_CATEG_ID
            FROM EC_CATEG cp
            FULL OUTER JOIN SN_CATEG sn on cp.CATEG_ID=sn.CP_CATEG_ID
            """
            response = self.db.query_db(query)
            if response:
                for x in response:
                    cp_categ_id = x[0]
                    if cp_categ_id == "0":
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
                query = """
                SELECT CATEG_ID, ISNULL(PARENT_ID, 0), DESCR, DISP_SEQ_NO, HTML_DESCR, LST_MAINT_DT
                FROM EC_CATEG
                WHERE CATEG_ID != '0'
                """
                response = self.db.query_db(query)
                if response is not None:
                    for ec_cat in response:
                        category = self.Category(
                            cp_categ_id=ec_cat[0],
                            cp_parent_id=ec_cat[1],
                            category_name=ec_cat[2],
                            sort_order=ec_cat[3],
                            description=ec_cat[4],
                            lst_maint_dt=ec_cat[5],
                        )
                        self.categories.add(category)

            get_categories()

            for x in self.categories:
                for y in self.categories:
                    if y.cp_parent_id == x.cp_categ_id:
                        x.add_child(y)

            self.heads = [x for x in self.categories if x.cp_parent_id == "0"]

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
                    url = (
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}"
                        f"/v3/catalog/trees/categories?category_id:in={bc_category_id}"
                    )
                    response = requests.delete(
                        url=url, headers=creds.test_bc_api_headers
                    )
                    if 207 >= response.status_code >= 200:
                        print(
                            response.status_code
                        )  # figure what code they are actually returning
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
                        print(
                            f"Error deleting category {bc_category_id} from BigCommerce."
                        )
                        print(response.json())

        class Category:
            def __init__(
                self,
                cp_categ_id,
                cp_parent_id,
                category_name,
                bc_categ_id=None,
                bc_parent_id=None,
                sort_order=0,
                description="",
                lst_maint_dt=datetime(1970, 1, 1),
            ):
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
                return (
                    f"Category Name: {self.category_name}\n"
                    f"---------------------------------------\n"
                    f"Counterpoint Category ID: {self.cp_categ_id}\n"
                    f"Counterpoint Parent ID: {self.cp_parent_id}\n"
                    f"BigCommerce Category ID: {self.bc_categ_id}\n"
                    f"BigCommerce Parent ID: {self.bc_parent_id}\n"
                    f"Sort Order: {self.sort_order}\n"
                    f"Last Maintenance Date: {self.lst_maint_dt}\n\n"
                )

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
                payload = [
                    {
                        "name": self.category_name,
                        "url": {
                            "path": f"/{self.category_name}/",
                            "is_customized": False,
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
                        "default_product_sort": "use_store_settings",
                    }
                ]

                response = requests.post(
                    url=url, headers=creds.test_bc_api_headers, json=payload
                )
                if response.status_code == 201 or response.status_code == 207:
                    print(
                        f"BigCommerce: POST: {self.category_name}: SUCCESS Code: {response.status_code}"
                    )
                    category_id = response.json()["data"][0]["category_id"]
                    return category_id
                else:
                    print(
                        f"BigCommerce: POST: {self.category_name}: Failure Code: {response.status_code}"
                    )
                    print(response.json())

            def bc_update_category(self):
                url = f" https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/trees/categories"
                payload = [
                    {
                        "category_id": self.bc_categ_id,
                        "name": self.category_name,
                        "parent_id": self.bc_parent_id,
                        "tree_id": 1,
                        "page_title": self.category_name,
                        "is_visible": True,
                    }
                ]

                response = requests.put(
                    url=url, headers=creds.test_bc_api_headers, json=payload, timeout=10
                )
                if response.status_code == 200:
                    print(
                        f"BigCommerce: UPDATE: {self.category_name} Category: SUCCESS Code: {response.status_code}\n"
                    )
                else:
                    print(
                        f"BigCommerce: UPDATE: {self.category_name} "
                        f"Category: FAILED Status: {response.status_code}"
                        f"Payload: {payload}\n"
                        f"Response: {response.text}\n"
                    )

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
                url = (
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                    f"catalog/trees/categories/{self.bc_categ_id}"
                )
                response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                if response.status_code == 204:
                    print(f"Category {self.category_name} deleted from BigCommerce.")
                else:
                    print(
                        f"Error deleting category {self.category_name} from BigCommerce."
                    )
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
            self.db = Database.db
            self.last_sync = last_sync
            self.cp_brands = set()
            self.mw_brands = set()
            # self.brands will be a set of Brand objects only created if the last_maint_dt is > than last sync
            self.brands = set()
            # get all brands from CP and MW
            self.get_brands()

        def __str__(self):
            result = ""
            for brand in self.brands:
                result += (
                    f"{brand.name}\n"
                    f"---------------------------------------\n"
                    f"Last Modified: {brand.last_maint_dt}\n\n"
                )
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
                            modified_date = datetime.fromtimestamp(
                                os.path.getmtime(file_path)
                            )
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
                query = (
                    f"UPDATE {creds.bc_brands_table} "
                    f"SET IMAGE_LST_MAINT_DT = GETDATE(), LST_MAINT_DT = GETDATE() "
                    f"{mw_where_filter} "
                    f"UPDATE IM_ITEM_PROF_COD "
                    f"SET LST_MAINT_DT = GETDATE() "
                    f"{cp_where_filter} "
                )
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

        def sync(self):
            # trip lst_maint_dt for all brands whose photos have been updated
            self.update_brand_timestamps(last_run=self.last_sync)
            # process deletes
            self.process_deletes()
            # create Brand objects for each brand that has been updated
            self.construct_brands()

        def construct_brands(self):
            for cp_brand in self.cp_brands:
                # Filter out brands that are not new or updated
                if cp_brand[2] > self.last_sync:
                    brand = self.Brand(
                        cp_brand[0], cp_brand[1], cp_brand[2], self.last_sync
                    )
                    self.brands.add(brand)
                    print(f"Brand {brand.name} added to sync queue.")

        def get_brands(self):
            def get_cp_brands():
                query = """
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
            delete_targets = Catalog.get_deletion_target(
                middleware_list=mw_brand_ids, counterpoint_list=cp_brand_ids
            )

            def bc_delete(target):
                url = (
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                    f"catalog/brands/{target}"
                )
                response = requests.delete(url=url, headers=creds.test_bc_api_headers)
                if response.status_code == 204:
                    print(
                        f"BigCommerce: Brand {x} DELETE: SUCCESS. Code: {response.status_code}"
                    )
                elif response.status_code == 404:
                    print(f"BigCommerce: Brand {x} DELETE: Brand Not Found.")
                else:
                    print(
                        f"BigCommerce: Brand {x} DELETE: FAILED! Status Code: {response.status_code}"
                    )
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
                self.custom_url = "-".join(
                    str(re.sub("[^A-Za-z0-9 ]+", "", self.name)).split(" ")
                )
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
                        self.image_last_modified = (
                            x[11] if x[11] else self.get_image_last_modified()
                        )
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
                            "meta_keywords": self.meta_keywords.split(",")
                            if self.meta_keywords
                            else [],
                            "meta_description": self.meta_description,
                            "search_keywords": self.search_keywords
                            if self.search_keywords
                            else "",
                            "image_url": self.image_url,
                            "custom_url": {
                                "url": f"/{self.custom_url}/",
                                "is_customized": self.is_custom_url,
                            },
                        }

                    url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/brands"
                    payload = construct_payload()
                    response = requests.post(
                        url=url, headers=creds.test_bc_api_headers, json=payload
                    )
                    if response.status_code in [200, 207]:
                        print(
                            f"BigCommerce: Brand {self.name} POST: SUCCESS. Code: {response.status_code}"
                        )
                        return response.json()["data"]["id"]
                    else:
                        print(
                            f"BigCommerce: Brand {self.name} POST: FAILED! Status Code: {response.status_code}"
                        )
                        print(
                            response.json()
                        )  # figure out what they are actually returning

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
                    url = (
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3"
                        f"/catalog/brands/{self.bc_brand_id}"
                    )
                    payload = {
                        "name": self.name,
                        "page_title": self.page_title,
                        "meta_keywords": self.meta_keywords.split(",")
                        if self.meta_keywords
                        else [],
                        "meta_description": self.meta_description
                        if self.meta_description
                        else "",
                        "search_keywords": self.search_keywords
                        if self.search_keywords
                        else "",
                        "image_url": self.image_url,
                        "custom_url": {
                            "url": f"/{self.custom_url}/",
                            "is_customized": self.is_custom_url,
                        },
                    }
                    response = requests.put(
                        url=url, headers=creds.test_bc_api_headers, json=payload
                    )
                    if response.status_code in [200, 207]:
                        print(
                            f"BigCommerce: Brand {self.name} PUT: SUCCESS. Code: {response.status_code}"
                        )
                    else:
                        print(
                            f"BigCommerce: Brand {self.name} PUT: FAILED! Status Code: {response.status_code}"
                        )
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
                    data = open(self.image_filepath, "rb")
                except FileNotFoundError:
                    return ""

                self.image_name = f"{self.cp_brand_id}.jpg"

                url = f"{creds.web_dav_product_photos}/{self.image_name}"

                try:
                    requests.put(
                        url,
                        data=data,
                        auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw),
                    )
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

            self.sku = product_data["sku"]
            self.binding_id = product_data["binding_id"]

            # Will be set to True if product gets a success response from BigCommerce API on POST or PUT
            self.is_uploaded = False

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
            self.product_id = None
            self.web_title: str = ""
            self.long_descr = ""
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

            self.lst_maint_dt = datetime(1970, 1, 1)

            # E-Commerce Categories
            self.cp_ecommerce_categories = []
            self.bc_ecommerce_categories = []

            # Property Getter
            self.get_product_details(last_sync=self.last_sync)

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

        def get_product_details(self, last_sync):
            """Get product details from Counterpoint and Middleware"""

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
                        self.variants.append(
                            self.Variant(item[0], last_run_date=last_sync)
                        )

                # Set parent
                self.parent = [item for item in self.variants if item.is_parent]

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
                        self.custom_attracts_pollinators = (
                            bound.custom_attracts_pollinators
                        )
                        self.custom_growth_rate = bound.custom_growth_rate
                        self.custom_deer_resistant = bound.custom_deer_resistant
                        self.custom_soil_type = bound.custom_soil_type
                        self.custom_color = bound.custom_color
                        self.custom_size = bound.custom_size
                        self.cp_ecommerce_categories = bound.cp_ecommerce_categories
                        self.custom_url = bound.custom_url
                        self.is_custom_url = bound.is_custom_url
                        self.custom_field_ids = bound.custom_field_ids
                        self.long_descr = bound.long_descr

                def get_binding_id_images():
                    binding_images = []
                    photo_path = creds.photo_path
                    list_of_files = os.listdir(photo_path)
                    if list_of_files is not None:
                        for file in list_of_files:
                            if (
                                file.split(".")[0].split("^")[0].lower()
                                == self.binding_id.lower()
                            ):
                                binding_images.append(file)

                    total_binding_images = len(binding_images)

                    if total_binding_images > 0:
                        # print(f"Found {total_binding_images} binding images for Binding ID: {self.binding_id}")
                        for image in binding_images:
                            binding_img = self.Image(image, last_run_time=last_sync)

                            if binding_img.validate():
                                print(
                                    f"Image {binding_img.image_name} is valid. Adding to self images list."
                                )
                                self.images.append(binding_img)

                    else:
                        print(
                            f"No binding images found for Binding ID: {self.binding_id}"
                        )

                # Add Binding ID Images to image list
                get_binding_id_images()

                # Get last maintained date of all the variants and set product last maintained date to the latest
                # Add Variant Images to image list and establish which image is the variant thumbnail
                lst_maint_dt_list = []

                for variant in self.variants:
                    variant_image_count = 0
                    # While we are here, let's get all the last maintenance dates for the variants
                    lst_maint_dt_list.append(variant.lst_maint_dt)
                    for variant_image in variant.images:
                        if variant_image_count == 0:
                            variant_image.is_variant_image = True
                        self.images.append(variant_image)
                        variant_image_count += 1

                # Set the product last maintained date to the latest of the variants. This will be used in the validation process.
                # If the product has been updated since the last sync, it will go through full validation. Otherwise, it will be skipped.
                self.lst_maint_dt = max(lst_maint_dt_list)

            def get_single_product_details():
                self.variants.append(self.Variant(self.sku, last_run_date=last_sync))
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
                self.cp_ecommerce_categories = single.cp_ecommerce_categories
                self.images = single.images
                self.custom_url = single.custom_url
                self.is_custom_url = single.is_custom_url
                self.custom_field_ids = single.custom_field_ids
                # Set the product last maintained date to the single product's last maintained date
                self.lst_maint_dt = single.lst_maint_dt
                self.long_descr = single.long_descr

            if self.is_bound:
                get_bound_product_details()
            else:
                get_single_product_details()

            self.bc_ecommerce_categories = self.get_bc_ecomm_categories()

            print("BC E-Commerce Categories: ", self.bc_ecommerce_categories)

            # Now all images are in self.images list and are in order by binding img first then variant img

            sort_order = 0
            for x in self.images:
                if sort_order == 0:
                    x.is_thumbnail = True
                x.sort_order = sort_order
                sort_order += 1

        def validate_product_inputs(self):
            """Validate product inputs to check for errors in user input"""
            # If the product has been updated since the last sync, it will go through full validation.
            # Otherwise, it will be skipped.
            if not self.lst_maint_dt > self.last_sync:
                print(
                    "Product has not been updated since last sync. Skipping validation."
                )
                return True
            check_web_title = True
            check_for_missing_categories = False
            check_html_description = False
            min_description_length = 20
            check_missing_images = True
            check_for_invalid_brand = True
            check_for_item_cost = False

            def set_parent(status: bool = True) -> None:
                """Target lowest price item in family to set as parent."""
                # Reestablish parent relationship
                flag = "Y" if status else "N"

                target_item = min(self.variants, key=lambda x: x.price_1).sku

                query = f"""
                UPDATE IM_ITEM
                SET IS_ADM_TKT = '{flag}', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO = '{target_item}'
                """
                self.db.query_db(query, commit=True)
                print(f"Parent status set to {flag} for {target_item}")
                return self.get_product_details()

            if self.is_bound:
                # Test for missing binding ID. Potentially add corrective action
                # (i.e. generate binding ID or remove product
                # and rebuild as a new single product)
                if self.binding_id == "":
                    message = f"Product {self.binding_id} has no binding ID. Validation failed."
                    self.errors.append(message)
                    print(message)
                    return False

                # Test for valid Binding ID Schema (ex. B0001)
                pattern = r"B\d{4}"
                if not bool(re.fullmatch(pattern, self.binding_id)):
                    message = f"Product {self.binding_id} has an invalid binding ID. Validation failed."
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

                # Test for parent product problems
                if self.validation_retries > 0:
                    if len(self.parent) != 1:
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
                            print(
                                f"Product {self.binding_id} has multiple parents. Will reestablish parent."
                            )
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

            # ALL PRODUCTS
            if check_web_title:
                # Test for missing web title
                if self.web_title == "":
                    message = f"Product {self.binding_id} is missing a web title. Will set to long description."
                    if self.is_bound:
                        # Bound product: use binding key and parent variant
                        query = f"""
                        UPDATE IM_ITEM
                        SET ADDL_DESCR_1 = '{self.web_title}'
                        WHERE USR_PROF_ALPHA_16 = '{self.binding_id}' and IS_ADM_TKT = 'Y'"""

                    # Single Product use sku
                    else:
                        query = f"""
                        UPDATE IM_ITEM
                        SET ADDL_DESCR_1 = '{self.long_descr}'
                        WHERE ITEM_NO = '{self.sku}'"""

                    try:
                        self.db.query_db(query, commit=True)
                    except Exception as e:
                        message = f"Error updating web title: {e}"
                        print(message)
                        self.errors.append(message)
                        return False
                    else:
                        print(f"Web Title set to {self.web_title}")
                        self.web_title = self.long_descr

                # Test for dupicate web title
                if self.is_bound:
                    # For bound products, look for matching web titles OUTSIDE of the current binding id
                    query = f"""
                    SELECT COUNT(ITEM_NO)
                    FROM IM_ITEM
                    WHERE ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}' AND USR_PROF_ALPHA_16 != '{self.binding_id}' AND IS_ECOMM_ITEM = 'Y'"""

                else:
                    query = f"""
                    SELECT COUNT(ITEM_NO)
                    FROM IM_ITEM
                    WHERE ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}' AND IS_ECOMM_ITEM = 'Y'"""

                response = self.db.query_db(query)

                if response:
                    if response[0][0] > 1:
                        message = f"Product {self.binding_id} has a duplicate web title. Will Append Sku to Web Title."
                        self.errors.append(message)

                        print(message)
                        if self.is_bound:
                            new_web_title = f"{self.web_title} - {self.binding_id}"
                        else:
                            new_web_title = f"{self.web_title} - {self.sku}"

                        self.web_title = new_web_title

                        print(f"New Web Title: {self.web_title}")
                        if self.is_bound:
                            # Update Parent Variant
                            query = f"""
                            UPDATE IM_ITEM
                            SET ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}'
                            WHERE USR_PROF_ALPHA_16 = '{self.binding_id}' and IS_ADM_TKT = 'Y'
                            
                            """
                        else:
                            # Update Single Product
                            query = f"""
                            UPDATE IM_ITEM
                            SET ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}'
                            WHERE ITEM_NO = '{self.sku}'"""

                        self.db.query_db(query, commit=True)

            # Test for missing html description
            if check_html_description:
                if len(self.html_description) < min_description_length:
                    message = f"Product {self.binding_id} is missing an html description. Validation failed."
                    self.errors.append(message)
                    print(message)
                    return False

            # Test for missing E-Commerce Categories
            if check_for_missing_categories:
                if not self.bc_ecommerce_categories:
                    message = f"Product {self.binding_id} is missing E-Commerce Categories. Validation failed."
                    self.errors.append(message)
                    print(message)
                    return False

            # Test for missing brand
            if check_for_invalid_brand:
                # Test for missing brand
                if self.brand:
                    bc_brands = [
                        x[0] for x in Catalog.Brands(last_sync=self.last_sync).mw_brands
                    ]
                    if self.brand not in bc_brands:
                        message = f"Product {self.binding_id} has a brand, but it is not valid. Will delete invalid brand."
                        print(message)
                        if self.validation_retries > 0:
                            self.reset_brand()
                            self.validation_retries -= 1
                            return self.validate_product_inputs()
                        else:
                            message = f"Product {self.binding_id} has an invalid brand. Validation failed."
                            self.errors.append(message)
                            print(message)
                            return False
                else:
                    message = f"Product {self.binding_id} is missing a brand. Will set to default."
                    if self.validation_retries > 0:
                        self.reset_brand()
                        self.validation_retries -= 1
                        self.brand = creds.default_brand
                    print(message)

            # Test for missing cost
            if check_for_item_cost:
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

            if check_missing_images:
                # Test for missing product images
                if len(self.images) == 0:
                    message = f"Product {self.binding_id} is missing images. Will turn visibility to off."
                    print(message)
                    self.visible = False

            # BOUND PRODUCTS
            if self.is_bound:
                # print(f"Product {self.binding_id} is a bound product. Validation starting...")
                if check_web_title:
                    for child in self.variants:
                        if not child.is_parent:
                            if child.web_title == self.web_title:
                                print(
                                    f"Non-Parent Variant {child.sku} has a web title. Will remove from child."
                                )
                                child.web_title = ""
                                query = f"""
                                UPDATE IM_ITEM
                                SET ADDL_DESCR_1 = NULL
                                WHERE ITEM_NO = '{child.sku}'"""
                                self.db.query_db(query, commit=True)

            # Need validations for character counts on all fields
            # print(f"Product {self.sku} has passed validation.")
            # Validation has Passed.
            return True

        def construct_product_payload(self, mode="create"):
            """Build the payload for creating a product in BigCommerce.
            This will include all variants, images, and custom fields."""

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
                    result.append(
                        {"name": "Botanical Name", "value": self.custom_botanical_name}
                    )
                if self.custom_climate_zone:
                    result.append(
                        {"name": "Climate Zone", "value": self.custom_climate_zone}
                    )
                if self.custom_plant_type:
                    result.append(
                        {"name": "Plant Type", "value": self.custom_plant_type}
                    )
                if self.custom_type:
                    result.append({"name": "Type", "value": self.custom_type})
                if self.custom_height:
                    result.append({"name": "Height", "value": self.custom_height})
                if self.custom_width:
                    result.append({"name": "Width", "value": self.custom_width})
                if self.custom_sun_exposure:
                    result.append(
                        {"name": "Sun Exposure", "value": self.custom_sun_exposure}
                    )
                if self.custom_bloom_time:
                    result.append(
                        {"name": "Bloom Time", "value": self.custom_bloom_time}
                    )
                if self.custom_bloom_color:
                    result.append(
                        {"name": "Bloom Color", "value": self.custom_bloom_color}
                    )
                if self.custom_attracts_pollinators:
                    result.append(
                        {
                            "name": "Attracts Pollinators",
                            "value": self.custom_attracts_pollinators,
                        }
                    )
                if self.custom_growth_rate:
                    result.append(
                        {"name": "Growth Rate", "value": self.custom_growth_rate}
                    )
                if self.custom_deer_resistant:
                    result.append(
                        {"name": "Deer Resistant", "value": self.custom_deer_resistant}
                    )
                if self.custom_soil_type:
                    result.append({"name": "Soil Type", "value": self.custom_soil_type})
                if self.custom_color:
                    result.append({"name": "Color", "value": self.custom_color})
                if self.custom_size:
                    result.append({"name": "Size", "value": self.custom_size})
                return result

            def construct_image_payload():
                result = []
                # Child Images
                for image in self.images:
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
                            "sku": child.sku,
                            "option_values": [
                                {
                                    "option_display_name": "Option",
                                    "label": child.variant_name,
                                }
                            ],
                            "calculated_price": 0.1,
                            "calculated_weight": 0.1,
                        }

                        if self.product_id:
                            variant_payload["product_id"] = self.product_id

                        if child.variant_id:
                            variant_payload["id"] = child.variant_id

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
                # "product_tax_code": "string",
                "brand_id": get_brand_id(),
                "brand_name": self.brand,
                "inventory_level": self.buffered_quantity,
                "inventory_warning_level": 10,
                "inventory_tracking": "variant" if self.is_bound else "product",
                "is_free_shipping": False,
                "is_visible": self.visible,
                "is_featured": self.featured,
                "sort_order": self.sort_order,
                "search_keywords": self.search_keywords,
                "availability": "available" if not self.in_store_only else "disabled",
                "gift_wrapping_options_type": "none" if not self.gift_wrap else "any",
                "condition": "New",
                "is_condition_shown": True,
                "page_title": self.meta_title,
                "meta_description": self.meta_description,
                # "preorder_release_date": self.preorder_release_date,
                "preorder_message": self.preorder_message,
                "is_preorder_only": self.is_preorder_only,
                "is_price_hidden": self.is_price_hidden,
                "custom_fields": construct_custom_fields(),
                "videos": construct_video_payload(),
            }
            # If the product has a product_id, it is an update
            if self.product_id:
                payload["id"] = self.product_id

            # Add child products
            if len(self.variants) >= 1:
                payload["variants"] = construct_variant_payload()

            # Add images
            if self.images:
                payload["images"] = construct_image_payload()

            # Add custom URL
            if self.custom_url != "":
                print("adding custom url")
                payload["custom_url"] = {
                    "url": f"/{self.custom_url}/",
                    "is_customized": self.is_custom_url,
                    "create_redirect": True,
                }

            # Add E-Commerce Categories
            if self.bc_ecommerce_categories:
                payload["categories"] = self.bc_ecommerce_categories

            return payload

        def process(self, retries=3):
            """Process Product Creation/Delete/Update in BigCommerce and Middleware."""
            if retries > 0:

                def create():
                    """Create new product in BigCommerce and Middleware."""
                    response = self.bc_post_product()
                    if response.status_code == 200:
                        self.get_product_data_from_bc(bc_response=response)
                        self.insert_product()
                        self.insert_images()
                    elif response.status_code == 409:
                        print("Product already exists in BigCommerce")
                        return self.rollback_product()

                def update():
                    """Will update existing product. Will clear out custom field data and reinsert."""
                    print("Entering Update Product Function")
                    update_payload = self.construct_product_payload(
                        mode="update_product"
                    )
                    self.bc_delete_custom_fields(asynchronous=True)
                    response = self.bc_update_product(update_payload)
                    if response.status_code in [200, 201, 207]:
                        self.get_product_data_from_bc(bc_response=response)
                        self.middleware_sync_product()
                        self.middleware_sync_images()
                    elif response.status_code in [400, 404]:
                        print("BC Product update error")
                        return self.rollback_product()

                # This is problematic. If products change, they may exist in the database but still require a full reset...
                # Perhaps this is where second stage product validation should occur.

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

        ### DELETES WILL HAVE TO HAPPEN BEFORE IMAGES ARE APPENDED TO SELF.IMAGES***
        def replace_image(self, image) -> bool:
            """Replace image in BigCommerce and SQL."""
            print("Entering replace_image function")
            delete_response = self.delete_image(image.image_name)
            if delete_response.status_code == 204:
                self.delete_product_image_from_sql
                post_response = self.bc_post_image(image)
                if post_response.status_code == 200:
                    insert_response = self.insert_image(image)
                    print(insert_response)
                else:
                    print(
                        f"Replace Image: Error posting {image.image_name} to BigCommerce"
                    )
                    print(post_response.status_code, post_response.content)
                    return False
            else:
                print(f"Error deleting {image.image_name} from BigCommerce")
                print(delete_response.status_code, delete_response.content)
                return False

        def get_bc_ecomm_categories(self):
            """Get BigCommerce Category IDs from Middleware Category IDs"""
            result = []

            if self.cp_ecommerce_categories:
                for category in self.cp_ecommerce_categories:
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

        # BigCommerce Methods

        def get_product_data_from_bc(self, bc_response):
            # Assign PRODUCT_ID, VARIANT_ID, and CATEG_ID to product and insert into middleware
            self.product_id = bc_response.json()["data"]["id"]
            custom_field_response = bc_response.json()["data"]["custom_fields"]

            if custom_field_response:
                self.custom_field_ids = ",".join(
                    [str(x["id"]) for x in custom_field_response]
                )
                print("Custom Field IDs: ", self.custom_field_ids)

            for x, variant in enumerate(self.variants):
                variant.binding_id = self.binding_id
                variant.product_id = self.product_id
                variant.variant_id = bc_response.json()["data"]["variants"][x]["id"]

            # Update Image IDs
            print("Updating Image IDs")
            image_response = bc_response.json()["data"]["images"]
            if image_response and self.images:
                for bc_image in image_response:
                    for image in self.images:
                        if bc_image["sort_order"] == image.sort_order:
                            image.image_id = bc_image["id"]

            for image in self.images:
                image.product_id = self.product_id

        def bc_post_product(self):
            """Create product in BigCommerce. For this implementation, this is a single product with no
            variants"""
            print("Entering bc_post_product function of product class")
            url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products?include=custom_fields,variants,images"
            payload = self.construct_product_payload()

            retries = 1

            while True:
                print("-----" * 10)
                print("BigCommerce POST Request")
                print("---" * 10)

                bc_response = requests.post(
                    url=url, headers=creds.test_bc_api_headers, json=payload
                )
                return bc_response

        def bc_post_image(self, image):
            # Post New Image to Big Commerce

            image_payload = {
                "is_thumbnail": image.is_thumbnail,
                "sort_order": image.sort_order,
                "description": image.description,
                "image_url": image.image_url,
            }

            if self.product_id:
                image_payload["product_id"] = self.product_id

            url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products/{self.product_id}/images"

            bc_response = requests.post(
                url=url, headers=creds.test_bc_api_headers, json=image_payload
            )
            return bc_response

        def bc_update_product(self, payload):
            print("Entering bc_update_product function of product class")
            url = (
                f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                f"catalog/products/{self.product_id}?include=custom_fields,variants,images"
            )
            print("-----" * 10)
            print("BigCommerce PUT Request")
            print("---" * 10)

            update_response = requests.put(
                url=url,
                headers=creds.test_bc_api_headers,
                json=payload,
                timeout=10,
            )
            return update_response

        def bc_get_custom_fields(self):
            print("Entering bc_get_custom_fields function of product class")
            custom_fields = []
            cf_url = (
                f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                f"catalog/products/{self.product_id}/custom-fields"
            )
            cf_response = requests.get(url=cf_url, headers=creds.test_bc_api_headers)
            if cf_response.status_code == 200:
                custom_field_data = cf_response.json()["data"]
                for field in custom_field_data:
                    custom_fields.append(field["id"])
            return custom_fields

        def bc_delete_custom_fields(self, asynchronous=False):
            print("Entering bc_delete_custom_fields function of product class")
            if self.custom_field_ids:
                id_list = self.custom_field_ids.split(",")
            else:
                # If there are no custom fields in the middleware, get them from BigCommerce
                id_list = self.bc_get_custom_fields()
                # If there are no custom fields in BigCommerce, return
                if not id_list:
                    return

            if asynchronous:

                async def bc_delete_custom_fields_async():
                    async with aiohttp.ClientSession() as session:
                        for field_id in id_list:
                            async_url = f"""https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products/{self.product_id}/custom-fields/{field_id}"""

                            async with session.delete(
                                url=async_url, headers=creds.test_bc_api_headers
                            ) as resp:
                                text_response = await resp.text()
                                if text_response:
                                    print(text_response)

                asyncio.run(bc_delete_custom_fields_async())
                update_cf1_query = f"""UPDATE {creds.bc_product_table}
                            SET CUSTOM_FIELDS = NULL, LST_MAINT_DT = GETDATE()
                            WHERE PRODUCT_ID = '{self.product_id}' AND IS_PARENT = 1
                            """
                self.db.query_db(update_cf1_query, commit=True)

            else:
                # Synchronous Version
                success = True
                success_list = []
                db = query_engine.QueryEngine()
                # Delete Each Custom Field
                for number in id_list:
                    url = (
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                        f"catalog/products/{self.product_id}/custom-fields/{number}"
                    )

                    cf_remove_response = requests.delete(
                        url=url, headers=creds.test_bc_api_headers, timeout=10
                    )
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
                        message = (
                            f"Middleware REMOVE CUSTOM_FIELDS: {self.sku}: FAILED ",
                            e,
                        )
                        print(message)
                        self.errors.append(message)
                else:
                    # Partial Success
                    # If this wasn't totally successful, but some were deleted,
                    # update the CUSTOM_FIELDS in middleware
                    if success_list:
                        success_list_string = ",".join(
                            str(cust_field) for cust_field in success_list
                        )
                        update_cf2_query = f"""
                        UPDATE {creds.bc_product_table}
                        SET CUSTOM_FIELDS = '{success_list_string}', LST_MAINT_DT = GETDATE()
                        WHERE PRODUCT_ID = '{self.product_id}' AND IS_PARENT = 1
                        """
                        try:
                            db.query_db(update_cf2_query, commit=True)
                        except Exception as e:
                            message = (
                                f"Middleware CUSTOM_FIELDS ROLLBACK: {self.sku}: FAILED ",
                                e,
                            )
                            print(message)
                            self.errors.append(message)
                    else:
                        # If no custom fields were deleted, but there was an error,
                        # don't update the CUSTOM_FIELDS in middleware
                        pass

        def second_stage_validation(self):
            print("Entering Second Stage Validation Function of Product Class")
            """Second-State Validation will check for changes in the product being updated that would require a full reset"""
            valid = True
            # Check for bound product trying to update a single product
            # Check for single item trying to update a bound product
            return valid

        # Middleware Methods
        def middleware_sync_product(self):
            print("Entering Middleware Sync Product Function of Product Class")
            success = True
            for variant in self.variants:
                if variant.db_id is None:
                    # If variant.db_id is None, this is a new product to be inserted into SQL
                    self.insert_product(variant)
                else:
                    self.update_product(variant)
            if not success:
                self.rollback_product()

            return success

        def middleware_sync_images(self):
            """Sync images to middleware. Will check for success and rollback by deleting image
            references from BigCommerce and the middleware if needed."""
            print("Entering Middleware Sync Images Function of Product Class")
            rollback = False
            for image in self.images:
                if image.id is None:
                    print("Inserting Image")
                    if not self.insert_image(image=image):
                        rollback = True
                else:
                    print("Updating Image")
                    status = self.update_image(image)
                    if status is False:
                        rollback = True

                # If rollback is True, delete image from BigCommerce and Middleware
                if rollback:
                    print("Rolling Back Image")
                    self.rollback_image(image)

        def insert_product(self):
            """Insert product into middleware"""
            print("Entering Insert Product Function of Product Class")
            success = True
            for variant in self.variants:
                self.insert_variant(variant)
            if success:
                print(
                    f"Product {self.sku} Binding: {self.binding_id} inserted into middleware."
                )
            else:
                self.rollback_product()
            return success

        def insert_variant(self, variant):
            print("Entering Insert Variant Function of Product Class")

            custom_field_string = self.custom_field_ids
            if not variant.is_parent:
                custom_field_string = None

            if self.bc_ecommerce_categories:
                categories_string = ",".join(
                    str(x) for x in self.bc_ecommerce_categories
                )
            else:
                categories_string = None

            insert_query = (
                f"INSERT INTO {creds.bc_product_table} (ITEM_NO, BINDING_ID, IS_PARENT, "
                f"PRODUCT_ID, VARIANT_ID, CATEG_ID, CUSTOM_FIELDS) VALUES ('{variant.sku}', "
                f"{f"'{self.binding_id}'" if self.binding_id != '' else 'NULL'}, "
                f"{1 if variant.is_parent else 0}, {self.product_id if self.product_id else "NULL"}, "
                f"{variant.variant_id if variant.variant_id else "NULL"}, "
                f"{f"'{categories_string}'" if categories_string else "NULL"}, "
                f"{f"'{custom_field_string}'" if custom_field_string else "NULL"})"
            )
            print("insert_variant query")
            print(insert_query)

            insert_product_response = self.db.query_db(insert_query, commit=True)
            print("insert_product_response")
            print(insert_product_response)
            if insert_product_response["code"] != 200:
                message = (
                    f"Middleware INSERT product {self.sku}: "
                    f"Non 200 response: {insert_product_response}"
                )
                print(message)
                self.errors.append(message)
                self.errors.append(insert_product_response)

        def update_product(self, variant):
            print("Entering Update Product Function of Product Class")
            custom_field_string = self.custom_field_ids
            if not variant.is_parent:
                custom_field_string = None

            if self.bc_ecommerce_categories:
                categories_string = ",".join(
                    str(x) for x in self.bc_ecommerce_categories
                )
            else:
                categories_string = None

            update_query = (
                f"UPDATE {creds.bc_product_table} "
                f"SET ITEM_NO = '{variant.sku}', "
                f"BINDING_ID = "
                f"{f"'{self.binding_id}'" if self.binding_id != '' else 'NULL'}, "
                f"IS_PARENT = {1 if variant.is_parent else 0}, "
                f"PRODUCT_ID = {self.product_id if self.product_id else 'NULL'}, "
                f"VARIANT_ID = "
                f"{variant.variant_id if variant.variant_id else 'NULL'}, "
                f"CATEG_ID = {f"'{categories_string}'" if categories_string else "NULL"}, "
                f"CUSTOM_FIELDS = {f"'{custom_field_string}'" if custom_field_string else "NULL"}, "
                f"LST_MAINT_DT = GETDATE() "
                f"WHERE ID = {variant.db_id}"
            )

            print("update_product query")
            print(update_query)

            update_product_response = self.db.query_db(update_query, commit=True)
            if update_product_response["code"] != 200:
                message = (
                    f"Middleware UPDATE product {self.sku}: "
                    f"Non 200 response: {update_product_response}"
                )
                print(message)
                self.errors.append(message)
                self.errors.append(update_product_response)

        def reset_brand(self):
            """Delete brand from item in counterpoint. Used as a corrective measure when an item has a prof_cod_1 that doesn't exist in
            ITEM_PROF_COD"""
            if self.is_bound:
                reset_brand_query = f"""
                UPDATE IM_ITEM
                SET PROF_COD_1 = "SETTLEMYRE", LST_MOD_DT = GETDATE()
                WHERE USR_PROF_ALPHA_16 = '{self.binding_id}' AND IS_ADM_TKT = 'Y'
                """
            else:
                reset_brand_query = f"""
                UPDATE IM_ITEM
                SET PROF_COD_1 = NULL, LST_MOD_DT = GETDATE()
                WHERE ITEM_NO = '{self.sku}'
                """
            self.db.query_db(reset_brand_query, commit=True)

        def get_image_id(self, target):
            """Get image ID from SQL using filename. If not found, return None."""
            image_query = f"SELECT IMAGE_ID FROM {creds.bc_image_table} WHERE IMAGE_NAME = '{target}'"
            img_id_res = self.db.query_db(image_query)
            if img_id_res is not None:
                return img_id_res[0][0]

                print("No images to delete.")

        def delete_image(self, image_name) -> bool:
            """Takes in an image name and looks for matching image file in middleware. If found, deletes from BC and SQL."""
            print("Entering Delete Image Function of Product Class")
            print(f"Deleting {image_name}")

            image_id = self.get_image_id(image_name)

            delete_img_url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products/{self.product_id}/images/{image_id}"

            img_del_res = requests.delete(
                url=delete_img_url, headers=creds.test_bc_api_headers, timeout=10
            )

            delete_images_query = (
                f"DELETE FROM {creds.bc_image_table} " f"WHERE IMAGE_ID = '{image_id}'"
            )
            self.db.query_db(delete_images_query, commit=True)

            return img_del_res

        def delete_product_image_from_sql(self, image):
            """Delete product images from SQL."""
            print("Entering Delete Product Image Function of Product Class")
            if self.is_bound:
                delete_images_query = (
                    f"DELETE FROM {creds.bc_image_table} "
                    f"WHERE BINDING_ID = '{self.binding_id}'"
                )
            else:
                delete_images_query = (
                    f"DELETE FROM {creds.bc_image_table} "
                    f"WHERE ITEM_NO = '{self.sku}'"
                )

                self.db.query_db(delete_images_query, commit=True)

        def delete_product_images_from_sql(self):
            """Delete all images associated with a product from SQL."""
            print("Entering Delete Product Images Function of Product Class")
            if self.is_bound:
                child_skus = tuple([variant.sku for variant in self.variants])

                delete_images_query = (
                    f"DELETE FROM {creds.bc_image_table} "
                    f"WHERE BINDING_ID = '{self.binding_id}'"
                    f"DELETE FROM {creds.bc_image_table} "
                    f"WHERE ITEM_NO IN {child_skus}"
                )

            else:
                delete_images_query = (
                    f"DELETE FROM {creds.bc_image_table} "
                    f"WHERE ITEM_NO = '{self.sku}'"
                )
            self.db.query_db(delete_images_query, commit=True)

        def insert_images(self):
            """Insert images into SQL."""
            print("Entering Insert Images Function of Product Class")
            for image in self.images:
                self.insert_image(image)
            print(f"Images Inserted: At this point we have {len(self.errors)} errors.")

        def insert_image(self, image) -> bool:
            """Insert image into SQL."""
            print("Entering Insert Image Function of Product Class")

            img_insert = f"""
            INSERT INTO {creds.bc_image_table} (IMAGE_NAME, ITEM_NO, FILE_PATH,
            IMAGE_URL, PRODUCT_ID, IMAGE_ID, THUMBNAIL, IMAGE_NUMBER, SORT_ORDER,
            IS_BINDING_IMAGE, BINDING_ID, IS_VARIANT_IMAGE, DESCR, SIZE)
            VALUES (
            '{image.image_name}',
            {f"'{image.sku}'" if image.sku != '' else 'NULL'},
            '{image.file_path}',
            '{image.image_url}',
            '{image.product_id}',
            '{image.image_id}',
            '{1 if image.is_thumbnail else 0}', '{image.image_number}',
            '{image.sort_order}',
            '{image.is_binding_image}',
            {f"'{image.binding_id}'" if image.binding_id != '' else 'NULL'},
            '{image.is_variant_image}',
            {f"'{image.description.replace("'", "''")}'" if image.description != '' else 'NULL'},
            {image.size})"""

            try:
                print(f"Inserting Image {image.image_name} into SQL")
                insert_img_response = query_engine.QueryEngine().query_db(
                    img_insert, commit=True
                )
                print("Response: ", insert_img_response)

            except Exception as e:
                print("Insert image function uncaught error")
                message = f"Middleware INSERT image {image.image_name}: FAILED {e}"
                print(message)
                self.errors.append(message)
                return False

            else:
                if insert_img_response["code"] != 200:
                    message = (
                        f"Middleware INSERT image {image.image_name}: "
                        f"Non 200 response: {insert_img_response}"
                    )
                    print(message)
                    self.errors.append(message)
                    self.errors.append(insert_img_response)
                    return False
                else:
                    return True

        def update_image(self, image) -> bool:
            """Update image in SQL."""
            print("Entering Update Image Function of Product Class")
            img_update = f"""
                UPDATE {creds.bc_image_table}
                SET IMAGE_NAME = '{image.image_name}',
                ITEM_NO = '{image.sku}',
                FILE_PATH = '{image.file_path}',
                IMAGE_URL = '{image.image_url}',
                PRODUCT_ID = '{image.product_id}',
                IMAGE_ID = '{image.image_id}',
                THUMBNAIL = '{1 if image.is_thumbnail else 0}',
                IMAGE_NUMBER = '{image.image_number}',
                SORT_ORDER = '{image.sort_order}',
                IS_BINDING_IMAGE = '{image.is_binding_image}',
                BINDING_ID = {f"'{image.binding_id}'" if image.binding_id != '' else 'NULL'},
                IS_VARIANT_IMAGE = '{image.is_variant_image}',
                DESCR = {f"'{image.description.replace("'", "''")}'" if
                            image.description != '' else 'NULL'},
                SIZE = '{image.size}'
                WHERE ID = {image.id}"""

            try:
                update_img_response = self.db.query_db(img_update, commit=True)
            except Exception as e:
                message = f"Middleware UPDATE image {image.image_name}: FAILED {e}"
                print(message)
                self.errors.append(message)
                return False
            else:
                if update_img_response["code"] != 200:
                    message = (
                        f"Middleware UPDATE image {image.image_name}: "
                        f"Non 200 response: {update_img_response}"
                    )
                    print(message)
                    self.errors.append(message)
                    self.errors.append(update_img_response)
                    return False
                else:
                    return True

        def rollback_product(self, retries=3):
            """Delete product from BigCommerce and Middleware."""
            print("Entering Rollback Product Function of Product Class")
            if retries > 0:
                retries -= 1
                print(
                    f"\n\n!!! Rolling back product SKU: {self.sku}, Binding: {self.binding_id}!!! \n\n"
                )
                self.hard_reset_product()

                print("\n\nReinitializing Product!\n\n")

                if self.is_bound:
                    prod_data = {
                        "sku": self.parent[0].sku,
                        "binding_id": self.binding_id,
                    }
                else:
                    prod_data = {"sku": self.sku, "binding_id": self.binding_id}

                self.__init__(
                    product_data=prod_data,
                    last_sync=date_presets.business_start_date,
                )
                if self.validate_product_inputs:
                    print("HEY IT PASSED VALIDATION")
                    self.process(retries)

        def hard_reset_product(self):
            """Hard reset product from BigCommerce and Middleware AND deletes associated images."""
            print("Entering Hard Reset Product Function of Product Class")
            self.delete_product_from_bc()
            self.delete_product_from_sql()
            self.delete_product_images_from_sql()

        def delete_product_from_bc(self):
            # Delete product from BigCommerce
            print("Entering Delete Product from BC Function of Product Class")
            print(
                f"Deleting Product Product ID: {self.product_id}, Sku:{self.sku}, Binding ID: {self.binding_id} from BigCommerce"
            )
            if self.is_bound:
                url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products?sku={self.binding_id}"
            else:
                url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/products?sku={self.sku}"

            print(url)

            delete_response = requests.delete(
                url=url, headers=creds.test_bc_api_headers, timeout=10
            )

            if delete_response.status_code == 204:
                self.is_uploaded = False
                message = f"Product {self.sku} deleted from BigCommerce."
                print(message)
            else:
                message = f"Error deleting product {self.sku} from BigCommerce."
                print(message)
                print(url)
                self.errors.append(message, delete_response.content)

        def delete_product_from_sql(self):
            print("Entering Delete Product from SQL Function of Product Class")
            if self.is_bound:
                delete_product_query = (
                    f"DELETE FROM {creds.bc_product_table} "
                    f"WHERE BINDING_ID = '{self.binding_id}'"
                )
            else:
                delete_product_query = (
                    f"DELETE FROM {creds.bc_product_table} "
                    f"WHERE ITEM_NO = '{self.sku}'"
                )
            print("Deleting Product from SQL")
            self.db.query_db(delete_product_query, commit=True)

        def rollback_image(self, image):
            print("Entering Rollback Image Function of Product Class")
            """Delete associated image from BigCommerce and Middleware"""
            print(f"Rolling back image {image.image_name}")
            url = (
                f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                f"catalog/products/{self.product_id}/images/{image.image_id}"
            )

            delete_response = requests.delete(
                url=url, headers=creds.test_bc_api_headers, timeout=10
            )

            if delete_response.status_code == 204:
                print(f"Image: {image.image_name} deleted from BigCommerce.")

                del_img = f"DELETE FROM {creds.bc_image_table} WHERE IMAGE_ID = '{image.image_id}'"
                try:
                    self.db.query_db(del_img, commit=True)
                except Exception as e:
                    message = f"Middleware Rollback DELETE image {image.image_name}: FAILED {e}"
                    print(message)
                    self.errors.append(message)
            else:
                message = f"Error deleting product {self.sku} from BigCommerce."
                print(message)
                self.errors.append(message)
                self.errors.append(delete_response.content)

        def remove_parent(self):
            print("Entering Remove Parent Function of Product Class")
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
            query = """
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
                self.last_run_date = last_run_date

                # Product ID Info
                product_data = self.get_variant_details()

                # Product Information
                self.db_id = product_data["db_id"]
                self.binding_id = product_data["binding_id"]
                self.is_parent = True if product_data["is_parent"] == "Y" else False
                self.product_id: int = (
                    product_data["product_id"] if product_data["product_id"] else None
                )
                self.variant_id: int = (
                    product_data["variant_id"] if product_data["variant_id"] else None
                )
                self.web_title: str = product_data["web_title"]
                self.long_descr = product_data["long_descr"]
                self.variant_name = product_data["variant_name"]
                self.status = product_data["status"]
                self.price_1 = float(product_data["price_1"])
                self.cost = float(product_data["cost"])
                self.price_2 = float(product_data["price_2"])
                # Inventory Levels
                self.quantity_available = product_data["quantity_available"]
                self.buffer = product_data["buffer"]
                self.buffered_quantity = self.quantity_available - self.buffer
                if self.buffered_quantity < 0:
                    self.buffered_quantity = 0
                self.weight = 0.1
                self.width = 0.1
                self.height = 0.1
                self.depth = 0.1
                self.in_store_only = product_data["in_store_only"]
                self.sort_order = product_data["sort_order"]
                self.is_price_hidden = False
                # Purchasing Disabled is for Variants Only
                self.purchasing_disabled = False
                self.purchasing_disabled_message = ""
                # Brand
                self.brand = product_data["brand"]
                self.html_description = product_data["html_description"]
                self.search_keywords = product_data["search_keywords"]
                self.meta_title = product_data["meta_title"]
                self.meta_description = product_data["meta_description"]
                self.visible: bool = product_data["web_visible"]
                self.featured: bool = product_data["is_featured"]
                self.gift_wrap: bool = product_data["gift_wrap"]
                self.is_free_shipping = False
                self.is_preorder = False
                self.preorder_release_date = datetime(1970, 1, 1)
                self.preorder_message = product_data["preorder_message"]
                self.alt_text_1 = product_data["alt_text_1"]
                self.alt_text_2 = product_data["alt_text_2"]
                self.alt_text_3 = product_data["alt_text_3"]
                self.alt_text_4 = product_data["alt_text_4"]

                # Custom Fields
                self.custom_botanical_name = product_data["custom_botanical_name"]
                self.custom_climate_zone = product_data["custom_climate_zone"]
                self.custom_plant_type = product_data["custom_plant_type"]
                self.custom_type = product_data["custom_type"]
                self.custom_height = product_data["custom_height"]
                self.custom_width = product_data["custom_width"]
                self.custom_sun_exposure = product_data["custom_sun_exposure"]
                self.custom_bloom_time = product_data["custom_bloom_time"]
                self.custom_bloom_color = product_data["custom_bloom_color"]
                self.custom_attracts_pollinators = product_data[
                    "custom_attracts_pollinators"
                ]
                self.custom_growth_rate = product_data["custom_growth_rate"]
                self.custom_deer_resistant = product_data["custom_deer_resistant"]
                self.custom_soil_type = product_data["custom_soil_type"]
                self.custom_color = product_data["custom_color"]
                self.custom_size = product_data["custom_size"]
                self.custom_field_ids = product_data["custom_field_ids"]
                self.custom_url = product_data["custom_url"]
                self.custom_url = "-".join(
                    str(re.sub("[^A-Za-z0-9 ]+", "", self.custom_url)).split(" ")
                )
                self.is_custom_url = product_data["is_custom_url"]

                # Product Images
                self.images = []

                # Dates
                self.lst_maint_dt = product_data["lst_maint_dt"]

                # E-Commerce Categories

                self.cp_ecommerce_categories = product_data["cp_ecommerce_categories"]

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
                query = f""" select ISNULL(ITEM.USR_PROF_ALPHA_16, '') as 'Binding ID(0)', ITEM.IS_ECOMM_ITEM as 'Web 
                Enabled(1)', ISNULL(ITEM.IS_ADM_TKT, 'N') as 'Is Parent(2)', BC_PROD.PRODUCT_ID as 'Product ID (3)', 
                BC_PROD.VARIANT_ID as 'Variant ID(4)', ITEM.USR_CPC_IS_ENABLED 
                as 'Web Visible(5)', ITEM.USR_ALWAYS_ONLINE as 'ALWAYS ONLINE(6)', ITEM.IS_FOOD_STMP_ITEM as 
                'GIFT_WRAP(7)', ITEM.PROF_COD_1 as 'BRAND_CP_COD(8)', ITEM.ECOMM_NEW as 'IS_FEATURED(9)', 
                ITEM.USR_IN_STORE_ONLY as 'IN_STORE_ONLY(10)', ITEM.USR_PROF_ALPHA_27 as 'SORT ORDER(11)', 
                ISNULL(ITEM.ADDL_DESCR_1, '') as 'WEB_TITLE(12)', ISNULL(ITEM.ADDL_DESCR_2, '') as 'META_TITLE(13)', 
                ISNULL(USR_PROF_ALPHA_21, '') as 'META_DESCRIPTION(14)', ISNULL(ITEM.USR_PROF_ALPHA_17, 
                '') as 'VARIANT NAME(15)', ITEM.STAT as 'STATUS(16)', ISNULL(ITEM.REG_PRC, 0) as 'REG_PRC(17)', 
                ISNULL(ITEM.PRC_1, 0) as 'PRC_1(18)', ISNULL(PRC.PRC_2, 0) as 'PRC_2(19)', CAST(ISNULL(INV.QTY_AVAIL, 
                0) as INTEGER) as 'QUANTITY_AVAILABLE(20)', CAST(ISNULL(ITEM.PROF_NO_1, 0) as INTEGER) as 'BUFFER(
                21)', ITEM.ITEM_TYP as 'ITEM_TYPE(22)', ITEM.LONG_DESCR as 'LONG_DESCR(23)', 
                ISNULL(ITEM.USR_PROF_ALPHA_26, '') as 'SEARCH_KEYWORDS(24)', ITEM.USR_PROF_ALPHA_19 as 
                'PREORDER_MESSAGE(25)', ISNULL(EC_ITEM_DESCR.HTML_DESCR, '') as 'HTML_DESCRIPTION(26)', 
                ISNULL(USR_PROF_ALPHA_22, '') as 'ALT_TEXT_1(27)', ISNULL(USR_PROF_ALPHA_23, '') as 'ALT_TEXT_2(28)', 
                ISNULL(USR_PROF_ALPHA_24, '') as 'ALT_TEXT_3(29)', ISNULL(USR_PROF_ALPHA_25, '') as 'ALT_TEXT_4(30)', 
                ISNULL(PROF_ALPHA_1, '') as 'BOTANICAL_NAM(31)', ISNULL(PROF_ALPHA_2, '') as 'CLIMATE_ZONE(32)', 
                ISNULL(PROF_ALPHA_3, '') as 'PLANT_TYPE(33)', ISNULL(PROF_ALPHA_4, '') as 'TYPE(34)', 
                ISNULL(PROF_ALPHA_5, '') as 'HEIGHT(35)', ISNULL(USR_PROF_ALPHA_6, '') as 'WIDTH(36)', 
                ISNULL(USR_PROF_ALPHA_7, '') as 'SUN_EXPOSURE(37)', ISNULL(USR_PROF_ALPHA_8, '') as 'BLOOM_TIME(38)', 
                ISNULL(USR_PROF_ALPHA_9, '') as 'BLOOM_COLOR(39)', ISNULL(USR_PROF_ALPHA_10, 
                '') as 'ATTRACTS_POLLINATORS(40)', ISNULL(USR_PROF_ALPHA_11, '') as 'GROWTH_RATE(41)', 
                ISNULL(USR_PROF_ALPHA_12, '') as 'DEER_RESISTANT(42)', ISNULL(USR_PROF_ALPHA_13, '') as 'SOIL_TYPE(
                43)', ISNULL(USR_PROF_ALPHA_14, '') as 'COLOR(44)', ISNULL(USR_PROF_ALPHA_15, '') as 'SIZE(45)', 
                ITEM.LST_MAINT_DT as 'LST_MAINT_DT(46)', ISNULL(ITEM.LST_COST, 0) as 'LAST_COST(47)', ITEM.ITEM_NO as 
                'ITEM_NO (48)', stuff(( select ',' + EC_CATEG_ITEM.CATEG_ID from EC_CATEG_ITEM where 
                EC_CATEG_ITEM.ITEM_NO =ITEM.ITEM_NO for xml path('')),1,1,'') as 'categories(49)',

                BC_PROD.ID as 'db_id(50)', BC_PROD.CUSTOM_FIELDS as 'custom_field_ids(51)', ITEM.LONG_DESCR as 'long_descr(52)'


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
                        "sku": item[0][48],
                        "db_id": item[0][50],
                        "binding_id": item[0][0],
                        "is_bound": True if item[0][0] else False,
                        "web_enabled": True if item[0][1] == "Y" else False,
                        "is_parent": item[0][2],
                        "product_id": item[0][3],
                        "variant_id": item[0][4],
                        "web_visible": True if item[0][5] == "Y" else False,
                        "always_online": True if item[0][6] == "Y" else False,
                        "gift_wrap": True if item[0][7] == "Y" else False,
                        "brand": item[0][8],
                        "is_featured": True if item[0][9] == "Y" else False,
                        "in_store_only": True if item[0][10] == "Y" else False,
                        "sort_order": int(item[0][11]) if item[0][11] else 0,
                        "web_title": item[0][12],
                        "meta_title": item[0][13],
                        "meta_description": item[0][14],
                        "variant_name": item[0][15],
                        "status": item[0][16],
                        # Product Pricing
                        "reg_price": item[0][17],
                        "price_1": item[0][18],
                        "price_2": item[0][19],
                        # # Inventory Levels
                        "quantity_available": item[0][20],
                        "buffer": item[0][21],
                        # Additional Details
                        "item_type": item[0][22],
                        "long_description": item[0][23],
                        "search_keywords": item[0][24],
                        "preorder_message": item[0][25],
                        "html_description": item[0][26],
                        "alt_text_1": item[0][27],
                        "alt_text_2": item[0][28],
                        "alt_text_3": item[0][29],
                        "alt_text_4": item[0][30],
                        # Custom Fields
                        "custom_botanical_name": item[0][31],
                        "custom_climate_zone": item[0][32],
                        "custom_plant_type": item[0][33],
                        "custom_type": item[0][34],
                        "custom_height": item[0][35],
                        "custom_width": item[0][36],
                        "custom_sun_exposure": item[0][37],
                        "custom_bloom_time": item[0][38],
                        "custom_bloom_color": item[0][39],
                        "custom_attracts_pollinators": item[0][40],
                        "custom_growth_rate": item[0][41],
                        "custom_deer_resistant": item[0][42],
                        "custom_soil_type": item[0][43],
                        "custom_color": item[0][44],
                        "custom_size": item[0][45],
                        "lst_maint_dt": item[0][46],
                        "cost": item[0][47],
                        "cp_ecommerce_categories": str(item[0][49]).split(",")
                        if item[0][49]
                        else [],
                        "custom_url": "",
                        "is_custom_url": False,
                        "custom_field_ids": item[0][51],
                        "long_descr": item[0][52],
                    }
                    # for x in details:
                    #     print(f"{x}: {details[x]}")
                    return details

            def validate_product(self):
                print(f"Validating product {self.sku}")
                # Test for missing variant name
                if self.variant_name == "":
                    print(
                        f"Product {self.sku} is missing a variant name. Validation failed."
                    )
                    return False
                # Test for missing price 1
                if self.price_1 == 0:
                    print(
                        f"Product {self.sku} is missing a price 1. Validation failed."
                    )
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
                        if x.split(".")[0].split("^")[0].lower() == self.sku.lower():
                            product_images.append(x)
                total_images = len(product_images)
                print(f"Found {total_images} product images for item: {self.sku}")
                if total_images > 0:
                    # print(f"Found {total_images} product images for item: {self.sku}")
                    for image in product_images:
                        img = Catalog.Product.Image(
                            image_name=image, last_run_time=self.last_run_date
                        )
                        if img.validate():
                            self.images.append(img)
                print(f"Total self.Images: {len(self.images)}")

            def get_bc_product_images(self):
                """Get BigCommerce image information for product's images"""
                url = (
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/"
                    f"catalog/products/{self.product_id}/images"
                )
                response = requests.get(url=url, headers=creds.test_bc_api_headers)
                if response is not None:
                    for x in response.json():
                        # Could use this to back-fill database with image id and sort order info
                        pass

            def construct_image_payload(self):
                result = []
                for image in self.images:
                    result.append(
                        {
                            "is_thumbnail": image.is_thumbnail,
                            "sort_order": image.sort_order,
                            "description": image.alt_text_1,
                            "image_url": f"{creds.public_web_dav_photos}/{image.image_name}",
                            "id": 0,
                            "product_id": self.product_id,
                            "date_modified": image.modified_date,
                        }
                    )
                return result

            @staticmethod
            def get_lst_maint_dt(file_path):
                return (
                    datetime.fromtimestamp(os.path.getmtime(file_path))
                    if os.path.exists(file_path)
                    else datetime(1970, 1, 1)
                )

        class Video:
            """Placeholder for video class"""

            pass

        class Modifier:
            """Placeholder for modifier class"""

            pass

        class Image:
            def __init__(
                self,
                image_name: str,
                last_run_time,
                sku="",
                image_url="",
                product_id=0,
                variant_id=0,
                image_id=0,
                is_thumbnail=False,
                sort_order=0,
                is_binding_image=False,
                is_binding_id=None,
                is_variant_image=False,
                description="",
                size=0,
            ):
                self.db = Database.db
                self.should_replace_image = False
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
                self.size = size
                self.last_maintained_dt = None
                self.get_image_details()
                # print(self)
                # print("-----------------")

            def __str__(self):
                result = ""
                for k, v in self.__dict__.items():
                    result += f"{k}: {v}\n"
                return result

            def get_image_details(self):
                print("\nEntering Get Image Details Function of Image Class")
                """Get image details from SQL"""
                print("\nGetting Image Details for ", self.image_name)
                print("-----------------")

                query = (
                    f"SELECT * FROM SN_IMAGES WHERE IMAGE_NAME = '{self.image_name}'"
                )
                print("Query: ", query)
                print()
                response = self.db.query_db(query)
                print("Response: ", response)
                print()
                if response is not None:
                    self.id = response[0][0]
                    self.image_name = response[0][1]
                    self.sku = response[0][2]
                    self.file_path = response[0][3]
                    self.image_url = response[0][4]
                    self.product_id = response[0][5]
                    self.image_id = response[0][6]
                    # self.is_thumbnail = True if response[0][7] == 1 else False
                    self.image_number = response[0][8]
                    # self.sort_order = response[0][9]
                    self.is_binding_image = True if response[0][10] == 1 else False
                    self.binding_id = response[0][11]
                    self.is_variant_image = True if response[0][12] == 1 else False
                    self.description = (
                        self.get_image_description()
                    )  # This will pull fresh data each sync.
                    self.size = response[0][14]
                    self.last_maintained_dt = response[0][15]

                    # size_check = os.path.getsize(self.file_path)

                    # if self.size != size_check:
                    #     print(f"\n\nUPLOADING NEW IMAGE FOR {self.image_name}\n\n")
                    #     self.image_url = self.upload_product_image()
                    #     print(f"new image url: {self.image_url}")
                    #     self.delete_image(self.image_name)
                    #     self.set_image_details()
                    # else:
                    #     print(f"Image {self.image_name} has not been modified.")
                else:
                    self.image_url = self.upload_product_image()
                    self.set_image_details()

                print("-----------------")

            def validate(self):
                """Images will be validated for size and format before being uploaded and written to middleware.
                Images that have been written to database previously will be considered valid and will pass."""
                if self.id:
                    # These items have already been through check before.
                    return True
                else:
                    # Check for valid file size/format
                    size = (1280, 1280)
                    q = 90
                    exif_orientation = 0x0112
                    if self.image_name.lower().endswith("jpg"):
                        # Resize files larger than 1.8 MB
                        if self.size > 1800000:
                            print(
                                f"Found large file {self.image_name}. Attempting to resize."
                            )
                            try:
                                im = Image.open(self.file_path)
                                im.thumbnail(size, Image.LANCZOS)
                                code = im.getexif().get(exif_orientation, 1)
                                if code and code != 1:
                                    im = ImageOps.exif_transpose(im)
                                im.save(self.file_path, "JPEG", quality=q)
                                im.close()
                                self.size = os.path.getsize(self.file_path)
                                print(f"{self.image_name} resized.")
                            except Exception as e:
                                print(f"Error resizing {self.image_name}: {e}")
                                return False
                            else:
                                print(f"Image {self.image_name} was resized.")

                    # Remove Alpha Layer and Convert PNG to JPG
                    if self.image_name.lower().endswith("png"):
                        print(
                            f"Found PNG file: {self.image_name}. Attempting to reformat."
                        )
                        try:
                            im = Image.open(self.file_path)
                            im.thumbnail(size, Image.LANCZOS)
                            # Preserve Rotational Data
                            code = im.getexif().get(exif_orientation, 1)
                            if code and code != 1:
                                im = ImageOps.exif_transpose(im)
                            print("Stripping Alpha Layer.")
                            rgb_im = im.convert("RGB")
                            print("Saving new file in JPG format.")
                            new_image_name = self.image_name.split(".")[0] + ".jpg"
                            new_file_path = f"{creds.photo_path}/{new_image_name}"
                            rgb_im.save(new_file_path, "JPEG", quality=q)
                            im.close()
                            print("Removing old PNG file")
                            os.remove(self.file_path)
                            self.file_path = new_file_path
                            self.image_name = new_image_name
                        except Exception as e:
                            print(f"Error converting {self.image_name}: {e}")
                            return False
                        else:
                            print("Conversion successful.")

                    # replace .JPEG with .JPG
                    if self.image_name.lower().endswith("jpeg"):
                        print("Found file ending with .JPEG. Attempting to reformat.")
                        try:
                            print(self.file_path)
                            im = Image.open(self.file_path)
                            im.thumbnail(size, Image.LANCZOS)
                            # Preserve Rotational Data
                            code = im.getexif().get(exif_orientation, 1)
                            if code and code != 1:
                                im = ImageOps.exif_transpose(im)
                            new_image_name = self.image_name.split(".")[0] + ".jpg"
                            new_file_path = f"{creds.photo_path}/{new_image_name}"
                            im.save(new_file_path, "JPEG", quality=q)
                            im.close()
                            os.remove(self.file_path)
                            self.file_path = new_file_path
                            self.image_name = new_image_name
                        except Exception as e:
                            print(f"Error converting {self.image_name}: {e}")
                            return False
                        else:
                            print("Conversion successful.")

                    # check for description that is too long
                    if len(self.description) >= 500:
                        print(
                            f"Description for {self.image_name} is too long. Validation failed."
                        )
                        return False

                    # Check for images with words or trailing numbers in the name
                    if (
                        "^" in self.image_name
                        and not self.image_name.split(".")[0].split("^")[1].isdigit()
                    ):
                        print(f"Image {self.image_name} is not valid.")
                        return False

                    # Valid Image
                    print(f"Image {self.image_name} is valid.")
                    return True

            def set_image_details(self):
                def get_item_no_from_image_name(image_name):
                    def get_binding_id(item_no):
                        query = f"""
                               SELECT USR_PROF_ALPHA_16 FROM IM_ITEM
                               WHERE ITEM_NO = '{item_no}'
                               """
                        response = self.db.query_db(query)
                        if response is not None:
                            return response[0][0] if response[0][0] else ""

                    # Check for binding image
                    if (
                        image_name.split(".")[0].split("^")[0]
                        in Catalog.all_binding_ids
                    ):
                        item_no = ""
                        binding_id = image_name.split(".")[0].split("^")[0]
                        self.is_binding_image = True
                    else:
                        item_no = image_name.split(".")[0].split("^")[0]
                        binding_id = get_binding_id(item_no)

                    return item_no, binding_id

                def get_image_number():
                    image_number = 1
                    if (
                        "^" in self.image_name
                        and self.image_name.split(".")[0].split("^")[1].isdigit()
                    ):
                        # secondary images
                        for x in range(1, 100):
                            if int(self.image_name.split(".")[0].split("^")[1]) == x:
                                image_number = x + 1
                                break
                    return image_number

                self.sku, self.binding_id = get_item_no_from_image_name(self.image_name)
                self.image_number = get_image_number()

                self.size = os.path.getsize(self.file_path)

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
                data = open(self.file_path, "rb")
                random_int = random.randint(1000, 9999)
                new_name = f"{self.image_name.split(".")[0].replace("^", "-")}-{random_int}.jpg"

                url = f"{creds.web_dav_product_photos}/{new_name}"
                try:
                    requests.put(
                        url,
                        data=data,
                        auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw),
                    )
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
                    im.save(self.file_path, "JPEG", quality=q)
                    print(f"Resized {self.image_name}")

            def bc_get_image(self):
                url = (
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/"
                    f"products/{self.product_id}/images/{self.image_id}"
                )
                response = requests.get(url=url, headers=creds.test_bc_api_headers)
                return response.content

            def bc_delete_image(self):
                """Photos can either be variant images or product images. Two flows in this function"""
                if self.is_variant_image:
                    url = (
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/"
                        f"products/{self.product_id}/variants/{self.variant_id}/images/{self.image_id}"
                    )
                    response = requests.delete(
                        url=url, headers=creds.test_bc_api_headers
                    )

                url = (
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/"
                    f"products/{self.product_id}/images/{self.image_id}"
                )
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
