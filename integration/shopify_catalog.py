import asyncio
import os
import shutil
import re
import json
from datetime import datetime

import time
import aiohttp

from integration.requests_handler import BCRequests
from integration.shopify_api import Shopify
import requests
from PIL import Image, ImageOps
from requests.auth import HTTPDigestAuth
import random

from integration.database import Database

from setup import creds
from setup import query_engine
from setup.utilities import get_all_binding_ids, convert_to_utc
from setup.utilities import VirtualRateLimiter

from setup.error_handler import ProcessOutErrorHandler


class Catalog:
    error_handler = ProcessOutErrorHandler.error_handler
    logger = error_handler.logger

    all_binding_ids = get_all_binding_ids()
    mw_brands = set()
    metafields = Database.Shopify.Metafield_Definition.get_all()

    def __init__(self, last_sync=datetime(1970, 1, 1)):
        self.last_sync = last_sync
        self.db = Database.db
        self.category_tree = self.CategoryTree(last_sync=last_sync)
        # Used to process preliminary deletions of products and images
        self.cp_items = []
        self.mw_items = []
        self.sync_queue = []
        self.binding_ids = set()
        if self.sync_queue:
            self.binding_ids = set(x['binding_id'] for x in self.sync_queue if 'binding_id' in x)

    def __str__(self):
        return f'Items to Process: {len(self.sync_queue)}\n'

    def get_products(self, test_mode=False):
        # Get data for self.cp_items and self.mw_items
        counterpoint_items = self.db.query_db("SELECT ITEM_NO FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'")
        self.cp_items = [x[0] for x in counterpoint_items] if counterpoint_items else []

        middleware_items = self.db.query_db(f'SELECT ITEM_NO FROM {creds.shopify_product_table}')
        self.mw_items = [x[0] for x in middleware_items] if middleware_items else []

        # Create the Sync Queue
        # ---------------------
        # Get all products that have been updated since the last sync

        query = f"""
        SELECT ITEM_NO, ITEM.{creds.cp_field_binding_id} as 'Binding ID'
        FROM IM_ITEM ITEM
        WHERE ITEM.LST_MAINT_DT > '{self.last_sync: %Y-%m-%d %H:%M:%S}' and
        ITEM.IS_ECOMM_ITEM = 'Y'
        ORDER BY {creds.cp_field_binding_id} DESC
        """
        response = self.db.query_db(query)
        print(response)
        if response is not None:
            result = []
            for item in response:
                sku = item[0]
                binding_id = item[1]
                queue_payload = {}
                if binding_id is not None:
                    # Check if the binding ID matches the correct format. (e.g. B0001)
                    pattern = creds.binding_id_format
                    if not bool(re.fullmatch(pattern, binding_id)):
                        message = f'Product {binding_id} has an invalid binding ID.'
                        Catalog.error_handler.add_error_v(error=message, origin='get_products()')
                        # Skip this product
                        continue

                    else:
                        # Get Parent to Process.
                        query = f"""
                        SELECT ITEM_NO
                        FROM IM_ITEM
                        WHERE {creds.cp_field_binding_id} = '{binding_id}' AND IS_ECOMM_ITEM = 'Y' AND IS_ADM_TKT = 'Y'"""

                        get_parent_response = self.db.query_db(query)

                        if get_parent_response is not None:
                            # Parent(s) found
                            parent_list = [x[0] for x in get_parent_response]
                            # If multiple parents are found, choose the lowest price parent.
                            if len(parent_list) > 1:
                                Catalog.logger.warn(f'Multiple parents found for {binding_id}.')
                                # Set Parent Status for new parent.
                                parent_sku = self.set_parent(binding_id=binding_id, remove_current=True)

                            else:
                                # Single Parent Found.
                                parent_sku = parent_list[0]
                        else:
                            # Missing Parent! Will choose the lowest price web enabled variant as the parent.
                            Catalog.logger.warn(f'Parent SKU not found for {binding_id}.')
                            parent_sku = self.set_parent(binding_id=binding_id)

                        queue_payload = {'sku': parent_sku, 'binding_id': binding_id}
                else:
                    # This will add single products to the queue
                    queue_payload = {'sku': sku}

                result.append(queue_payload)

            res = []
            [res.append(x) for x in result if x not in res]
            self.sync_queue = res
            if test_mode:
                self.sync_queue = {'sku': '10338', 'binding_id': 'B0001'}

            Catalog.logger.info(f'Sync Queue: {self.sync_queue}')

    def set_parent(self, binding_id, remove_current=False):
        # Get Family Members.
        family_members = Catalog.get_family_members(binding_id=binding_id, price=True)
        # Choose the lowest price family member as the parent.
        parent_sku = min(family_members, key=lambda x: x['price_1'])['sku']

        Catalog.logger.info(f'Family Members: {family_members}, Target new parent item: {parent_sku}')

        if remove_current:
            # Remove Parent Status from all children.
            remove_parent_query = f"""
                    UPDATE IM_ITEM 
                    SET IS_ADM_TKT = 'N', LST_MAINT_DT = GETDATE()
                    WHERE {creds.cp_field_binding_id} = '{binding_id}'
                    """
            remove_parent_response = self.db.query_db(remove_parent_query, commit=True)
            if remove_parent_response['code'] == 200:
                Catalog.logger.success(f'Parent status removed from all children of binding: {binding_id}.')
            else:
                Catalog.error_handler.add_error_v(
                    error=f'Error removing parent status from children of binding: {binding_id}. Response: {remove_parent_response}'
                )

        # Set Parent Status for new parent.
        query = f"""
        UPDATE IM_ITEM
        SET IS_ADM_TKT = 'Y'
        WHERE ITEM_NO = '{parent_sku}'
        """
        set_parent_response = self.db.query_db(query, commit=True)

        if set_parent_response['code'] == 200:
            Catalog.logger.success(f'Parent status set for {parent_sku}')
        else:
            Catalog.error_handler.add_error_v(
                error=f'Error setting parent status for {parent_sku}. Response {set_parent_response}'
            )

        return parent_sku

    def process_product_deletes(self):
        # This compares the CP and MW product lists and deletes any products that are not in both lists.
        Catalog.logger.info('Processing Product Deletions.')

        delete_targets = Catalog.get_deletion_target(secondary_source=self.mw_items, primary_source=self.cp_items)
        add_targets = []

        for item in self.sync_queue:
            if 'binding_id' not in item:
                # Check if the target product has a binding ID in the middleware database.
                mw_binding_id = Catalog.get_binding_id_from_sku(item['sku'], middleware=True)
                if mw_binding_id:
                    # This is a former bound product. Delete it.
                    delete_targets.append(item['sku'])
            else:
                # These products have a binding ID. Get all family members of the binding ID.
                family_members = Catalog.get_family_members(binding_id=item['binding_id'], counterpoint=True)

                for member in family_members:
                    query = f"""
                    SELECT ID, BINDING_ID
                    FROM {creds.shopify_product_table}
                    WHERE ITEM_NO = '{member}'
                    """
                    response = self.db.query_db(query)

                    if response is not None:
                        exists_in_mw = True if response[0][0] else False
                        member_mw_binding_id = response[0][1]
                    else:
                        exists_in_mw = False
                        member_mw_binding_id = None

                    if exists_in_mw and member_mw_binding_id != item['binding_id']:
                        delete_targets.append(member)
                        add_targets.append({'parent': item['sku'], 'variant': member})
                    elif not exists_in_mw:
                        add_targets.append({'parent': item['sku'], 'variant': member})

        if delete_targets:
            Catalog.logger.info(f'Product Delete Targets: {delete_targets}')
            for x in delete_targets:
                self.delete_product(sku=x)
        else:
            Catalog.logger.info('No products to delete.')
        time.sleep(2)

        Catalog.logger.info('Processing Product Additions.')
        if add_targets:
            Catalog.logger.info(f'Product Add Targets: {add_targets}')
            for x in add_targets:
                parent_sku = x['parent']
                variant_sku = x['variant']
                # Get Product ID associated with item.
                product_id = Catalog.get_product_id_from_sku(parent_sku)

                if product_id is not None:
                    variant = Catalog.Product.Variant(sku=variant_sku, last_run_date=self.last_sync)
                    print(f'\n\nPosting new variant for {variant_sku} to product ID {product_id}.\n\n')
                    variant.bc_post_variant(product_id=product_id)
        else:
            Catalog.logger.info('No products to add.')

    def process_images(self):
        """Assesses Image folder. Deletes images from MW and BC. Updates LST_MAINT_DT in CP if new images have been added."""

        def get_local_images():
            """Get a tuple of two sets:
            1. all SKUs that have had their photo modified since the input date.
            2. all file names that have been modified since the input date."""

            all_files = []
            # Iterate over all files in the directory
            for filename in os.listdir(creds.photo_path):
                if filename not in ['Thumbs.db', 'desktop.ini', '.DS_Store']:
                    # filter out trailing filenames
                    if '^' in filename:
                        if filename.split('.')[0].split('^')[1].isdigit():
                            all_files.append([filename, os.path.getsize(f'{creds.photo_path}/{filename}')])
                    else:
                        all_files.append([filename, os.path.getsize(f'{creds.photo_path}/{filename}')])

            return all_files

        def get_middleware_images():
            query = f'SELECT IMAGE_NAME, SIZE FROM {creds.shopify_image_table}'
            response = self.db.query_db(query)
            return [[x[0], x[1]] for x in response] if response else []

        def delete_image(image_name) -> bool:
            """Takes in an image name and looks for matching image file in middleware. If found, deletes from BC and SQL."""
            Catalog.logger.info(f'Deleting {image_name}')
            image_query = f"SELECT PRODUCT_ID, IMAGE_ID, IS_VARIANT_IMAGE, IMAGE_URL FROM {creds.shopify_image_table} WHERE IMAGE_NAME = '{image_name}'"
            img_id_res = self.db.query_db(image_query)
            if img_id_res is not None:
                product_id, image_id, is_variant, image_url = (
                    img_id_res[0][0],
                    img_id_res[0][1],
                    img_id_res[0][2],
                    img_id_res[0][3],
                )

            if is_variant == 1:
                # Get Variant ID
                item_number = image_name.split('.')[0].split('^')[0]
                variant_query = (
                    f"SELECT VARIANT_ID FROM {creds.shopify_product_table} WHERE ITEM_NO = '{item_number}'"
                )
                variant_id_res = self.db.query_db(variant_query)
                if variant_id_res is not None:
                    variant_id = variant_id_res[0][0]
                else:
                    Catalog.logger.warn(f'Variant ID not found for {image_name}. Response: {variant_id_res}')

                if variant_id is not None:
                    url = (
                        f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
                        f'products/{product_id}/variants/{variant_id}/images/'
                    )
                    response = BCRequests.post(url=url, json={'image_url': ''})
                    if response.status_code == 200:
                        Catalog.logger.success(f'Primary Variant Image {image_name} deleted from BigCommerce.')
                    else:
                        Catalog.error_handler.add_error_v(
                            error=f'Error deleting Primary Variant Image {image_name} from BigCommerce. {response.json()}',
                            origin='process_images() -> delete_image()',
                        )
                        Catalog.logger.warn(f'Error deleting primary variant image: {response.json()}')

            delete_img_url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images/{image_id}'

            img_del_res = BCRequests.delete(url=delete_img_url)

            if img_del_res.status_code == 204:
                Catalog.logger.success(f'Image {image_name} deleted from BigCommerce.')
            else:
                Catalog.error_handler.add_error_v(
                    error=f'Error deleting image {image_name} from BigCommerce.', origin='Catalog.delete_image()'
                )
            delete_images_query = f'DELETE FROM {creds.shopify_image_table} ' f"WHERE IMAGE_ID = '{image_id}'"
            response = self.db.query_db(delete_images_query, commit=True)
            if response['code'] == 200:
                Catalog.logger.success(f'Image {image_name} deleted from SQL.')
            else:
                Catalog.error_handler.add_error_v(
                    error=f'Error deleting image {image_name} from SQL.', origin='Catalog.delete_image()'
                )
            # Delete image from WebDav directory
            url_filename = image_url.split('/')[-1]
            print(f'Deleting {url_filename}')
            web_dav_response = Catalog.delete_image_from_webdav(url_filename)
            if web_dav_response.status_code == 204:
                Catalog.logger.success(f'Image {image_name} deleted from WebDav.')
            else:
                Catalog.error_handler.add_error_v(
                    error=f'Error deleting image {image_name} from WebDav. {web_dav_response.text}',
                    origin='Catalog.delete_image()',
                )

        Catalog.logger.info('Processing Image Updates.')
        start_time = time.time()
        local_images = get_local_images()
        mw_image_list = get_middleware_images()

        delete_targets = Catalog.get_deletion_target(primary_source=local_images, secondary_source=mw_image_list)

        if delete_targets:
            Catalog.logger.info(message=f'Delete Targets: {delete_targets}')
            for x in delete_targets:
                Catalog.logger.info(f'Deleting Image {x[0]}.\n')
                Catalog.logger.warn('Skipping image deletion during testing.')
                # delete_image(x[0])
        else:
            Catalog.logger.info('No image deletions found.')

        update_list = delete_targets

        addition_targets = Catalog.get_deletion_target(primary_source=mw_image_list, secondary_source=local_images)

        if addition_targets:
            for x in addition_targets:
                update_list.append(x)
        else:
            Catalog.logger.info('No image additions found.')

        if update_list:
            sku_list = [x[0].split('.')[0].split('^')[0] for x in update_list]
            binding_list = [x for x in sku_list if x in Catalog.all_binding_ids]

            sku_list = tuple(sku_list)
            if binding_list:
                if len(binding_list) > 1:
                    binding_list = tuple(binding_list)
                    where_filter = f' or {creds.cp_field_binding_id} in {binding_list}'
                else:
                    where_filter = f" or {creds.cp_field_binding_id} = '{binding_list[0]}'"
            else:
                where_filter = ''

            query = (
                'UPDATE IM_ITEM '
                'SET LST_MAINT_DT = GETDATE() '
                f"WHERE (ITEM_NO in {sku_list} {where_filter}) and IS_ECOMM_ITEM = 'Y'"
            )

            self.db.query_db(query, commit=True)

        Catalog.logger.info(f'Image Add/Delete Processing Complete. Time: {time.time() - start_time}')

    def sync(self, initial=False):
        # # Sync Category Tree
        # self.category_tree.sync()

        # if not initial:
        #     # Process Product Deletions and Images
        #     self.process_product_deletes()
        #     self.process_images()

        # Sync Products
        # self.get_products()  # Get all products that have been updated since the last sync
        self.sync_queue = [{'sku': '10344', 'binding_id': 'B0001'}]

        # Small Bound Product
        # self.sync_queue = [{'sku': 'SYUFICG01', 'binding_id': 'B0050'}]

        # single product
        # self.sync_queue = [{'sku': '201376'}]
        if not self.sync_queue:
            Catalog.logger.success('No products to sync.')
        else:
            queue_length = len(self.sync_queue)
            success_count = 0
            fail_count = 0

            Catalog.logger.info(f'Syncing {queue_length} products.')
            while len(self.sync_queue) > 0:
                start_time = time.time()
                target = self.sync_queue.pop()
                prod = self.Product(target, last_sync=self.last_sync)
                prod.get_product_details(last_sync=self.last_sync)
                Catalog.logger.info(
                    f'Processing Product: {prod.sku}, Binding: {prod.binding_id}, Title: {prod.web_title}'
                )
                if prod.validate_inputs():
                    prod.process()
                    success_count += 1
                else:
                    fail_count += 1

                queue_length -= 1
                Catalog.logger.info(
                    f'Product {prod.sku} processed in {time.time() - start_time} seconds. Products Remaining: {queue_length}\n\n'
                )

            Catalog.logger.info(
                '-----------------------\n'
                'Sync Complete.\n'
                f'Success Count: {success_count}\n'
                f'Fail Count: {fail_count}\n'
            )

    def delete_product(self, sku, update_timestamp=False):
        delete_payload = {'sku': sku}
        binding_id = Catalog.get_binding_id_from_sku(sku, middleware=True)
        if binding_id is not None:
            delete_payload['binding_id'] = binding_id
        else:
            Catalog.logger.warn(f'Binding ID not found for {sku}.')

        product = Catalog.Product(product_data=delete_payload, last_sync=self.last_sync)

        if binding_id:
            Catalog.logger.info(f'Deleting Product: {sku} with Binding ID: {binding_id}')
            product.delete_variant(sku=sku, binding_id=binding_id)
        else:
            Catalog.logger.info(f'Deleting Product: {sku}')
            Database.Shopify.Product.delete(sku=sku)

        if update_timestamp:
            Catalog.update_timestamp(sku=sku)

    @staticmethod
    def parse_custom_url_string(string: str):
        """Uses regular expression to parse a string into a URL-friendly format."""
        return '-'.join(str(re.sub('[^A-Za-z0-9 ]+', '', string)).lower().split(' '))

    @staticmethod
    def update_timestamp(sku):
        """Updates the LST_MAINT_DT field in Counterpoint for a given SKU."""
        query = f"""
        UPDATE IM_ITEM
        SET LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{sku}'
        """
        response = Database.db.query_db(query, commit=True)
        if response['code'] == 200:
            Catalog.logger.success(f'Timestamp updated for {sku}.')
        else:
            Catalog.error_handler.add_error_v(error=f'Error updating timestamp for {sku}. Response: {response}')

    @staticmethod
    def get_product(item_no):
        query = f"SELECT ITEM_NO, {creds.cp_field_binding_id} FROM IM_ITEM WHERE ITEM_NO = '{item_no}'"
        response = Database.db.query_db(query)
        if response is not None:
            sku = response[0][0]
            binding_id = response[0][1]
        if binding_id:
            return {'sku': sku, 'binding_id': binding_id}
        else:
            return {'sku': sku}

    @staticmethod
    def get_family_members(binding_id, count=False, price=False, counterpoint=False):
        db = Database.db
        """Get all items associated with a binding_id. If count is True, return the count."""
        # return a count of items in family
        if count:
            query = f"""
            SELECT COUNT(ITEM_NO)
            FROM {creds.shopify_product_table}
            WHERE BINDING_ID = '{binding_id}'
            """
            response = db.query_db(query)
            return response[0][0]

        else:
            if price:
                # include retail price for each item
                query = f"""
                SELECT ITEM_NO, PRC_1
                FROM IM_ITEM
                WHERE {creds.cp_field_binding_id} = '{binding_id}'
                """
                response = db.query_db(query)
                if response is not None:
                    return [{'sku': x[0], 'price_1': float(x[1])} for x in response]

            elif counterpoint:
                query = f"""
                SELECT ITEM_NO
                FROM IM_ITEM
                WHERE {creds.cp_field_binding_id} = '{binding_id}' and IS_ECOMM_ITEM = 'Y'
                """
                response = db.query_db(query)
                if response is not None:
                    return [x[0] for x in response]

            else:
                query = f"""
                SELECT ITEM_NO
                FROM {creds.shopify_product_table}
                WHERE BINDING_ID = '{binding_id}'
                """
                response = db.query_db(query)
                if response is not None:
                    return [x[0] for x in response]

    @staticmethod
    def get_binding_id_from_sku(sku, middleware=False):
        if middleware:
            query = f"""
            SELECT BINDING_ID
            FROM {creds.shopify_product_table}
            WHERE ITEM_NO = '{sku}'
            """
        else:
            query = f"""
            SELECT {creds.cp_field_binding_id}
            FROM IM_ITEM
            WHERE ITEM_NO = '{sku}'
            """
        response = Database.db.query_db(query)
        if response is not None:
            return response[0][0]

    @staticmethod
    def get_product_id_from_sku(sku):
        query = f"SELECT PRODUCT_ID FROM {creds.shopify_product_table} WHERE ITEM_NO = '{sku}'"
        response = Database.db.query_db(query)
        if response is not None:
            return response[0][0]

    @staticmethod
    def get_filesize(filepath):
        try:
            file_size = os.path.getsize(filepath)
        except FileNotFoundError:
            return None
        else:
            return file_size

    @staticmethod
    def delete_image_from_webdav(image_name):
        url = f'{creds.web_dav_product_photos}/{image_name}'
        response = requests.delete(url, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw))
        return response

    @staticmethod
    def get_deletion_target(primary_source, secondary_source):
        return [element for element in secondary_source if element not in primary_source]

    @staticmethod
    def delete_categories():
        # Get all categories from Middleware. Delete from Shopify and Middleware.
        query = f'SELECT DISTINCT COLLECTION_ID FROM {creds.shopify_category_table}'
        response = Database.db.query_db(query)
        parent_category_list = [x[0] for x in response] if response else []
        while parent_category_list:
            target = parent_category_list.pop()
            Shopify.Collection.delete(collection_id=target)
            Database.Shopify.Collection.delete(collection_id=target)

        # Check for any remaining categories in Shopify
        response = Shopify.Collection.get_all()
        if response:
            for collection in response:
                Shopify.Collection.delete(collection)

    @staticmethod
    def delete_products():
        """Deletes all products from BigCommerce and Middleware."""
        # Get all product IDs from Middleware
        query = f'SELECT DISTINCT PRODUCT_ID FROM {creds.shopify_product_table}'
        response = Database.db.query_db(query)
        product_id_list = [x[0] for x in response] if response else []

        while product_id_list:
            target = product_id_list.pop()
            try:
                Shopify.Product.delete(product_id=target)
            except Exception as e:
                Catalog.error_handler.add_error_v(
                    error=f'Error deleting product {target}. {e}', origin='delete_products()'
                )
            Database.Shopify.Product.delete(product_id=target)

        # Check for any remaining products in Shopify
        response = Shopify.Product.get_all()
        if response:
            for product in response:
                Shopify.Product.delete(product)

    @staticmethod
    def delete_catalog():
        """Deletes all products, categories, and brands from BigCommerce and Middleware."""
        Catalog.delete_products()
        Catalog.delete_categories()

    class CategoryTree:
        def __init__(self, last_sync):
            self.db = Database.db
            self.last_sync = last_sync
            self.categories = set()
            self.heads = []
            self.get_tree()

        def __str__(self):
            def print_category_tree(category, level=0):
                # Print the category id and name, indented by the category's level in the tree
                res = (
                    f"{'    ' * level}Category Name: {category.category_name}\n"
                    f"{'    ' * level}---------------------------------------\n"
                    f"{'    ' * level}Counterpoint Category ID: {category.cp_categ_id}\n"
                    f"{'    ' * level}Counterpoint Parent ID: {category.cp_parent_id}\n"
                    f"{'    ' * level}Shopify Collection ID: {category.bc_categ_id}\n"
                    f"{'    ' * level}BigCommerce Parent ID: {category.bc_parent_id}\n"
                    f"{'    ' * level}Sort Order: {category.sort_order}\n"
                    f"{'    ' * level}Last Maintenance Date: {category.lst_maint_dt}\n\n"
                )

                # Recursively call this function for each child category
                for child in category.children:
                    res += print_category_tree(child, level + 1)
                return res

            # Use the helper function to print the entire tree
            result = ''
            for root in self.heads:
                result += print_category_tree(root)

            return result

        def get_tree(self):
            """Updates middleware category tables with Counterpoint category information."""
            query = f"""
            SELECT cp.CATEG_ID, ISNULL(cp.PARENT_ID, 0), mw.COLLECTION_ID, mw.MENU_ID, cp.DESCR, cp.DISP_SEQ_NO, cp.HTML_DESCR, 
            cp.LST_MAINT_DT, mw.CP_CATEG_ID, mw.is_visible, mw.IMG_SIZE
            FROM EC_CATEG cp
            FULL OUTER JOIN {creds.shopify_category_table} mw on cp.CATEG_ID=mw.CP_CATEG_ID
            WHERE cp.CATEG_ID != '0'
            """
            response = self.db.query_db(query)
            if response:
                for x in response:
                    cp_categ_id = x[0]
                    mw_cp_categ_id = x[8]  # Middleware Counterpoint Category ID

                    if cp_categ_id is None:  # These are categories that exist in the middleware but not CP
                        self.delete_category(mw_cp_categ_id)
                        continue  # Skip deleted categories

                    # Create Category Object
                    cat = self.Category(
                        cp_categ_id=cp_categ_id,
                        cp_parent_id=x[1],
                        collection_id=x[2],
                        menu_id=x[3],
                        name=x[4],
                        sort_order=x[5],
                        description=x[6].replace("'", "''"),
                        lst_maint_dt=x[7],
                        is_visible=True if x[9] == 1 else False,
                        image_size=x[10] if x[10] else None,
                    )

                    if mw_cp_categ_id is None:
                        Database.Shopify.Collection.insert(cat)

                    else:
                        if cat.lst_maint_dt > self.last_sync:
                            Database.Shopify.Collection.update(cat)

                    self.categories.add(cat)

                for x in self.categories:
                    for y in self.categories:
                        if y.cp_parent_id == x.cp_categ_id:
                            x.add_child(y)

                self.heads = [x for x in self.categories if x.cp_parent_id == '0']
                # Sort Heads by x.sort_order
                self.heads.sort(key=lambda x: x.sort_order)

        def sync(self):
            self.process_collections()
            self.process_menus()
            self.update_tree_in_middleware()

        def process_collections(self):
            """Recursively updates collections in Shopify."""
            # Establish queue for dealing with category images
            queue = []

            def collections_h(category):
                # Get BC Category ID and Parent ID
                print(f'Processing Category: {category.name}, category collection ID: {category.collection_id}')
                queue.append(category)
                if category.collection_id is None:
                    category.collection_id = Shopify.Collection.create(category.get_category_payload())
                for child in category.children:
                    collections_h(child)

            for category in self.heads:
                if category.lst_maint_dt > self.last_sync:
                    collections_h(category)

            self.upload_category_images(queue)
            for category in queue:
                Shopify.Collection.update(category.get_category_payload())

        def process_menus(self):
            """Recursively updates menus in Shopify."""
            main_menu = {
                'id': f'gid://shopify/Menu/{creds.shopify_main_menu_id}',
                'title': 'Main Menu',
                'handle': 'main-menu',
                'items': [],
            }

            def menu_helper(category):
                # Sort category children by sort order
                category.children.sort(key=lambda x: x.sort_order)

                menu_item = {
                    'title': category.name,
                    'type': 'COLLECTION',
                    'resourceId': f'gid://shopify/Collection/{category.collection_id}',
                    'items': [menu_helper(child) for child in category.children],
                }

                # if category.menu_id is not None:
                #     menu_item['id'] = f'gid://shopify/MenuItem/{category.menu_id}'

                return menu_item

            # Recursively call this function for each child category
            for category in self.heads:
                main_menu['items'].append(menu_helper(category))

            # Add the Landing Page to the Main Menu
            main_menu['items'].append(
                {
                    'title': 'Landscape Design',
                    'type': 'PAGE',
                    'resourceId': 'gid://shopify/Page/138277978406',
                    'items': [],
                }
            )

            Shopify.Menu.update(main_menu)
            response = Shopify.Menu.get(creds.shopify_main_menu_id)
            heads = response['menu']['items']
            result = []

            def response_helper(menu_item_list):
                menu_item_id = menu_item_list['id'].split('/')[-1]
                result.append({'menu_id': menu_item_id, 'title': menu_item_list['title']})
                if 'items' in menu_item_list:
                    for i in menu_item_list['items']:
                        response_helper(i)

            for head in heads:
                response_helper(head)

            # Assign menu item IDs to categories
            def assign_menu_ids(category):
                for i in result:
                    if i['title'] == category.name:
                        category.menu_id = i['menu_id']
                for child in category.children:
                    assign_menu_ids(child)

            for category in self.heads:
                assign_menu_ids(category)

        def update_tree_in_middleware(self):
            # Update Entire Category Tree in Middleware
            def update_helper(category):
                if category.lst_maint_dt > self.last_sync:
                    Database.Shopify.Collection.update(category)
                for child in category.children:
                    update_helper(child)

            for category in self.heads:
                update_helper(category)

        def upload_category_images(self, queue):
            file_list = []
            stagedUploadsCreateVariables = {'input': []}

            for category in queue:
                if category.image_path:
                    image_size = Catalog.get_filesize(category.image_path)
                    print(f'\n\nImage Size: {image_size}, Category Image Size: {category.image_size}\n\n')
                    if image_size != category.image_size:
                        category.image_size = image_size
                        file_list.append(category.image_path)
                        stagedUploadsCreateVariables['input'].append(
                            {
                                'filename': category.image_name,
                                'mimeType': 'image/jpg',
                                'httpMethod': 'POST',
                                'resource': 'COLLECTION_IMAGE',
                            }
                        )

            if file_list:
                uploaded_files = Shopify.Collection.Files.create(
                    variables=stagedUploadsCreateVariables, file_list=file_list
                )
                for file in uploaded_files:
                    for category in queue:
                        if file['file_path'] == category.image_path:
                            category_image_url = file['url']
                            print(f'Updating Image URL for {category.image_name} to {category_image_url}')
                            category.image_url = category_image_url

        def delete_category(self, cp_categ_id):
            query = f"""
            SELECT COLLECTION_ID
            FROM {creds.shopify_category_table}
            WHERE CP_CATEG_ID = {cp_categ_id}
            """
            response = self.db.query_db(query)
            if response:
                collection_id = response[0][0] if response and response[0][0] is not None else None
                if collection_id:
                    # Delete Category from Shopify
                    Shopify.Collection.delete(collection_id)
                    Database.Shopify.Collection.delete(cp_categ_id)
                else:
                    Database.Shopify.Collection.delete(cp_categ_id=cp_categ_id)

        class Category:
            def __init__(
                self,
                cp_categ_id,
                cp_parent_id,
                name,
                collection_id=None,
                menu_id=None,
                shopify_parent_id=None,
                sort_order=0,
                description='',
                is_visible=True,
                image_url=None,
                image_size=None,
                image_alt_text=None,
                lst_maint_dt=datetime(1970, 1, 1),
            ):
                # Category Properties
                self.cp_categ_id = cp_categ_id
                self.cp_parent_id = cp_parent_id
                self.name = name
                self.handle = self.get_full_custom_url_path()
                self.collection_id = collection_id
                self.menu_id = menu_id
                self.shopify_parent_id = shopify_parent_id
                self.sort_order = sort_order
                self.description = description
                self.image_size = image_size
                self.image_name = str(self.handle)[1:-1].replace('/', '_').replace(' ', '-') + '.jpg'
                self.image_path = self.get_category_image_path()
                self.image_url = image_url
                self.image_alt_text = image_alt_text
                self.is_visible = is_visible
                self.rule_set = None  # for future use
                self.lst_maint_dt = lst_maint_dt
                self.children = []

            def __str__(self):
                return (
                    f'Category Name: {self.name}\n'
                    f'---------------------------------------\n'
                    f'Counterpoint Category ID: {self.cp_categ_id}\n'
                    f'Counterpoint Parent ID: {self.cp_parent_id}\n'
                    f'BigCommerce Category ID: {self.collection_id}\n'
                    f'BigCommerce Parent ID: {self.shopify_parent_id}\n'
                    f'Sort Order: {self.sort_order}\n'
                    f'Last Maintenance Date: {self.lst_maint_dt}\n\n'
                )

            def add_child(self, child):
                self.children.append(child)

            def get_shopify_cat_id(self):
                query = f"""
                SELECT COLLECTION_ID
                FROM {creds.shopify_category_table}
                WHERE CP_CATEG_ID = {self.cp_categ_id}
                """
                response = query_engine.QueryEngine().query_db(query)
                if response is not None:
                    shopify_category_id = response[0][0] if response[0][0] is not None else None
                    print(f'Category ID: {shopify_category_id}')
                    if shopify_category_id is not None:
                        self.collection_id = response[0][0]
                    else:
                        self.shopify_parent_id = self.get_shopify_parent_id()
                        category_payload = self.get_category_payload()
                        self.collection_id = Shopify.Collection.create(category_payload)
                        print(f'Category ID: {self.collection_id}')

            def get_shopify_parent_id(self):
                query = f"""
                SELECT COLLECTION_ID
                FROM {creds.shopify_category_table}
                WHERE CP_CATEG_ID = (SELECT CP_PARENT_ID 
                                    FROM {creds.shopify_category_table} 
                                    WHERE CP_CATEG_ID = {self.cp_categ_id})
                """
                response = query_engine.QueryEngine().query_db(query)
                return response[0][0] if response and response[0][0] is not None else 0

            def get_full_custom_url_path(self):
                parent_id = self.cp_parent_id
                url_path = []
                url_path.append(Catalog.parse_custom_url_string(self.name))
                while parent_id != 0:
                    query = f'SELECT CATEG_NAME, CP_PARENT_ID FROM {creds.shopify_category_table} WHERE CP_CATEG_ID = {parent_id}'
                    response = query_engine.QueryEngine().query_db(query)
                    if response:
                        url_path.append(Catalog.parse_custom_url_string(response[0][0] or ''))
                        parent_id = response[0][1]
                    else:
                        break
                return f"/{"/".join(url_path[::-1])}/"

            def get_category_image_path(self, local=True):
                image_name = str(self.handle)[1:-1].replace('/', '_').replace(' ', '-') + '.jpg'
                local_path = f'{creds.public_files}/{creds.category_images}/{image_name}'
                if os.path.exists(local_path):
                    return local_path

            def get_category_payload(self):
                payload = {'input': {'title': self.name, 'handle': self.handle, 'sortOrder': 'BEST_SELLING'}}
                # New items - Add store channel
                if not self.collection_id:
                    payload['input']['publications'] = {'publicationId': creds.shopify_online_store_channel_id}
                # Updates - Add Collection ID
                if self.collection_id:
                    payload['input']['id'] = f'gid://shopify/Collection/{self.collection_id}'
                if self.description:
                    payload['input']['descriptionHtml'] = self.description
                if self.image_url:
                    payload['input']['image'] = {'src': self.image_url}
                    # if self.image_alt_text:
                    #     payload['input']['image']['altText'] = self.image_alt_text
                # for future use
                if self.rule_set:
                    payload['input']['ruleSet'] = {
                        'appliedDisjunctively': False,
                        'rules': [{{'column': 'TAG', 'relation': 'EQUALS', 'condition': 'summer'}}],
                    }
                return payload

    class Product:
        def __init__(self, product_data, last_sync):
            self.db = Database.db

            self.sku = product_data['sku']
            self.binding_id = product_data['binding_id'] if 'binding_id' in product_data else None
            # Will be set to True if product gets a success response from BigCommerce API on POST or PUT
            self.is_uploaded = False

            self.last_sync = last_sync

            # Determine if Bound
            self.is_bound = True if self.binding_id else False

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
            self.option_id = None
            self.web_title: str = ''
            self.long_descr = ''
            self.default_price = 0.0
            self.cost = 0.0
            self.sale_price = 0.0
            self.weight = 0.1
            self.width = 0.1
            self.height = 0.1
            self.depth = 0.1
            self.buffered_quantity = 0
            self.is_price_hidden = False
            self.brand = ''
            self.html_description = ''
            self.search_keywords = ''
            self.meta_title = ''
            self.meta_description = ''
            self.visible: bool = False
            self.featured: bool = False
            self.sort_order = 0
            self.gift_wrap: bool = False
            self.in_store_only: bool = False
            self.is_preorder = False
            self.is_preorder_only = False
            self.preorder_message = ''
            self.preorder_release_date = None
            self.is_free_shipping = False
            self.alt_text_1 = ''
            self.alt_text_2 = ''
            self.alt_text_3 = ''
            self.alt_text_4 = ''
            self.custom_url = None

            # Custom Fields
            self.custom_botanical_name = {'id': None, 'value': None}
            self.custom_climate_zone = {'id': None, 'value': None}
            self.custom_plant_type = {'id': None, 'value': None}
            self.custom_type = {'id': None, 'value': None}
            self.custom_height = {'id': None, 'value': None}
            self.custom_width = {'id': None, 'value': None}
            self.custom_sun_exposure = {'id': None, 'value': None}
            self.custom_bloom_time = {'id': None, 'value': None}
            self.custom_bloom_color = {'id': None, 'value': None}
            self.custom_attracts_pollinators = {'id': None, 'value': None}
            self.custom_growth_rate = {'id': None, 'value': None}
            self.custom_deer_resistant = {'id': None, 'value': None}
            self.custom_soil_type = {'id': None, 'value': None}
            self.custom_color = {'id': None, 'value': None}
            self.custom_size = {'id': None, 'value': None}

            self.lst_maint_dt = datetime(1970, 1, 1)

            # E-Commerce Categories
            self.cp_ecommerce_categories = []
            self.shopify_collections = []

            # Property Getter

            # Validate Product
            self.validation_retries = 10

        def __str__(self):
            result = ''
            line = '-' * 25 + '\n\n'
            result += line
            result += f'Printing Product Details for: {self.web_title}\n'
            for k, v in self.__dict__.items():
                result += f'{k}: {v}\n'
            result += line
            if len(self.variants) > 1:
                result += 'Printing Child Product Details\n'
                variant_index = 1
                for variant in self.variants:
                    result += f'Variant: {variant_index}\n'
                    result += line
                    for k, v in variant.__dict__.items():
                        result += f'    {k}: {v}\n'
                    for image in variant.images:
                        result += f'Image: {image.image_name}\n'
                        result += f'    Thumbnail: {image.is_thumbnail}\n'
                        result += f'    Variant Image: {image.is_variant_image}\n'
                        result += f'    Sort Order: {image.sort_order}\n'
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
                WHERE {creds.cp_field_binding_id} = '{self.binding_id}' and IS_ECOMM_ITEM = 'Y'
                ORDER BY PRC_1
                """
                # Get children and append to child list in order of price
                response = self.db.query_db(query)
                if response is not None:
                    # Create Product objects for each child and add object to bound parent list
                    for item in response:
                        variant = self.Variant(item[0], last_run_date=last_sync)
                        self.variants.append(variant)

                # Sort self.variants by variant.is_parent so parent is processed first.
                self.variants.sort(key=lambda x: x.is_parent, reverse=True)

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
                        self.custom_attracts_pollinators = bound.custom_attracts_pollinators
                        self.custom_growth_rate = bound.custom_growth_rate
                        self.custom_deer_resistant = bound.custom_deer_resistant
                        self.custom_soil_type = bound.custom_soil_type
                        self.custom_color = bound.custom_color
                        self.custom_size = bound.custom_size
                        self.cp_ecommerce_categories = bound.cp_ecommerce_categories
                        self.custom_url = bound.custom_url
                        self.long_descr = bound.long_descr
                        self.is_preorder = bound.is_preorder
                        self.preorder_release_date = bound.preorder_release_date
                        self.preorder_message = bound.preorder_message

                def get_binding_id_images():
                    binding_images = []
                    photo_path = creds.photo_path
                    list_of_files = os.listdir(photo_path)
                    if list_of_files is not None:
                        for file in list_of_files:
                            if file.split('.')[0].split('^')[0].lower() == self.binding_id.lower():
                                binding_images.append(file)

                    total_binding_images = len(binding_images)

                    if total_binding_images > 0:
                        # print(f"Found {total_binding_images} binding images for Binding ID: {self.binding_id}")
                        for image in binding_images:
                            binding_img = self.Image(image)

                            if binding_img.validate():
                                self.images.append(binding_img)
                            else:
                                Catalog.error_handler.add_error_v(
                                    error=f'Image {binding_img.image_name} failed validation. Image will not be added to product.',
                                    origin='Image Validation',
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
                self.custom_field_ids = single.custom_field_ids
                # Set the product last maintained date to the single product's last maintained date
                self.lst_maint_dt = single.lst_maint_dt
                self.long_descr = single.long_descr
                self.is_preorder = single.is_preorder
                self.preorder_release_date = single.preorder_release_date
                self.preorder_message = single.preorder_message

            if self.is_bound:
                get_bound_product_details()
            else:
                get_single_product_details()

            self.shopify_collections = self.get_shopify_collections()

            # Now all images are in self.images list and are in order by binding img first then variant img

        def validate_inputs(self):
            """Validate product inputs to check for errors in user input"""
            check_web_title = True
            check_for_missing_categories = False
            check_html_description = False
            min_description_length = 20
            check_missing_images = True
            check_for_invalid_brand = False
            check_for_item_cost = False

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
                Catalog.logger.info(f'Parent status set to {flag} for {target_item}')
                return self.get_product_details(last_sync=self.last_sync)

            if self.is_bound:
                # Test for missing variant names
                for child in self.variants:
                    if child.variant_name == '':
                        message = f'Product {child.sku} is missing a variant name. Validation failed.'
                        Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                        return False

                # Check for duplicate variant names
                variant_names = [x.variant_name for x in self.variants]
                if len(variant_names) != len(set(variant_names)):
                    message = f'Product {self.binding_id} has duplicate variant names. Validation failed.'
                    Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False

            # ALL PRODUCTS
            if check_web_title:
                # Test for missing web title
                if self.web_title is None or self.web_title == '':
                    if self.long_descr is None or self.long_descr == '':
                        message = f'Product {self.binding_id} is missing a web title and long description. Validation failed.'
                        Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                        return False
                    else:
                        message = f'Product {self.binding_id} is missing a web title. Will set to long description.'
                        Catalog.logger.warn(message)

                        if self.is_bound:
                            # Bound product: use binding key and parent variant
                            query = f"""
                            UPDATE IM_ITEM
                            SET ADDL_DESCR_1 = '{self.long_descr}'
                            WHERE {creds.cp_field_binding_id} = '{self.binding_id}' and IS_ADM_TKT = 'Y'"""

                        # Single Product use sku
                        else:
                            query = f"""
                            UPDATE IM_ITEM
                            SET ADDL_DESCR_1 = '{self.long_descr}'
                            WHERE ITEM_NO = '{self.sku}'"""

                            self.db.query_db(query, commit=True)
                            Catalog.logger.info(f'Web Title set to {self.web_title}')
                            self.web_title = self.long_descr

                # Test for dupicate web title
                if self.web_title is not None:
                    if self.is_bound:
                        # For bound products, look for matching web titles OUTSIDE of the current binding id
                        query = f"""
                        SELECT COUNT(ITEM_NO)
                        FROM IM_ITEM
                        WHERE ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}' AND {creds.cp_field_binding_id} != '{self.binding_id}' AND IS_ECOMM_ITEM = 'Y'"""

                    else:
                        query = f"""
                        SELECT COUNT(ITEM_NO)
                        FROM IM_ITEM
                        WHERE ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}' AND IS_ECOMM_ITEM = 'Y'"""

                    response = self.db.query_db(query)

                    if response:
                        if response[0][0] > 1:
                            Catalog.logger.warn(
                                f'Product {self.binding_id} has a duplicate web title. Will Append Sku to Web Title.'
                            )

                            if self.is_bound:
                                new_web_title = f'{self.web_title} - {self.binding_id}'
                            else:
                                new_web_title = f'{self.web_title} - {self.sku}'

                            self.web_title = new_web_title

                            Catalog.logger.info(f'New Web Title: {self.web_title}')
                            if self.is_bound:
                                # Update Parent Variant
                                query = f"""
                                UPDATE IM_ITEM
                                SET ADDL_DESCR_1 = '{self.web_title.replace("'", "''")}'
                                WHERE {creds.cp_field_binding_id} = '{self.binding_id}' and IS_ADM_TKT = 'Y'
                                
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
                    message = f'Product {self.binding_id} is missing an html description. Validation failed.'
                    Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False

            # Test for missing E-Commerce Categories
            if check_for_missing_categories:
                if not self.shopify_collections:
                    message = f'Product {self.binding_id} is missing E-Commerce Categories. Validation failed.'
                    Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False

            # Test for missing brand
            if check_for_invalid_brand:
                # Test for missing brand
                if self.brand:
                    bc_brands = [x[0] for x in list(Catalog.mw_brands)]
                    if self.brand not in bc_brands:
                        message = f'Product {self.binding_id} has a brand, but it is not valid. Will delete invalid brand.'
                        Catalog.logger.warn(message)
                        if self.validation_retries > 0:
                            self.reset_brand()
                            self.validation_retries -= 1
                            return self.validate_inputs()
                        else:
                            message = f'Product {self.binding_id} has an invalid brand. Validation failed.'
                            Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                            return False
                else:
                    message = f'Product {self.binding_id} is missing a brand. Will set to default.'
                    if self.validation_retries > 0:
                        self.reset_brand()
                        self.validation_retries -= 1
                        self.brand = creds.default_brand
                    Catalog.logger.warn(message)

            # Test for missing cost
            if check_for_item_cost:
                if self.cost == 0:
                    message = f'Product {self.sku} is missing a cost. Validation passed for now :).'
                    Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False

            # Test for missing price 1
            if self.default_price == 0:
                message = f'Product {self.sku} is missing a price 1. Validation failed.'
                Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

            if check_html_description:
                # Test for missing html description
                if len(self.html_description) < min_description_length:
                    message = f'Product {self.sku} is missing an html description. Validation failed.'
                    Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False

            if check_missing_images:
                # Test for missing product images
                if len(self.images) == 0:
                    message = f'Product {self.binding_id} is missing images. Will turn visibility to off.'
                    Catalog.logger.warn(message)
                    self.visible = False

            # BOUND PRODUCTS
            if self.is_bound:
                # print(f"Product {self.binding_id} is a bound product. Validation starting...")
                if check_web_title:
                    for child in self.variants:
                        if not child.is_parent:
                            if child.web_title == self.web_title:
                                Catalog.logger.warn(
                                    f'Non-Parent Variant {child.sku} has a web title. Will remove from child.'
                                )
                                child.web_title = ''
                                query = f"""
                                UPDATE IM_ITEM
                                SET ADDL_DESCR_1 = NULL
                                WHERE ITEM_NO = '{child.sku}'"""
                                self.db.query_db(query, commit=True)

            # Need validations for character counts on all fields
            # print(f"Product {self.sku} has passed validation.")
            # Validation has Passed.
            # Catalog.logger.success(
            #     f"Product SKU: {self.sku} Binding ID: {self.binding_id} has passed input validation."
            # )
            return True

        def get_product_payload(self):
            """Build the payload for creating a product in BigCommerce.
            This will include all variants, images, and custom fields."""

            def construct_custom_fields():
                result = []

                if self.custom_botanical_name:
                    cf_custom_botanical_name = {'value': self.custom_botanical_name['value']}

                    if self.custom_botanical_name['id']:
                        # Update Custom Field
                        cf_custom_botanical_name['id'] = self.custom_botanical_name['id']
                    else:
                        # Create Custom Field
                        cf_custom_botanical_name['key'] = Catalog.metafields['botanical name']['META_KEY']
                        cf_custom_botanical_name['type'] = Catalog.metafields['botanical name']['TYPE']
                        cf_custom_botanical_name['namespace'] = Catalog.metafields['botanical name']['NAME_SPACE']

                    result.append(cf_custom_botanical_name)

                if self.custom_climate_zone:
                    cf_custom_climate_zone = {'value': self.custom_climate_zone['value']}

                    if self.custom_climate_zone['id']:
                        # Update Custom Field
                        cf_custom_climate_zone['id'] = self.custom_climate_zone['id']
                    else:
                        # Create Custom Field
                        cf_custom_climate_zone['key'] = Catalog.metafields['climate zone']['META_KEY']
                        cf_custom_climate_zone['type'] = Catalog.metafields['climate zone']['TYPE']
                        cf_custom_climate_zone['namespace'] = Catalog.metafields['climate zone']['NAME_SPACE']

                    result.append(cf_custom_climate_zone)

                if self.custom_plant_type:
                    cf_custom_plant_type = {'value': self.custom_plant_type['value']}

                    if self.custom_plant_type['id']:
                        # Update Custom Field
                        cf_custom_plant_type['id'] = self.custom_plant_type['id']
                    else:
                        # Create Custom Field
                        cf_custom_plant_type['key'] = Catalog.metafields['plant type']['META_KEY']
                        cf_custom_plant_type['type'] = Catalog.metafields['plant type']['TYPE']
                        cf_custom_plant_type['namespace'] = Catalog.metafields['plant type']['NAME_SPACE']

                    result.append(cf_custom_plant_type)

                if self.custom_type:
                    cf_custom_type = {'value': self.custom_type['value']}

                    if self.custom_type['id']:
                        # Update Custom Field
                        cf_custom_type['id'] = self.custom_type['id']
                    else:
                        # Create Custom Field
                        cf_custom_type['key'] = Catalog.metafields['type']['META_KEY']
                        cf_custom_type['type'] = Catalog.metafields['type']['TYPE']
                        cf_custom_type['namespace'] = Catalog.metafields['type']['NAME_SPACE']

                    result.append(cf_custom_type)

                if self.custom_height:
                    cf_custom_height = {'value': self.custom_height['value']}

                    if self.custom_height['id']:
                        # Update Custom Field
                        cf_custom_height['id'] = self.custom_height['id']
                    else:
                        # Create Custom Field
                        cf_custom_height['key'] = Catalog.metafields['mature height']['META_KEY']
                        cf_custom_height['type'] = Catalog.metafields['mature height']['TYPE']
                        cf_custom_height['namespace'] = Catalog.metafields['mature height']['NAME_SPACE']

                    result.append(cf_custom_height)

                if self.custom_width:
                    cf_custom_width = {'value': self.custom_width['value']}

                    if self.custom_width['id']:
                        # Update Custom Field
                        cf_custom_width['id'] = self.custom_width['id']
                    else:
                        # Create Custom Field
                        cf_custom_width['key'] = Catalog.metafields['mature width']['META_KEY']
                        cf_custom_width['type'] = Catalog.metafields['mature width']['TYPE']
                        cf_custom_width['namespace'] = Catalog.metafields['mature width']['NAME_SPACE']

                    result.append(cf_custom_width)

                if self.custom_sun_exposure:
                    cf_custom_sun_exposure = {'value': self.custom_sun_exposure['value']}

                    if self.custom_sun_exposure['id']:
                        # Update Custom Field
                        cf_custom_sun_exposure['id'] = self.custom_sun_exposure['id']
                    else:
                        # Create Custom Field
                        cf_custom_sun_exposure['key'] = Catalog.metafields['sun exposure']['META_KEY']
                        cf_custom_sun_exposure['type'] = Catalog.metafields['sun exposure']['TYPE']
                        cf_custom_sun_exposure['namespace'] = Catalog.metafields['sun exposure']['NAME_SPACE']

                    result.append(cf_custom_sun_exposure)

                if self.custom_bloom_time:
                    cf_custom_bloom_time = {'value': self.custom_bloom_time['value']}

                    if self.custom_bloom_time['id']:
                        # Update Custom Field
                        cf_custom_bloom_time['id'] = self.custom_bloom_time['id']
                    else:
                        # Create Custom Field
                        cf_custom_bloom_time['key'] = Catalog.metafields['bloom time']['META_KEY']
                        cf_custom_bloom_time['type'] = Catalog.metafields['bloom time']['TYPE']
                        cf_custom_bloom_time['namespace'] = Catalog.metafields['bloom time']['NAME_SPACE']

                    result.append(cf_custom_bloom_time)

                if self.custom_bloom_color:
                    cf_custom_bloom_color = {'value': self.custom_bloom_color['value']}

                    if self.custom_bloom_color['id']:
                        # Update Custom Field
                        cf_custom_bloom_color['id'] = self.custom_bloom_color['id']
                    else:
                        # Create Custom Field
                        cf_custom_bloom_color['key'] = Catalog.metafields['bloom color']['META_KEY']
                        cf_custom_bloom_color['type'] = Catalog.metafields['bloom color']['TYPE']
                        cf_custom_bloom_color['namespace'] = Catalog.metafields['bloom color']['NAME_SPACE']

                    result.append(cf_custom_bloom_color)

                if self.custom_attracts_pollinators:
                    cf_custom_attracts_pollinators = {'value': self.custom_attracts_pollinators['value']}

                    if self.custom_attracts_pollinators['id']:
                        # Update Custom Field
                        cf_custom_attracts_pollinators['id'] = self.custom_attracts_pollinators['id']
                    else:
                        # Create Custom Field
                        cf_custom_attracts_pollinators['key'] = Catalog.metafields['attracts pollinators'][
                            'META_KEY'
                        ]
                        cf_custom_attracts_pollinators['type'] = Catalog.metafields['attracts pollinators']['TYPE']
                        cf_custom_attracts_pollinators['namespace'] = Catalog.metafields['attracts pollinators'][
                            'NAME_SPACE'
                        ]

                    result.append(cf_custom_attracts_pollinators)

                if self.custom_growth_rate:
                    cf_custom_growth_rate = {'value': self.custom_growth_rate['value']}

                    if self.custom_growth_rate['id']:
                        # Update Custom Field
                        cf_custom_growth_rate['id'] = self.custom_growth_rate['id']
                    else:
                        # Create Custom Field
                        cf_custom_growth_rate['key'] = Catalog.metafields['growth rate']['META_KEY']
                        cf_custom_growth_rate['type'] = Catalog.metafields['growth rate']['TYPE']
                        cf_custom_growth_rate['namespace'] = Catalog.metafields['growth rate']['NAME_SPACE']

                    result.append(cf_custom_growth_rate)

                if self.custom_deer_resistant:
                    cf_custom_deer_resistant = {'value': self.custom_deer_resistant['value']}

                    if self.custom_deer_resistant['id']:
                        # Update Custom Field
                        cf_custom_deer_resistant['id'] = self.custom_deer_resistant['id']
                    else:
                        # Create Custom Field
                        cf_custom_deer_resistant['key'] = Catalog.metafields['deer resistant']['META_KEY']
                        cf_custom_deer_resistant['type'] = Catalog.metafields['deer resistant']['TYPE']
                        cf_custom_deer_resistant['namespace'] = Catalog.metafields['deer resistant']['NAME_SPACE']

                    result.append(cf_custom_deer_resistant)

                if self.custom_soil_type:
                    cf_custom_soil_type = {'value': self.custom_soil_type['value']}

                    if self.custom_soil_type['id']:
                        # Update Custom Field
                        cf_custom_soil_type['id'] = self.custom_soil_type['id']
                    else:
                        # Create Custom Field
                        cf_custom_soil_type['key'] = Catalog.metafields['soil type']['META_KEY']
                        cf_custom_soil_type['type'] = Catalog.metafields['soil type']['TYPE']
                        cf_custom_soil_type['namespace'] = Catalog.metafields['soil type']['NAME_SPACE']

                    result.append(cf_custom_soil_type)

                if self.custom_color:
                    cf_custom_color = {'value': self.custom_color['value']}

                    if self.custom_color['id']:
                        # Update Custom Field
                        cf_custom_color['id'] = self.custom_color['id']
                    else:
                        # Create Custom Field
                        cf_custom_color['key'] = Catalog.metafields['color']['META_KEY']
                        cf_custom_color['type'] = Catalog.metafields['color']['TYPE']
                        cf_custom_color['namespace'] = Catalog.metafields['color']['NAME_SPACE']

                    result.append(cf_custom_color)

                if self.custom_size:
                    cf_custom_size = {'value': self.custom_size['value']}

                    if self.custom_size['id']:
                        # Update Custom Field
                        cf_custom_size['id'] = self.custom_size['id']
                    else:
                        # Create Custom Field
                        cf_custom_size['key'] = Catalog.metafields['size']['META_KEY']
                        cf_custom_size['type'] = Catalog.metafields['size']['TYPE']
                        cf_custom_size['namespace'] = Catalog.metafields['size']['NAME_SPACE']

                    result.append(cf_custom_size)

                return result

            def create_image_payload():
                sort_order = 0
                for x in self.images:
                    if sort_order == 0:
                        x.is_thumbnail = True
                    x.sort_order = sort_order
                    sort_order += 1
                result = []

                file_list = []
                stagedUploadsCreateVariables = {'input': []}

                for image in self.images:
                    if not image.image_url:
                        file_list.append(image.file_path)
                        stagedUploadsCreateVariables['input'].append(
                            {
                                'filename': image.image_name,
                                'mimeType': 'image/jpg',
                                'httpMethod': 'POST',
                                'resource': 'IMAGE',
                            }
                        )
                if file_list:
                    uploaded_files = Shopify.Product.Files.create(
                        variables=stagedUploadsCreateVariables, file_list=file_list
                    )
                    for file in uploaded_files:
                        print(f'Uploaded File: {file}')
                        for image in self.images:
                            if file['file_path'] == image.file_path:
                                print(f'Updating Image URL for {image.image_name} to {file['url']}')
                                image.image_url = file['url']

                for image in self.images:
                    image_payload = {
                        'originalSource': image.image_url,
                        'alt': image.description,
                        'mediaContentType': 'IMAGE',
                    }
                    if image.image_id:
                        image_payload['id'] = f'gid://shopify/MediaImage/{image.image_id}'

                    print(f'Image Payload: {image_payload}')
                    result.append(image_payload)
                return result

            def get_brand_name(brand):
                """Takes the brand profile code and returns the brand name"""
                query = f"""
                SELECT DESCR
                FROM IM_ITEM_PROF_COD
                WHERE PROF_COD = '{brand}'"""
                response = self.db.query_db(query)
                if response:
                    return response[0][0]
                else:
                    return brand

            product_payload = {
                'input': {
                    'title': self.web_title,
                    'productType': self.custom_type,
                    'descriptionHtml': self.html_description,
                    'seo': {'title': self.meta_title, 'description': self.meta_description},
                    'status': 'ACTIVE' if self.visible else 'DRAFT',
                    'tags': self.search_keywords.split(','),
                    'metafields': construct_custom_fields(),
                },
                'media': create_image_payload(),
            }
            if self.product_id:
                product_payload['input']['id'] = f'gid://shopify/Product/{self.product_id}'

            if self.brand:
                product_payload['input']['vendor'] = get_brand_name(self.brand)

            if self.shopify_collections:
                product_payload['input']['collectionsToJoin'] = [
                    f'gid://shopify/Collection/{x}' for x in self.shopify_collections
                ]

            if not self.product_id:  # new product
                # If Add Standalone Variant Option - will be deleted later
                if self.is_bound:
                    product_payload['input']['productOptions'] = [
                        {'name': 'Option', 'values': [{'name': '9999 Gallon'}]}
                    ]

            return product_payload

        def get_bulk_variant_payload(self):
            payload = {'media': [], 'strategy': 'REMOVE_STANDALONE_VARIANT', 'variants': []}
            # If product_id exists, this is an update
            if self.product_id:
                payload['productId'] = f'gid://shopify/Product/{self.product_id}'

            for child in self.variants:
                variant_payload = {
                    'inventoryItem': {
                        'cost': child.cost,
                        'measurement': {'weight': {'unit': 'POUNDS', 'value': self.weight}},
                        'tracked': True,
                        'requiresShipping': True,
                        'sku': child.sku,
                    },
                    'inventoryPolicy': 'DENY',  # Prevents overselling,
                    'price': child.price_1,  # May be overwritten by price_2 (below)
                    'compareAtPrice': child.price_1,  # Retail price before sales
                    'optionValues': {'optionName': 'Option'},
                    'taxable': False,
                }

                variant_payload['inventoryQuantities'] = {
                    'availableQuantity': child.buffered_quantity,
                    'locationId': creds.shopify_location_id,
                }

                if child.variant_id:
                    variant_payload['id'] = f'gid://shopify/ProductVariant/{child.variant_id}'

                if child.price_2:
                    variant_payload['price'] = min(child.price_1, child.price_2)

                if self.is_bound:
                    variant_payload['optionValues']['name'] = child.variant_name
                else:
                    if child.custom_size:
                        variant_payload['optionValues']['name'] = child.custom_size
                    else:
                        variant_payload['optionValues']['name'] = 'Default Title'

                # Add Variant Image
                for image in child.images:
                    if image.is_variant_image:
                        print(
                            f'Adding Variant Image: {image.image_name} to Variant: {child.sku}. Url: {image.image_url}'
                        )
                        variant_payload['mediaSrc'] = image.image_url

                payload['variants'].append(variant_payload)

                # for image in child.images:
                #     if image.is_variant_image:
                #         variant_payload['mediaId'] = f'gid://shopify/MediaImage/{image.image_id}'

            print(f'Bulk Variant Payload: {payload}')
            return payload

        def get_single_variant_payload(self):
            payload = {
                'input': {
                    'compareAtPrice': self.default_price,
                    'id': f'gid://shopify/ProductVariant/{self.variants[0].variant_id}',
                    'inventoryItem': {
                        'cost': self.cost,
                        'measurement': {'weight': {'unit': 'POUNDS', 'value': self.weight}},
                        'requiresShipping': True,
                        'sku': self.sku,
                        'tracked': True,
                    },
                    'inventoryQuantities': {
                        'availableQuantity': self.buffered_quantity,
                        'locationId': creds.shopify_location_id,
                    },
                    'inventoryPolicy': 'DENY',
                    'price': self.default_price,
                    'taxable': False,
                }
            }
            print(f'Single Variant Payload: {payload}')
            return payload

        def get_inventory_payload(self):
            payload = {
                'input': {'name': 'available', 'reason': 'other', 'ignoreCompareQuantity': True, 'quantities': []}
            }

            for child in self.variants:
                if child.variant_id:
                    payload['input']['quantities'].append(
                        {
                            'inventoryItemId': f'gid://shopify/InventoryItem/{child.inventory_id}',
                            'locationId': creds.shopify_location_id,
                            'quantity': child.buffered_quantity,
                        }
                    )
            print(f'Inventory Payload: {payload}')
            return payload

        def get_variant_image_payload(self):
            # Add Variant Image
            variant_image_payload = []
            for child in self.variants:
                for image in child.images:
                    if image.is_variant_image:
                        variant_image_payload.append(
                            {
                                'id': f'gid://shopify/ProductVariant/{child.variant_id}',
                                'imageId': f'gid://shopify/MediaImage/{image.image_id}',
                            }
                        )
            return variant_image_payload

        def process(self):
            """Process Product Creation/Delete/Update in BigCommerce and Middleware."""

            def create():
                """Create new product in Shopify and Middleware."""
                # Create Base Product
                response = Shopify.Product.create(self.get_product_payload())
                self.product_id = response['product_id']
                self.option_id = response['option_ids'][0]

                # Assign Default Variant Properties
                self.variants[0].variant_id = response['variant_ids'][0]
                self.variants[0].option_id = self.option_id
                if self.is_bound:
                    self.variants[0].option_value_id = response['option_value_ids'][0]
                self.variants[0].inventory_id = response['inventory_ids'][0]

                for x, image in enumerate(self.images):
                    image.product_id = self.product_id
                    image.image_id = response['media_ids'][x]

                if len(self.variants) > 1:
                    print('Creating Multiple Variants')
                    # Save Default Option Value ID for Deletion
                    delete_target = self.variants[0].option_value_id
                    # Create Variants in Bulk
                    self.variants[0].variant_id = None
                    response = Shopify.Product.Variant.create_bulk(self.get_bulk_variant_payload())

                    for x, variant in enumerate(self.variants):
                        variant.variant_id = response['variant_ids'][x]
                        variant.option_value_id = response['option_value_ids'][x]
                        variant.inventory_id = response['inventory_ids'][x]
                        variant.option_id = self.option_id

                    # # Remove Default Variant
                    Shopify.Product.Option.update(
                        product_id=self.product_id,
                        option_id=self.option_id,
                        option_values_to_delete=[delete_target],
                    )
                    # Wait for images to process
                    time.sleep(3)
                    Shopify.Product.Variant.Image.create(self.product_id, self.get_variant_image_payload())

                else:
                    # Update Default Variant
                    print('Updating Default Variant')
                    single_payload = self.get_single_variant_payload()
                    Shopify.Product.Variant.update_single(single_payload)

                # Add Product to Online Store Sales Channel
                Shopify.Product.publish(self.product_id)

                Database.Shopify.Product.insert(self)
                for image in self.images:
                    Database.Shopify.Product.Image.insert(image)

            def update():
                """Will update existing product. Will clear out custom field data and reinsert."""
                product_payload = self.get_product_payload()

                response = Shopify.Product.update(product_payload)
                self.option_id = response['option_ids'][0]
                for x, image in enumerate(self.images):
                    image.image_id = response['media_ids'][x]

                if self.is_bound:
                    variant_payload = self.get_bulk_variant_payload()
                    Shopify.Product.Variant.update_bulk(variant_payload)
                else:
                    variant_payload = self.get_single_variant_payload()
                    Shopify.Product.Variant.update_single(variant_payload)

                Database.Shopify.Product.sync(product=self)

            if self.product_id:
                update()
            else:
                create()

            # Update Inventory
            Shopify.Inventory.update(self.get_inventory_payload())

        def replace_image(self, image) -> bool:
            """Replace image in BigCommerce and SQL."""
            self.delete_image(image)
            image.image_id = Shopify.Product.Media.Image.create(image)
            Database.Shopify.Product.Image.insert(image)

        def get_shopify_collections(self):
            """Get Shopify Collection IDs from Middleware Category IDs"""
            result = []

            if self.cp_ecommerce_categories:
                for category in self.cp_ecommerce_categories:
                    # Get Collection ID from Middleware Category ID
                    q = f"""
                        SELECT COLLECTION_ID 
                        FROM {creds.shopify_category_table}
                        WHERE CP_CATEG_ID = '{category}'
                        """
                    response = self.db.query_db(q)
                    try:
                        result.append(response[0][0])
                    except:
                        continue
                    else:
                        top_level = False
                        while not top_level:
                            # Get Parent Collection ID from Middleware Category ID
                            q = f"""
                                SELECT COLLECTION_ID
                                FROM SN_SHOP_CATEG
                                WHERE CP_CATEG_ID = (SELECT CP_PARENT_ID
                                                    FROM SN_SHOP_CATEG
                                                    WHERE CP_CATEG_ID = '{category}')
                                """
                            parent_response = self.db.query_db(q)
                            print(parent_response)
                            try:
                                result.append(parent_response[0][0])
                            except:
                                top_level = True
                            else:
                                category = parent_response[0][0]

            return result

        def delete_product(self, sku, binding_id=None):
            """Delete Product from BigCommerce and Middleware."""
            self.db_id = None
            if self.product_id:
                product_id = self.product_id
            else:
                if binding_id:
                    product_id = Database.Shopify.Product.get_id(binding_id=binding_id)
                else:
                    product_id = Database.Shopify.Product.get_id(item_no=sku)

            Shopify.Product.delete(product_id)
            Database.Shopify.Product.delete(product_id)

        def delete_variant(self, sku, binding_id=None):
            """Delete Variant from BigCommerce and Middleware. This will also delete the option value from BigCommerce."""
            if self.is_last_variant(binding_id=binding_id):
                print('Last Variant in Product. Will delete product.')
                self.delete_product(sku=sku, binding_id=binding_id)
            elif self.is_parent(sku):
                print('Parent Product. Will delete product.')
                self.delete_product(sku=sku, binding_id=binding_id)
            else:
                # self.db_id = None # does this make sense. Should it be child only?
                variant_id = Database.Shopify.Product.Variant.get_variant_id(sku=sku)  # Use for MW deletion
                # Shopify.Product.Variant.delete(variant_id)
                option_value_id = Database.Shopify.Product.Variant.get_option_value_id(sku=sku)
                if self.product_id:
                    product_id = self.product_id
                else:
                    if binding_id:
                        product_id = Database.Shopify.Product.get_id(binding_id=binding_id)
                    else:
                        product_id = Database.Shopify.Product.get_id(item_no=sku)

                Shopify.Product.Option.update(
                    product_id=product_id, option_id=self.option_id, option_values_to_delete=[option_value_id]
                )

                Database.Shopify.Product.Variant.delete(variant_id)

        def delete_image(self, image):
            """Delete image from Shopify and Middleware."""
            Shopify.Product.Media.Image.delete(image)
            Database.Shopify.Product.Image.delete(image)

        def is_parent(self, sku):
            """Check if this product is a parent product."""
            query = f"""
            SELECT IS_PARENT
            FROM {creds.shopify_product_table}
            WHERE ITEM_NO = '{sku}'
            """
            response = self.db.query_db(query)
            if response is not None:
                return response[0][0] == 1

        def remove_parent(self):
            print('Entering Remove Parent Function of Product Class')
            """Remove parent status from all children"""
            query = f"""
                    UPDATE IM_ITEM 
                    SET IS_ADM_TKT = 'N', LST_MAINT_DT = GETDATE()
                    WHERE {creds.cp_field_binding_id} = '{self.binding_id}'
                    """
            self.db.query_db(query, commit=True)
            print('Parent status removed from all children.')

        def is_last_variant(self, binding_id):
            """Check if this is the last variant in the parent product."""
            if binding_id is None:
                return True
            query = f"""SELECT COUNT(*) 
            FROM {creds.shopify_product_table} 
            WHERE BINDING_ID = '{binding_id}'"""
            response = self.db.query_db(query)
            if response is not None:
                return response[0][0] == 1

        @staticmethod
        def get_all_binding_ids():
            binding_ids = set()
            db = query_engine.QueryEngine()
            query = """
            SELECT {creds.cp_field_binding_id}
            FROM IM_ITEM
            WHERE {creds.cp_field_binding_id} IS NOT NULL
            """
            response = db.query_db(query)
            if response is not None:
                for x in response:
                    binding_ids.add(x[0])
            return list(binding_ids)

        class Variant:
            def __init__(self, sku, last_run_date, get_images=True):
                self.db = Database.db
                self.sku = sku
                self.last_run_date = last_run_date

                # Product ID Info
                product_data = self.get_variant_details()

                # Product Information
                self.db_id = product_data['db_id']
                self.binding_id = product_data['binding_id']
                self.mw_binding_id = product_data['mw_binding_id']
                self.is_parent = True if product_data['is_parent'] == 'Y' else False
                self.product_id: int = product_data['product_id'] if product_data['product_id'] else None
                self.variant_id: int = product_data['variant_id'] if product_data['variant_id'] else None
                self.inventory_id: int = product_data['inventory_id'] if product_data['inventory_id'] else None
                self.option_id = None
                self.option_value_id = None
                self.web_title: str = product_data['web_title']
                self.long_descr = product_data['long_descr']
                self.variant_name = product_data['variant_name']
                self.status = product_data['status']
                self.price_1 = float(product_data['price_1'])
                self.cost = float(product_data['cost'])
                self.price_2 = float(product_data['price_2']) if product_data['price_2'] else None
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
                self.purchasing_disabled_message = ''
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
                self.is_preorder = product_data['is_preorder']
                self.preorder_release_date = product_data['preorder_release_date']
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

                # Custom URL
                custom_url = product_data['custom_url']
                if custom_url:
                    self.custom_url = '-'.join(str(re.sub('[^A-Za-z0-9 ]+', '', custom_url)).split(' '))
                else:
                    self.custom_url = None

                # Product Images
                self.images = []

                # Dates
                self.lst_maint_dt = product_data['lst_maint_dt']

                # E-Commerce Categories

                self.cp_ecommerce_categories = product_data['cp_ecommerce_categories']

                # Product Schema (i.e. Bound, Single, Variant.)
                self.item_schema = ''
                # Processing Method
                self.processing_method = ''

                # Initialize Images
                if get_images:
                    self.get_local_product_images()

                # Initialize Variant Image URL
                if len(self.images) > 0:
                    self.variant_image_url = self.images[0].image_url
                else:
                    self.variant_image_url = ''

            def __str__(self):
                result = ''
                for k, v in self.__dict__.items():
                    result += f'{k}: {v}\n'
                return result

            def get_variant_details(self):
                """Get a list of all products that have been updated since the last run date.
                Will check IM_ITEM. IM_PRC, IM_INV, EC_ITEM_DESCR, EC_CATEG_ITEM, and Image tables
                have an after update Trigger implemented for updating IM_ITEM.LST_MAINT_DT."""

                query = f""" select ITEM.{creds.cp_field_binding_id} as 'Binding ID(0)', ITEM.IS_ECOMM_ITEM as 'Web 
                Enabled(1)', ISNULL(ITEM.IS_ADM_TKT, 'N') as 'Is Parent(2)', MW_PROD.PRODUCT_ID as 'Product ID (3)', 
                MW_PROD.VARIANT_ID as 'Variant ID(4)', ITEM.USR_CPC_IS_ENABLED 
                as 'Web Visible(5)', ITEM.USR_ALWAYS_ONLINE as 'ALWAYS ONLINE(6)', ITEM.IS_FOOD_STMP_ITEM as 
                'GIFT_WRAP(7)', ITEM.PROF_COD_1 as 'BRAND(8)', ITEM.ECOMM_NEW as 'IS_FEATURED(9)', 
                ITEM.USR_IN_STORE_ONLY as 'IN_STORE_ONLY(10)', ITEM.USR_PROF_ALPHA_27 as 'SORT ORDER(11)', 
                ISNULL(ITEM.ADDL_DESCR_1, '') as 'WEB_TITLE(12)', ISNULL(ITEM.ADDL_DESCR_2, '') as 'META_TITLE(13)', 
                ISNULL(USR_PROF_ALPHA_21, '') as 'META_DESCRIPTION(14)', ISNULL(ITEM.USR_PROF_ALPHA_17, 
                '') as 'VARIANT NAME(15)', ITEM.STAT as 'STATUS(16)', ISNULL(ITEM.REG_PRC, 0) as 'REG_PRC(17)', 
                ISNULL(ITEM.PRC_1, 0) as 'PRC_1(18)', PRC.PRC_2 as 'PRC_2(19)', CAST(ISNULL(INV.QTY_AVAIL, 
                0) as INTEGER) as 'QUANTITY_AVAILABLE(20)', CAST(ISNULL(ITEM.PROF_NO_1, 0) as INTEGER) as 'BUFFER(
                21)', ITEM.ITEM_TYP as 'ITEM_TYPE(22)', ITEM.LONG_DESCR as 'LONG_DESCR(23)', 
                ISNULL(ITEM.USR_PROF_ALPHA_26, '') as 'SEARCH_KEYWORDS(24)', ITEM.USR_PROF_ALPHA_19 as 
                'PREORDER_MESSAGE(25)', ISNULL(EC_ITEM_DESCR.HTML_DESCR, '') as 'HTML_DESCRIPTION(26)', 
                ISNULL(USR_PROF_ALPHA_22, '') as 'ALT_TEXT_1(27)', ISNULL(USR_PROF_ALPHA_23, '') as 'ALT_TEXT_2(28)', 
                ISNULL(USR_PROF_ALPHA_24, '') as 'ALT_TEXT_3(29)', ISNULL(USR_PROF_ALPHA_25, '') as 'ALT_TEXT_4(30)', 
                PROF_ALPHA_1 as 'BOTANICAL_NAM(31)', ISNULL(PROF_ALPHA_2, '') as 'CLIMATE_ZONE(32)', 
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

                MW_PROD.ID as 'db_id(50)', MW_PROD.CUSTOM_FIELDS as 'custom_field_ids(51)', ITEM.LONG_DESCR as 'long_descr(52)',
                MW_PROD.BINDING_ID as 'mw_binding_id(53)', ITEM.USR_IS_PREORDER as 'is_preorder(54)', 
                ITEM.USR_PREORDER_REL_DT as 'preorder_release_date(55)', ITEM.USR_PROF_ALPHA_18 as 'CUSTOM_URL(56)', 
                MW_PROD.INVENTORY_ID as 'inventory_id(57)', 

                MW_PROD.CF_BOTAN_NAM as 'custom_botanical_name(58)',
                MW_PROD.CF_CLIM_ZON as 'custom_climate_zone(59)',
                MW_PROD.CF_PLANT_TYP as 'custom_plant_type(60)',
                MW_PROD.CF_TYP as 'custom_type(61)',
                MW_PROD.CF_HEIGHT as 'custom_height(62)',
                MW_PROD.CF_WIDTH as 'custom_width(63)',
                MW_PROD.CF_SUN_EXP as 'custom_sun_exposure(64)',
                MW_PROD.CF_BLOOM_TIM as 'custom_bloom_time(65)',
                MW_PROD.CF_FLOW_COL as 'custom_bloom_color(66)',
                MW_PROD.CF_POLLIN as 'custom_attracts_pollinators(67)',
                MW_PROD.CF_GROWTH_RT as 'custom_growth_rate(68)',
                MW_PROD.CF_DEER_RES as 'custom_deer_resistant(69)',
                MW_PROD.CF_SOIL_TYP as 'custom_soil_type(70)',
                MW_PROD.CF_COLOR as 'custom_color(71)',
                MW_PROD.CF_SIZE as 'custom_size(72)'

                FROM IM_ITEM ITEM
                LEFT OUTER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
                LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
                LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
                LEFT OUTER JOIN {creds.shopify_product_table} MW_PROD ON ITEM.ITEM_NO=MW_PROD.ITEM_NO
                LEFT OUTER JOIN IM_ITEM_PROF_COD COD ON ITEM.PROF_COD_1 = COD.PROF_COD
                WHERE ITEM.ITEM_NO = '{self.sku}'"""

                item = self.db.query_db(query)
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
                        'inventory_id': item[0][57],
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
                        'custom_botanical_name': {'id': item[0][58], 'value': item[0][31]},
                        'custom_climate_zone': {'id': item[0][59], 'value': item[0][32]},
                        'custom_plant_type': {'id': item[0][60], 'value': item[0][33]},
                        'custom_type': {'id': item[0][61], 'value': item[0][34]},
                        'custom_height': {'id': item[0][62], 'value': item[0][35]},
                        'custom_width': {'id': item[0][63], 'value': item[0][36]},
                        'custom_sun_exposure': {'id': item[0][64], 'value': item[0][37]},
                        'custom_bloom_time': {'id': item[0][65], 'value': item[0][38]},
                        'custom_bloom_color': {'id': item[0][66], 'value': item[0][39]},
                        'custom_attracts_pollinators': {'id': item[0][67], 'value': item[0][40]},
                        'custom_growth_rate': {'id': item[0][68], 'value': item[0][41]},
                        'custom_deer_resistant': {'id': item[0][69], 'value': item[0][42]},
                        'custom_soil_type': {'id': item[0][70], 'value': item[0][43]},
                        'custom_color': {'id': item[0][71], 'value': item[0][44]},
                        'custom_size': {'id': item[0][72], 'value': item[0][45]},
                        'lst_maint_dt': item[0][46],
                        'cost': item[0][47],
                        'cp_ecommerce_categories': str(item[0][49]).split(',') if item[0][49] else [],
                        'custom_url': item[0][56],
                        'long_descr': item[0][52],
                        'mw_binding_id': item[0][53],
                        'is_preorder': True if item[0][54] == 'Y' else False,
                        'preorder_release_date': convert_to_utc(item[0][55]) if item[0][55] else None,
                    }
                    return details

            def validate_product(self):
                print(f'Validating product {self.sku}')
                # Test for missing variant name
                if self.variant_name == '':
                    print(f'Product {self.sku} is missing a variant name. Validation failed.')
                    return False
                # Test for missing price 1
                if self.price_1 == 0:
                    print(f'Product {self.sku} is missing a price 1. Validation failed.')
                    return False

                return True

            def get_local_product_images(self):
                """Get local image information for product"""
                product_images = []
                photo_path = creds.photo_path
                list_of_files = os.listdir(photo_path)
                if list_of_files is not None:
                    for x in list_of_files:
                        if x.split('.')[0].split('^')[0].lower() == self.sku.lower():
                            product_images.append(x)
                total_images = len(product_images)
                if total_images > 0:
                    # print(f"Found {total_images} product images for item: {self.sku}")
                    for image in product_images:
                        img = Catalog.Product.Image(image_name=image)
                        if img.validate():
                            self.images.append(img)

            def construct_image_payload(self):
                result = []
                for image in self.images:
                    image_payload = {
                        'src': f'{creds.public_web_dav_photos}/{image.image_name}',
                        'position': image.image_number,
                        'alt': image.alt_text_1,
                    }
                    result.append(image_payload)

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
            def __init__(self, image_name: str):
                self.db = Database.db
                self.db_id = None
                self.image_name = image_name  # This is the file name
                self.sku = ''
                self.file_path = f'{creds.photo_path}/{self.image_name}'
                self.image_url = ''
                self.product_id = None
                self.variant_id = None
                self.image_id = None
                self.is_thumbnail = False
                self.image_number = 1
                self.sort_order = 0
                self.is_binding_image = False
                self.binding_id = None
                self.is_variant_image = False
                self.description = ''
                self.size = 0
                self.last_maintained_dt = None
                self.get_image_details()

            def __str__(self):
                result = ''
                for k, v in self.__dict__.items():
                    result += f'{k}: {v}\n'
                return result

            def get_image_details(self):
                """Get image details from SQL"""
                query = f"SELECT * FROM {creds.shopify_image_table} WHERE IMAGE_NAME = '{self.image_name}'"
                response = self.db.query_db(query)

                if response is not None:
                    self.db_id = response[0][0]
                    self.image_name = response[0][1]
                    self.sku = response[0][2]
                    self.file_path = response[0][3]
                    self.image_url = response[0][4]
                    self.product_id = response[0][5]
                    self.image_id = response[0][6]
                    self.image_number = response[0][8]
                    self.is_binding_image = True if response[0][10] == 1 else False
                    self.binding_id = response[0][11]
                    self.is_variant_image = True if response[0][12] == 1 else False
                    self.description = self.get_image_description()  # This will pull fresh data each sync.
                    self.size = response[0][14]
                    self.last_maintained_dt = response[0][15]

                else:
                    self.set_image_details()

            def validate(self):
                """Images will be validated for size and format before being uploaded and written to middleware.
                Images that have been written to database previously will be considered valid and will pass."""
                if self.db_id:
                    # These items have already been through check before.
                    return True
                else:
                    # Check for valid file size/format
                    size = (1280, 1280)
                    q = 90
                    exif_orientation = 0x0112
                    if self.image_name.lower().endswith('jpg'):
                        # Resize files larger than 1.8 MB
                        if self.size > 1800000:
                            print(f'Found large file {self.image_name}. Attempting to resize.')
                            try:
                                im = Image.open(self.file_path)
                                im.thumbnail(size, Image.LANCZOS)
                                code = im.getexif().get(exif_orientation, 1)
                                if code and code != 1:
                                    im = ImageOps.exif_transpose(im)
                                im.save(self.file_path, 'JPEG', quality=q)
                                im.close()
                                self.size = os.path.getsize(self.file_path)
                                print(f'{self.image_name} resized.')
                            except Exception as e:
                                print(f'Error resizing {self.image_name}: {e}')
                                return False
                            else:
                                print(f'Image {self.image_name} was resized.')

                    # Remove Alpha Layer and Convert PNG to JPG
                    if self.image_name.lower().endswith('png'):
                        print(f'Found PNG file: {self.image_name}. Attempting to reformat.')
                        try:
                            im = Image.open(self.file_path)
                            im.thumbnail(size, Image.LANCZOS)
                            # Preserve Rotational Data
                            code = im.getexif().get(exif_orientation, 1)
                            if code and code != 1:
                                im = ImageOps.exif_transpose(im)
                            print('Stripping Alpha Layer.')
                            rgb_im = im.convert('RGB')
                            print('Saving new file in JPG format.')
                            new_image_name = self.image_name.split('.')[0] + '.jpg'
                            new_file_path = f'{creds.photo_path}/{new_image_name}'
                            rgb_im.save(new_file_path, 'JPEG', quality=q)
                            im.close()
                            print('Removing old PNG file')
                            os.remove(self.file_path)
                            self.file_path = new_file_path
                            self.image_name = new_image_name
                        except Exception as e:
                            print(f'Error converting {self.image_name}: {e}')
                            return False
                        else:
                            print('Conversion successful.')

                    # replace .JPEG with .JPG
                    if self.image_name.lower().endswith('jpeg'):
                        print('Found file ending with .JPEG. Attempting to reformat.')
                        try:
                            print(self.file_path)
                            im = Image.open(self.file_path)
                            im.thumbnail(size, Image.LANCZOS)
                            # Preserve Rotational Data
                            code = im.getexif().get(exif_orientation, 1)
                            if code and code != 1:
                                im = ImageOps.exif_transpose(im)
                            new_image_name = self.image_name.split('.')[0] + '.jpg'
                            new_file_path = f'{creds.photo_path}/{new_image_name}'
                            im.save(new_file_path, 'JPEG', quality=q)
                            im.close()
                            os.remove(self.file_path)
                            self.file_path = new_file_path
                            self.image_name = new_image_name
                        except Exception as e:
                            print(f'Error converting {self.image_name}: {e}')
                            return False
                        else:
                            print('Conversion successful.')

                    # check for description that is too long
                    if len(self.description) >= 500:
                        print(f'Description for {self.image_name} is too long. Validation failed.')
                        return False

                    # Check for images with words or trailing numbers in the name
                    if '^' in self.image_name and not self.image_name.split('.')[0].split('^')[1].isdigit():
                        print(f'Image {self.image_name} is not valid.')
                        return False

                    # Valid Image
                    return True

            def set_image_details(self):
                def get_item_no_from_image_name(image_name):
                    def get_binding_id(item_no):
                        query = f"""
                               SELECT {creds.cp_field_binding_id} FROM IM_ITEM
                               WHERE ITEM_NO = '{item_no}'
                               """
                        response = self.db.query_db(query)
                        if response is not None:
                            return response[0][0] if response[0][0] else ''

                    # Check for binding image
                    if image_name.split('.')[0].split('^')[0] in Catalog.all_binding_ids:
                        item_no = ''
                        binding_id = image_name.split('.')[0].split('^')[0]
                        self.is_binding_image = True
                    else:
                        item_no = image_name.split('.')[0].split('^')[0]
                        binding_id = get_binding_id(item_no)

                    return item_no, binding_id

                def get_image_number():
                    image_number = 1
                    if '^' in self.image_name and self.image_name.split('.')[0].split('^')[1].isdigit():
                        # secondary images
                        for x in range(1, 100):
                            if int(self.image_name.split('.')[0].split('^')[1]) == x:
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
                            return ''
                    else:
                        return ''
                else:
                    # If image number is greater than 4, it  will not have a description
                    return ''

            def resize_image(self):
                size = (1280, 1280)
                q = 90
                exif_orientation = 0x0112
                if self.image_name.endswith('jpg'):
                    im = Image.open(self.file_path)
                    im.thumbnail(size, Image.LANCZOS)
                    code = im.getexif().get(exif_orientation, 1)
                    if code and code != 1:
                        im = ImageOps.exif_transpose(im)
                    im.save(self.file_path, 'JPEG', quality=q)
                    print(f'Resized {self.image_name}')


if __name__ == '__main__':  #
    from datetime import datetime

    # Catalog.delete_products()
    catalog = Catalog(datetime(2021, 1, 1))
    catalog.sync()
