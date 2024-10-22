import os
import re
import json
from datetime import datetime
from setup.date_presets import Dates
import time
import requests
from integration.shopify_api import Shopify
from PIL import ImageOps
from PIL import Image as PILImage

from setup import creds
from setup.creds import Table
from database import Database as db
from setup.utilities import get_product_images, convert_to_utc, parse_custom_url, get_filesize
from concurrent.futures import ThreadPoolExecutor
from product_tools.products import get_all_back_in_stock_items, get_all_new_new_items
from setup.error_handler import ProcessOutErrorHandler

from traceback import format_exc as tb


class Catalog:
    all_binding_ids = db.CP.Product.get_binding_id()
    metafields = db.Shopify.Metafield_Definition.get()
    eh = ProcessOutErrorHandler
    logger = eh.logger
    error_handler = eh.error_handler

    def __init__(
        self,
        dates,
        last_sync=datetime(1970, 1, 1),
        inventory_only=False,
        verbose=False,
        test_mode=False,
        test_queue=None,
        initial_sync=False,
    ):
        self.dates: Dates = dates
        self.initial_sync = initial_sync
        self.last_sync = last_sync
        self.inventory_only = inventory_only
        self.verbose = verbose
        self.test_mode = test_mode
        self.test_queue = test_queue
        if not self.inventory_only and not self.test_mode:
            self.category_tree = Collections(last_sync=last_sync)

        self.cp_items = []
        self.mw_items = []
        self.new_items = get_all_new_new_items(start_date=self.dates.one_month_ago)
        self.back_in_stock_items = get_all_back_in_stock_items(start_date=self.dates.one_month_ago)
        self.set_new_items()
        self.set_back_in_stock_items()
        self.product_images: dict = []
        self.mw_image_list: dict = []
        self.product_videos: dict = []
        self.sync_queue: dict = []
        self.queue_binding_ids = set()
        self.get_products()
        if not self.inventory_only and not self.initial_sync:
            self.process_media()
        if self.test_mode:
            self.sync_queue = self.test_queue
        self.get_sync_queue()

    def __str__(self):
        result = ''
        if self.sync_queue:
            result = f'Items to Process: {len(self.sync_queue)}\n'
        return result

    def get_products(self):
        """Get data for self.cp_items and self.mw_items. This is used for comparing CP to MW for deletions."""
        counterpoint_items = db.query(f"SELECT ITEM_NO FROM {Table.CP.Item.table} WHERE IS_ECOMM_ITEM = 'Y'")
        self.cp_items = [x[0] for x in counterpoint_items] if counterpoint_items else []

        middleware_items = db.query(f'SELECT ITEM_NO FROM {Table.Middleware.products}')
        self.mw_items = [x[0] for x in middleware_items] if middleware_items else []

    def get_sync_queue(self, test_mode=False):
        """Get all products that have been updated since the last sync"""
        if self.inventory_only:
            query = f"""
            SELECT ITEM.ITEM_NO, ITEM.{Table.CP.Item.Column.binding_id} as 'Binding ID'
            FROM {Table.CP.Item.table} ITEM
            INNER JOIN IM_INV INV on ITEM.ITEM_NO = INV.ITEM_NO
            WHERE INV.LST_MAINT_DT > '{self.last_sync: %Y-%m-%d %H:%M:%S}' and
            ITEM.{Table.CP.Item.Column.web_enabled} = 'Y'
            ORDER BY {Table.CP.Item.Column.binding_id} DESC"""
        else:
            query = f"""
            SELECT ITEM_NO, ITEM.{Table.CP.Item.Column.binding_id} as 'Binding ID'
            FROM {Table.CP.Item.table} ITEM
            WHERE ITEM.LST_MAINT_DT > '{self.last_sync: %Y-%m-%d %H:%M:%S}' and
            ITEM.{Table.CP.Item.Column.web_enabled} = 'Y'
            ORDER BY {Table.CP.Item.Column.binding_id} DESC
            """
        response = db.query(query)
        if response is not None:
            result = []
            for item in response:
                sku = item[0]
                binding_id = item[1]
                queue_payload = {}
                if binding_id is not None:
                    # Check if the binding ID matches the correct format. (e.g. B0001)
                    pattern = creds.Company.binding_id_format
                    if not bool(re.fullmatch(pattern, binding_id)):
                        message = f'Product {binding_id} has an invalid binding ID.'
                        Catalog.error_handler.add_error_v(error=message, origin='get_products()')
                        # Skip this product
                        continue

                    else:
                        # Get Parent to Process.
                        query = f"""
                        SELECT ITEM_NO
                        FROM {Table.CP.Item.table}
                        WHERE {Table.CP.Item.Column.binding_id} = '{binding_id}' AND 
                        {Table.CP.Item.Column.web_enabled} = 'Y' 
                        AND {Table.CP.Item.Column.is_parent} = 'Y'"""

                        get_parent_response = db.query(query)

                        if get_parent_response is not None:
                            # Parent(s) found
                            parent_list = [x[0] for x in get_parent_response]
                            # If multiple parents are found, choose the lowest price parent.
                            if len(parent_list) > 1:
                                Catalog.logger.warn(f'Multiple parents found for {binding_id}.')
                                # Set Parent Status for new parent.
                                parent_sku = Product.set_parent(binding_id=binding_id, remove_current=True)

                            else:
                                # Single Parent Found.
                                parent_sku = parent_list[0]
                        else:
                            # Missing Parent!
                            # Will choose the lowest price web enabled variant as the parent.
                            Catalog.logger.warn(f'Parent SKU not found for {binding_id}.')
                            parent_sku = Product.set_parent(binding_id=binding_id)

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
            if self.sync_queue:
                self.queue_binding_ids = set(x['binding_id'] for x in self.sync_queue if 'binding_id' in x)

    def process_product_deletes(self):
        # This compares the CP and MW product lists and deletes any products that are not in both lists.
        if self.verbose:
            Catalog.logger.info('Processing Product Deletions.')

        delete_targets = Catalog.get_deletion_target(secondary_source=self.mw_items, primary_source=self.cp_items)

        add_targets = []

        for item in self.sync_queue:
            if 'binding_id' not in item:
                # Check if the target product has a binding ID in the middleware database.
                mw_binding_id = Product.get_binding_id(item['sku'], middleware=True)
                if mw_binding_id:
                    # This is a former bound product. Delete it.
                    delete_targets.append(item['sku'])
            else:
                # These products have a binding ID. Get all family members of the binding ID.
                family_members = Product.get_family_members(binding_id=item['binding_id'], counterpoint=True)

                for member in family_members:
                    query = f"""
                    SELECT ID, BINDING_ID
                    FROM {Table.Middleware.products}
                    WHERE ITEM_NO = '{member}'
                    """
                    response = db.query(query)

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
        changes = False
        if delete_targets:
            changes = True
            Catalog.logger.info(f'Product Delete Targets: {delete_targets}')

            for x in delete_targets:
                Product.delete(sku=x, update_timestamp=True)
        else:
            if self.verbose:
                Catalog.logger.info('No products to delete.')

        if self.verbose:
            Catalog.logger.info('Processing Product Additions.')
        if add_targets:
            changes = True
            Catalog.logger.info(f'Product Add Targets: {add_targets}')

            for x in add_targets:
                parent_sku = x['parent']
                variant_sku = x['variant']

                # Get Product ID associated with item.
                product_id = Product.get_product_id(parent_sku)

                if product_id is not None:
                    Product.add_variant(product_id=product_id, variant_sku=variant_sku)
        else:
            if self.verbose:
                Catalog.logger.info('No products to add.')

        if changes:
            print('Getting Sync Queue after deletes/adds')
            self.get_sync_queue()
            print(self.sync_queue)

    def process_media(self):
        """Assesses Image folder. Deletes images from MW and Shopify.
        Updates LST_MAINT_DT in CP if new images have been added."""

        def process_images():
            if self.verbose:
                Catalog.logger.info('Processing Image Updates.')
            self.product_images = get_product_images(eh=self.eh, verbose=self.verbose)
            mw_images = db.Shopify.Product.Media.Image.get(column='IMAGE_NAME, SIZE')
            self.mw_image_list = [[x[0], x[1]] for x in mw_images] if mw_images else []

            delete_targets = Catalog.get_deletion_target(
                primary_source=self.product_images, secondary_source=self.mw_image_list
            )

            if delete_targets:
                delete_targets_set = list(set(x[0] for x in delete_targets))
                if self.verbose:
                    Catalog.logger.info(message=f'Delete Targets: {delete_targets_set}')
                for img_name in delete_targets_set:
                    if img_name == 'coming-soon.jpg':
                        # This block will run if you are dealing with a default image in the case of missing photos.
                        query = """
                        SELECT ITEM_NO, IMAGE_ID FROM SN_SHOP_IMAGES
                        WHERE IMAGE_NAME = 'coming-soon.jpg'
                        """
                        response = db.query(query)
                        if response is None:
                            continue

                        images = [{'sku': x[0], 'image_id': x[1]} for x in response]
                        for i in images:
                            sku = i['sku']
                            image_id = i['image_id']

                            # Check if sku has a image in the file explorer.
                            query = f"""
                            SELECT USR_PROF_ALPHA_16
                            FROM IM_ITEM WHERE ITEM_NO = '{sku}'
                            """
                            response = db.query(query)
                            if response is not None:
                                try:
                                    binding_id = response[0][0]
                                except:
                                    binding_id = None
                            # check itemImages for item's image using sku
                            local_images = Catalog.get_local_product_images(sku)
                            if binding_id:
                                local_binding_images = Catalog.get_local_product_images(binding_id)
                                local_images += local_binding_images

                            if len(local_images) > 0:
                                Image.delete(image_id=image_id)
                        break

                    else:
                        Catalog.logger.info(f'Deleting Image {img_name}.\n')
                        Image.delete(image_name=img_name)
            else:
                if self.verbose:
                    Catalog.logger.info('No image deletions found.')

            # Update Item LST_MAINT_DT if new images have been deleted/added/changed.
            update_list = delete_targets

            addition_targets = Catalog.get_deletion_target(
                primary_source=self.mw_image_list, secondary_source=self.product_images
            )

            if addition_targets:
                for x in addition_targets:
                    update_list.append(x)

            if update_list:
                sku_list = [x[0].split('.')[0].split('^')[0] for x in update_list]
                binding_list = [x for x in sku_list if x in Catalog.all_binding_ids]

                sku_list = tuple(sku_list)
                if binding_list:
                    if len(binding_list) > 1:
                        binding_list = tuple(binding_list)
                        where_filter = f' or {Table.CP.Item.Column.binding_id} in {binding_list}'
                    else:
                        where_filter = f" or {Table.CP.Item.Column.binding_id} = '{binding_list[0]}'"
                else:
                    where_filter = ''
                query = f"""
                    UPDATE {Table.CP.Item.table}
                    SET LST_MAINT_DT = GETDATE()
                    WHERE (ITEM_NO in {sku_list} {where_filter}) AND
                    {Table.CP.Item.Column.web_enabled} = 'Y'"""
                response = db.query(query, mapped=True)
                if response['code'] == 200:
                    if self.verbose:
                        Catalog.logger.info(
                            f'Process Media: Updated LST_MAINT_DT for {response['affected rows']} items.'
                        )

        def process_videos():
            if self.verbose:
                Catalog.logger.info('Processing Video Updates.')
            self.product_videos = db.CP.Product.Media.Video.get()
            mw_video_data = db.Shopify.Product.Media.Video.get(column='ITEM_NO, URL')
            mw_video_list = [[x[0], x[1]] for x in mw_video_data] if mw_video_data else []
            delete_targets = Catalog.get_deletion_target(
                primary_source=self.product_videos, secondary_source=mw_video_list
            )

            if delete_targets:
                Catalog.logger.info(f'Delete Targets: {delete_targets}')
                for x in delete_targets:
                    Catalog.logger.info(f'Deleting Video for:\nITEM: {x[0]}\nURL: {x[1]}\n')
                    Video.delete(sku=x[0], url=x[1])
            else:
                if self.verbose:
                    Catalog.logger.info('No video deletions found.')

        process_images()
        process_videos()

    def set_new_items(self):
        """Sets new items to the sync queue."""
        if self.new_items:
            new_items = [x[0] for x in self.new_items]
            db.CP.Product.add_to_new(new_items)
            db.CP.Product.remove_from_new(new_items)

    def set_back_in_stock_items(self):
        """Sets back in stock items to the sync queue."""
        if self.back_in_stock_items:
            back_in_stock_items = [x[0] for x in self.back_in_stock_items]
            db.CP.Product.add_to_back_in_stock(back_in_stock_items)
            db.CP.Product.remove_from_back_in_stock(back_in_stock_items)

    def sync(self, initial=False):
        """Syncs the catalog with Shopify. This will update products, categories, and media."""
        # get this at start of sync in case product timestamps were updated during promotion sync
        self.get_sync_queue()

        if not self.inventory_only:
            if not self.test_mode:
                self.category_tree.sync()

        if not self.inventory_only and not initial:
            self.process_product_deletes()

        if not self.sync_queue:
            if not self.inventory_only and not self.verbose:  # don't log this for inventory sync.
                Catalog.logger.info(f'No products to sync. Last Sync: {self.last_sync}')
        else:
            queue_length = len(self.sync_queue)
            success_count = 0
            fail_count = {'number': 0, 'items': []}

            if not self.inventory_only or self.verbose:
                Catalog.logger.info(f'Syncing {queue_length} products.', origin='CATALOG SYNC: ')

            def task(target):
                prod = Product(target, last_sync=self.last_sync, inventory_only=self.inventory_only)
                prod.get(last_sync=self.last_sync)
                if prod.validate():
                    return prod.process()
                else:
                    return False, target

            with ThreadPoolExecutor(max_workers=creds.Integrator.max_workers) as executor:
                results = executor.map(task, self.sync_queue)

                for x in results:
                    success, item = x
                    if success:
                        success_count += 1
                    else:
                        fail_count['number'] += 1
                        fail_count['items'].append(item)

            if not self.inventory_only:
                Catalog.logger.info(
                    '\n-----------------------\n'
                    'Catalog Sync Complete.\n'
                    f'Success Count: {success_count}\n'
                    f'Fail Count: {fail_count["number"]}\n'
                    f'Fail Items: {fail_count["items"]}\n'
                    '-----------------------\n'
                )

            if fail_count['items']:
                retry_success_count = 0
                retry_fail_count = {'number': 0, 'items': []}

                def retry(target):
                    Product.delete(sku=target['sku'])
                    return task(target)

                with ThreadPoolExecutor(max_workers=creds.Integrator.max_workers) as executor:
                    results = executor.map(retry, fail_count['items'])

                    for x in results:
                        success, item = x
                        if success:
                            retry_success_count += 1
                        else:
                            retry_fail_count['number'] += 1
                            retry_fail_count['items'].append(item)

                    Catalog.logger.info(
                        '\n-----------------------\n'
                        'Catalog Retry Complete.\n'
                        f'Success Count: {retry_success_count}\n'
                        f'Fail Count: {retry_fail_count["number"]}\n'
                        f'Fail Items: {retry_fail_count["items"]}\n'
                        '-----------------------\n'
                    )

    @staticmethod
    def get_deletion_target(primary_source, secondary_source):
        return [element for element in secondary_source if element not in primary_source]

    @staticmethod
    def get_local_product_images(sku):
        """Get local image information for product"""
        product_images = []
        photo_path = creds.Company.product_images
        list_of_files = os.listdir(photo_path)
        if list_of_files is not None:
            for x in list_of_files:
                if x.split('.')[0].split('^')[0].lower() == sku.lower():
                    product_images.append(x)
        return product_images

    def delete(products=True, collections=True):
        """Deletes all products, categories from Shopify and Middleware."""

        def delete_products():
            # Get all product IDs from Middleware
            query = f'SELECT DISTINCT PRODUCT_ID FROM {Table.Middleware.products}'
            response = db.query(query)
            product_id_list = [x[0] for x in response] if response else []

            while product_id_list:
                target = product_id_list.pop()
                try:
                    Shopify.Product.delete(product_id=target)
                except Exception as e:
                    Catalog.error_handler.add_error_v(
                        error=f'Error deleting product {target}. {e}', origin='delete_products()'
                    )
                db.Shopify.Product.delete(product_id=target)

            # Check for any remaining products in Shopify
            response = Shopify.Product.get_all()
            if response:
                for product in response:
                    Shopify.Product.delete(product)

        def delete_collections():
            # Get all categories from Middleware. Delete from Shopify and Middleware.
            query = f'SELECT DISTINCT COLLECTION_ID FROM {Table.Middleware.collections}'
            response = db.query(query)
            parent_category_list = [x[0] for x in response] if response else []
            while parent_category_list:
                target = parent_category_list.pop()
                Shopify.Collection.delete(collection_id=target)
                db.Shopify.Collection.delete(collection_id=target)

            # Check for any remaining categories in Shopify
            response = Shopify.Collection.get()
            if response:
                for collection in response:
                    Shopify.Collection.delete(collection)

        if products:
            delete_products()
        if collections:
            delete_collections()

    def reupload_all_media():
        # Get all e-commerce products
        product_ids = db.Shopify.Product.get_id(all=True)
        errors = []
        for product_id in product_ids:
            try:
                Catalog.logger.info('Processing', product_id)
                # Get SKU from product Id
                sku = db.Shopify.Product.get_sku(product_id=product_id)
                item = {'sku': sku}
                binding_id = db.Shopify.Product.get_binding_id(product_id=product_id)
                if binding_id:
                    item['binding_id'] = binding_id
                # Delete all media for product
                Shopify.Product.Media.delete(product_id=product_id, media_type='all')
                db.Shopify.Product.Media.delete(product_id=product_id)
                Shopify.logger.info('Deleted media for', sku)
                Shopify.logger.info('Syncing', sku)
                # Process Product
                cat = Catalog(test_mode=True, test_queue=[item])
                cat.sync()
            except Exception as e:
                Catalog.error_handler.add_error_v(
                    f'Error processing {product_id}: {e}', origin='Catalog Sync', traceback=tb()
                )

        Catalog.logger.info(errors)


class Collections:
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, last_sync=datetime(1970, 1, 1), verbose=False):
        self.last_sync: datetime = last_sync
        self.verbose: bool = verbose
        self.categories: set[Collection] = set()
        self.heads: list[Collection] = []  # top level collections
        self.get_tree()

    def __str__(self):
        def print_collections(category, level=0):
            res = (
                f"{'    ' * level}Collection Name: {category.category_name}\n"
                f"{'    ' * level}---------------------------------------\n"
                f"{'    ' * level}Counterpoint Category ID: {category.cp_categ_id}\n"
                f"{'    ' * level}Counterpoint Parent ID: {category.cp_parent_id}\n"
                f"{'    ' * level}Shopify Collection ID: {category.collection_id}\n"
                f"{'    ' * level}Shopify Parent ID: {category.shopify_parent_id}\n"
                f"{'    ' * level}Sort Order: {category.sort_order}\n"
                f"{'    ' * level}Last Maintenance Date: {category.lst_maint_dt}\n\n"
            )
            for child in category.children:
                res += print_collections(child, level + 1)
            return res

        result = ''
        for root in self.heads:
            result += print_collections(root)

        return result

    def get_tree(self):
        """Updates middleware category tables with Counterpoint category information."""
        query = f"""
        SELECT cp.CATEG_ID, ISNULL(cp.PARENT_ID, 0), mw.COLLECTION_ID, mw.MENU_ID, cp.DESCR, cp.DISP_SEQ_NO, cp.HTML_DESCR, 
        cp.LST_MAINT_DT, mw.CP_CATEG_ID, mw.is_visible, mw.IMG_SIZE
        FROM EC_CATEG cp
        FULL OUTER JOIN {Table.Middleware.collections} mw on cp.CATEG_ID=mw.CP_CATEG_ID
        """
        response = db.query(query)
        if response:
            for x in response:
                cp_categ_id = x[0]
                cp_parent_id = x[1]
                mw_cp_categ_id = x[8]  # Middleware Counterpoint Category ID

                if cp_categ_id is None:  # These are categories that exist in the middleware but not CP
                    Collections.delete_collection(mw_cp_categ_id)
                    continue  # Skip deleted categories

                if cp_parent_id == '0' and cp_categ_id == '0':
                    continue  # Skip the root category

                # Create Category Object
                cat = Collection(
                    cp_categ_id=cp_categ_id,
                    cp_parent_id=cp_parent_id,
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
                    db.Shopify.Collection.insert(cat)

                else:
                    if cat.lst_maint_dt > self.last_sync:
                        db.Shopify.Collection.update(cat)

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
        self.update_middleware()

    def process_collections(self):
        queue: list[Collection] = []

        def collections_h(category: Collection):
            # Get Shopify Collection ID and Parent ID
            if category.lst_maint_dt > self.last_sync:
                queue.append(category)
                if category.collection_id is None:
                    category.collection_id = Shopify.Collection.create(category.get_category_payload())

            for child in category.children:
                collections_h(child)

        for head in self.heads:
            collections_h(head)

        self.process_images(queue)

        for category in queue:
            Shopify.Collection.update(category.get_category_payload())

    def process_menus(self):
        """Recursively updates menus in Shopify."""
        main_menu = {'id': creds.Shopify.Menu.main, 'title': 'Menu menu', 'handle': 'main-menu', 'items': []}

        def menu_helper(category: Collection):
            # Sort category children by sort order
            category.children.sort(key=lambda x: x.sort_order)

            menu_item = {
                'title': category.name,
                'type': 'COLLECTION',
                'resourceId': f'gid://shopify/Collection/{category.collection_id}',
                'items': [menu_helper(child) for child in category.children],
            }

            if category.menu_id:
                menu_item['id'] = f'gid://shopify/MenuItem/{category.menu_id}'

            return menu_item

        # Recursively call this function for each child category
        for category in self.heads:
            main_menu['items'].append(menu_helper(category))

        # # Add the Landing Page to the Main Menu
        main_menu['items'].append(
            {
                'title': 'Landscape Design',
                'type': 'PAGE',
                'resourceId': 'gid://shopify/Page/98894217383',
                'items': [],
            }
        )

        response = Shopify.Menu.update(main_menu)

        def get_menu_ids(response):
            categories = []

            for item in response['menuUpdate']['menu']['items']:
                menu_item_id = item['id']
                title = item['title']
                children = item['items']
                categories.append({'id': menu_item_id, 'title': title})
                for child in children:
                    child_id = child['id']
                    child_title = child['title']
                    categories.append({'id': child_id, 'title': child_title})

            for cat in categories:
                for category in self.categories:
                    if cat['title'] == category.name:
                        category.menu_id = cat['id'].split('/')[-1]

        get_menu_ids(response)

    def update_middleware(self):
        # Update Entire Category Tree in Middleware
        def update_helper(collection: Collection):
            if collection.lst_maint_dt > self.last_sync:
                db.Shopify.Collection.update(collection)
            for child in collection.children:
                update_helper(child)

        for collection in self.heads:
            update_helper(collection)

    def process_images(self, queue: list['Collection']):
        file_list = []
        stagedUploadsCreateVariables = {'input': []}

        for category in queue:
            if category.image_path:
                image_size = get_filesize(category.image_path)
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
                variables=stagedUploadsCreateVariables, file_list=file_list, verbose=self.verbose
            )
            for file in uploaded_files:
                for category in queue:
                    if file['file_path'] == category.image_path:
                        category_image_url = file['url']
                        category.image_url = category_image_url

    @staticmethod
    def delete_collection(cp_categ_id):
        Collections.logger.info(f'Deleting Category: {cp_categ_id}')
        query = f"""
        SELECT COLLECTION_ID
        FROM {Table.Middleware.collections}
        WHERE CP_CATEG_ID = {cp_categ_id}
        """
        response = db.query(query)
        try:
            collection_id = response[0][0]
        except:
            collection_id = None

        if collection_id:
            Shopify.Collection.delete(collection_id)

        db.Shopify.Collection.delete(cp_categ_id=cp_categ_id)


class Collection:
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
        self.children: list[Collection] = []

    def __str__(self):
        return (
            f'Category Name: {self.name}\n'
            f'---------------------------------------\n'
            f'Counterpoint Category ID: {self.cp_categ_id}\n'
            f'Counterpoint Parent ID: {self.cp_parent_id}\n'
            f'Shopify Category ID: {self.collection_id}\n'
            f'Shopify Parent ID: {self.shopify_parent_id}\n'
            f'Sort Order: {self.sort_order}\n'
            f'Last Maintenance Date: {self.lst_maint_dt}\n\n'
        )

    def add_child(self, child):
        self.children.append(child)

    def get_shopify_id(self):
        query = f"""
        SELECT COLLECTION_ID
        FROM {Table.Middleware.collections}
        WHERE CP_CATEG_ID = {self.cp_categ_id}
        """
        response = db.query(query)
        if response is not None:
            shopify_category_id = response[0][0] if response[0][0] is not None else None
            if shopify_category_id is not None:
                self.collection_id = response[0][0]
            else:
                self.shopify_parent_id = self.get_shopify_parent_id()
                category_payload = self.get_category_payload()
                self.collection_id = Shopify.Collection.create(category_payload)

    def get_shopify_parent_id(self):
        query = f"""
        SELECT COLLECTION_ID
        FROM {Table.Middleware.collections}
        WHERE CP_CATEG_ID = (SELECT CP_PARENT_ID 
                            FROM {Table.Middleware.collections} 
                            WHERE CP_CATEG_ID = {self.cp_categ_id})
        """
        response = db.query(query)
        return response[0][0] if response and response[0][0] is not None else 0

    def get_full_custom_url_path(self):
        parent_id = self.cp_parent_id
        url_path = []
        url_path.append(parse_custom_url(self.name))
        while parent_id != 0:
            query = f'SELECT CATEG_NAME, CP_PARENT_ID FROM {Table.Middleware.collections} WHERE CP_CATEG_ID = {parent_id}'
            response = db.query(query)
            if response:
                url_path.append(parse_custom_url(response[0][0] or ''))
                parent_id = response[0][1]
            else:
                break
        return f"/{"/".join(url_path[::-1])}/"

    def get_category_image_path(self, local=True):
        image_name = str(self.handle)[1:-1].replace('/', '_').replace(' ', '-') + '.jpg'
        local_path = f'{creds.API.public_files_local_path}/{creds.Company.category_images}/{image_name}'
        if os.path.exists(local_path):
            return local_path

    def get_category_payload(self):
        payload = {'input': {'title': self.name, 'handle': self.handle, 'sortOrder': 'BEST_SELLING'}}
        # New items - Add store channel
        if not self.collection_id:
            payload['input']['publications'] = {'publicationId': creds.Shopify.SalesChannel.online_store}
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
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler
    """Product objects represent single products or bound products. Bound products are products that have
    multiple variants associated with them through a shared binding ID."""

    def __init__(self, product_data, last_sync=datetime(1970, 1, 1), verbose=False, inventory_only=False):
        self.last_sync = last_sync
        self.verbose = verbose
        self.inventory_only = inventory_only
        self.product_data = product_data
        self.sku = product_data['sku']
        self.binding_id = product_data['binding_id'] if 'binding_id' in product_data else None
        # Determine if Bound
        self.is_bound = True if self.binding_id else False
        # self.variants will be list of variant objects
        self.variants: list[Variant] = []
        # self.parent will be a list of parent products. If length of list > 1, product validation will fail
        self.parent: list[Variant] = []

        # A list of image objects
        self.images: list[Image] = []
        self.default_image: bool = False
        self.expected_media_order = []
        self.videos: list[Video] = []
        self.media: list[Video | Image] = []  # list of all media objects (Images and Videos)
        self.reorder_media_queue = []
        self.has_new_media: bool = False
        self.has_variant_image: bool = False  # used during image processing

        # Product Information
        self.product_id = None
        self.option_id = None
        self.web_title = None
        self.type = None
        self.long_descr = None
        self.default_price = None
        self.cost = None
        self.sale_price = None
        self.taxable = None
        self.weight = None
        self.buffered_quantity = 0
        self.brand = None
        self.html_description = None
        self.tags = None
        self.meta_title = None
        self.meta_description = None
        self.visible: bool = False
        self.track_inventory: bool = False
        self.featured: bool = False
        self.is_new: bool = False
        self.is_back_in_stock: bool = False
        self.sort_order = None
        self.in_store_only: bool = False
        self.is_preorder = False
        self.is_preorder_only = False
        self.is_workshop = False
        self.preorder_message = None
        self.preorder_release_date = None
        self.alt_text_1 = None
        self.alt_text_2 = None
        self.alt_text_3 = None
        self.alt_text_4 = None
        self.custom_url = None

        # Custom Fields
        self.meta_botanical_name = {'id': None, 'value': None}
        self.meta_climate_zone = {'id': None, 'value': None}
        self.meta_climate_zone_list = {'id': None, 'value': None}
        self.meta_plant_type = {'id': None, 'value': None}
        self.meta_height = {'id': None, 'value': None}
        self.meta_width = {'id': None, 'value': None}
        self.meta_light_requirements = {'id': None, 'value': None}
        self.meta_size = {'id': None, 'value': None}
        self.meta_bloom_season = {'id': None, 'value': None}
        self.meta_features = {'id': None, 'value': None}
        self.meta_colors = {'id': None, 'value': None}
        self.meta_bloom_color = {'id': None, 'value': None}
        self.meta_is_preorder = {'id': None, 'value': None}
        self.meta_preorder_message = {'id': None, 'value': None}
        self.meta_preorder_release_date = {'id': None, 'value': None}
        self.meta_is_featured = {'id': None, 'value': None}
        self.meta_is_new = {'id': None, 'value': None}
        self.meta_is_back_in_stock = {'id': None, 'value': None}
        self.meta_in_store_only = {'id': None, 'value': None}
        self.meta_is_on_sale = {'id': None, 'value': None}
        self.meta_sale_description = {'id': None, 'value': None}

        self.lst_maint_dt = datetime(1970, 1, 1)

        # E-Commerce Categories
        self.cp_ecommerce_categories = []
        self.shopify_collections = []

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
                    result += f'Image: {image.name}\n'
                    result += f'    Thumbnail: {image.is_thumbnail}\n'
                    result += f'    Variant Image: {image.is_variant_image}\n'
                    result += f'    Sort Order: {image.sort_order}\n'
                result += line
                variant_index += 1
        return result

    def get(self, last_sync):
        """Get product details from Counterpoint and Middleware"""

        def get_bound_product_details():
            # clear children list
            self.variants = []

            query = f"""
                SELECT ITEM_NO
                FROM {Table.CP.Item.table}
                WHERE {Table.CP.Item.Column.binding_id} = '{self.binding_id}' and IS_ECOMM_ITEM = 'Y'
                ORDER BY PRC_1
                """
            # Get children and append to child list in order of price
            response = db.query(query)
            if response is not None:
                # Create Product objects for each child and add object to bound parent list
                for item in response:
                    variant = Variant(item[0], last_sync=last_sync)
                    self.variants.append(variant)

            # Sort self.variants by variant.is_parent so parent is processed first.
            self.variants.sort(key=lambda x: x.is_parent, reverse=True)

            # Set parent
            self.parent = [item for item in self.variants if item.is_parent]

            # Inherit Product Information from Parent Item
            for bound in self.variants:
                if bound.is_parent:
                    self.product_id = bound.product_id
                    self.web_title = bound.web_title
                    # self.type = bound.type
                    self.default_price = bound.price_1
                    self.cost = bound.cost
                    self.sale_price = bound.price_2
                    self.brand = bound.brand
                    self.sort_order = bound.sort_order
                    self.html_description = bound.html_description
                    self.tags = bound.tags
                    self.meta_title = bound.meta_title
                    self.meta_description = bound.meta_description
                    self.visible = bound.visible
                    self.track_inventory = bound.track_inventory
                    self.in_store_only = bound.in_store_only
                    self.featured = bound.featured
                    self.is_new = bound.is_new
                    self.is_back_in_stock = bound.is_back_in_stock
                    self.cp_ecommerce_categories = bound.cp_ecommerce_categories
                    self.long_descr = bound.long_descr
                    self.is_preorder = bound.is_preorder
                    self.preorder_release_date = bound.preorder_release_date
                    self.preorder_message = bound.preorder_message

                    # Media
                    self.videos = bound.videos

                    # Product Description
                    self.product_id = bound.product_id
                    self.web_title = bound.web_title
                    self.long_descr = bound.long_descr
                    self.html_description = bound.html_description
                    self.meta_title = bound.meta_title
                    self.meta_description = bound.meta_description
                    self.brand = bound.brand
                    self.sort_order = bound.sort_order
                    # Prices/Cost
                    self.default_price = bound.price_1
                    self.cost = bound.cost
                    self.sale_price = bound.price_2
                    self.taxable = bound.taxable
                    # Inventory
                    self.buffered_quantity = bound.quantity_available - bound.buffer
                    if self.buffered_quantity < 0:
                        self.buffered_quantity = 0
                    # Collections and Tags
                    self.tags = bound.tags
                    self.cp_ecommerce_categories = bound.cp_ecommerce_categories
                    # Statuses
                    self.in_store_only = bound.in_store_only
                    self.visible = bound.visible
                    self.featured = bound.featured
                    self.is_preorder = bound.is_preorder
                    self.preorder_release_date = bound.preorder_release_date
                    self.preorder_message = bound.preorder_message
                    # Meta Fields
                    self.meta_botanical_name = bound.meta_botanical_name
                    self.meta_climate_zone = bound.meta_climate_zone
                    self.meta_climate_zone_list = bound.meta_climate_zone_list
                    self.meta_plant_type = bound.meta_plant_type
                    self.meta_light_requirements = bound.meta_light_requirements
                    self.meta_height = bound.meta_height
                    self.meta_width = bound.meta_width
                    self.meta_size = bound.meta_size
                    self.meta_bloom_season = bound.meta_bloom_season
                    self.meta_features = bound.meta_features
                    self.meta_colors = bound.meta_colors
                    self.meta_bloom_color = bound.meta_bloom_color
                    self.meta_is_preorder = bound.meta_is_preorder
                    self.meta_preorder_message = bound.meta_preorder_message
                    self.meta_preorder_release_date = bound.meta_preorder_release_date
                    self.meta_is_featured = bound.meta_is_featured
                    self.meta_in_store_only = bound.meta_in_store_only
                    self.meta_is_on_sale = bound.meta_is_on_sale
                    self.meta_sale_description = bound.meta_sale_description
                    self.meta_is_new = bound.meta_is_new
                    self.meta_is_back_in_stock = bound.meta_is_back_in_stock

                    # Shipping
                    self.weight = bound.weight
                    # Last Maintenance Date
                    self.lst_maint_dt = bound.lst_maint_dt

            def get_binding_id_images():
                binding_images = []
                photo_path = creds.Company.product_images
                list_of_files = os.listdir(photo_path)
                if list_of_files is not None:
                    for file in list_of_files:
                        if file.split('.')[0].split('^')[0].lower() == self.binding_id.lower():
                            binding_images.append(file)

                total_binding_images = len(binding_images)

                if total_binding_images > 0:
                    for image in binding_images:
                        binding_img = Image(image)

                        if binding_img.validate():
                            self.images.append(binding_img)
                        else:
                            Product.error_handler.add_error_v(
                                error=f'Image {binding_img.name} failed validation. Image will not be added to product.',
                                origin='Image Validation',
                            )

            if not self.inventory_only:
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

            self.lst_maint_dt = max(lst_maint_dt_list) if lst_maint_dt_list else datetime(1970, 1, 1)
            self.default_image = len(self.images) == 1 and self.images[0].name == 'coming-soon.jpg'

        def get_single_product_details():
            self.variants.append(Variant(self.sku, last_sync=last_sync, inventory_only=self.inventory_only))
            single = self.variants[0]
            # Product Description
            self.product_id = single.product_id
            self.web_title = single.web_title
            # self.type = single.type
            self.long_descr = single.long_descr
            self.html_description = single.html_description
            self.meta_title = single.meta_title
            self.meta_description = single.meta_description
            self.brand = single.brand
            self.sort_order = single.sort_order
            # Prices/Cost
            self.default_price = single.price_1
            self.cost = single.cost
            self.sale_price = single.price_2
            self.taxable = single.taxable
            # Inventory
            self.buffered_quantity = single.quantity_available - single.buffer
            if self.buffered_quantity < 0:
                self.buffered_quantity = 0
            # Collections and Tags
            self.tags = single.tags
            self.cp_ecommerce_categories = single.cp_ecommerce_categories
            # Media
            self.images = single.images
            self.videos = single.videos
            # Statuses
            self.in_store_only = single.in_store_only
            self.visible = single.visible
            self.track_inventory = single.track_inventory
            self.featured = single.featured
            self.is_new = single.is_new
            self.is_back_in_stock = single.is_back_in_stock
            self.is_preorder = single.is_preorder
            self.preorder_release_date = single.preorder_release_date
            self.preorder_message = single.preorder_message
            # Meta Fields
            self.meta_botanical_name = single.meta_botanical_name
            self.meta_climate_zone = single.meta_climate_zone
            self.meta_climate_zone_list = single.meta_climate_zone_list
            self.meta_light_requirements = single.meta_light_requirements
            self.meta_plant_type = single.meta_plant_type
            self.meta_height = single.meta_height
            self.meta_width = single.meta_width
            self.meta_size = single.meta_size
            self.meta_bloom_season = single.meta_bloom_season
            self.meta_features = single.meta_features
            self.meta_colors = single.meta_colors
            self.meta_bloom_color = single.meta_bloom_color
            self.meta_is_preorder = single.meta_is_preorder
            self.meta_preorder_message = single.meta_preorder_message
            self.meta_preorder_release_date = single.meta_preorder_release_date
            self.meta_is_featured = single.meta_is_featured
            self.meta_in_store_only = single.meta_in_store_only
            self.meta_is_on_sale = single.meta_is_on_sale
            self.meta_sale_description = single.meta_sale_description
            self.meta_is_new = single.meta_is_new
            self.meta_is_back_in_stock = single.meta_is_back_in_stock
            # Shipping
            self.weight = single.weight
            # Last Maintenance Date
            self.lst_maint_dt = single.lst_maint_dt

        if self.is_bound:
            get_bound_product_details()
        else:
            get_single_product_details()

        self.shopify_collections = self.get_shopify_collections()

        self.type = self.get_collection_names()[0] if self.get_collection_names() else None

        if self.tags == 'Workshop':
            self.is_workshop = True

        # Now all images are in self.images list and are in order by binding img first then variant img
        # Add all images and videos to self.media list
        self.media = self.images + self.videos

    def validate(self):
        """Validate product inputs to check for errors in user input"""
        check_web_title = True
        check_for_missing_categories = False
        check_html_description = False
        min_description_length = 20
        check_missing_images = True
        check_for_item_cost = False

        if self.inventory_only:
            return True

        def set_parent(status: bool = True) -> None:
            """Target lowest price item in family to set as parent."""
            # Reestablish parent relationship
            flag = 'Y' if status else 'N'

            target_item = min(self.variants, key=lambda x: x.price_1).sku

            query = f"""
                UPDATE {Table.CP.Item.table}
                SET {Table.CP.Item.Column.is_parent} = '{flag}', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO = '{target_item}'
                """
            db.query(query)
            Product.logger.info(f'Parent status set to {flag} for {target_item}')
            return self.get(last_sync=self.last_sync)

        if self.is_bound:
            # Test for missing variant names
            for child in self.variants:
                if not child.variant_name:
                    message = f'Product {child.sku} is missing a variant name. Validation failed.'
                    Product.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False

            # Check for duplicate variant names
            variant_names = [x.variant_name for x in self.variants]
            if len(variant_names) != len(set(variant_names)):
                message = f'Product {self.binding_id} has duplicate variant names. Validation failed.'
                Product.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

            # Check for single item with binding
            if len(self.variants) == 1:
                message = f'Product {self.binding_id} has only one variant. self.is_bound=False.'
                Product.logger.warn(message)
                self.is_bound = False

        # ALL PRODUCTS
        if check_web_title:
            # Test for missing web title
            if not self.web_title:
                if not self.long_descr:
                    message = (
                        f'Product {self.binding_id} is missing a web title and long description. Validation failed.'
                    )
                    Product.error_handler.add_error_v(error=message, origin='Input Validation')
                    return False
                else:
                    message = f'Product {self.binding_id} is missing a web title. Will set to long description.'
                    Product.logger.warn(message)

                    if self.is_bound:
                        # Bound product: use binding key and parent variant
                        query = f"""
                            UPDATE {Table.CP.Item.table}
                            SET {Table.CP.Item.Column.web_title} = '{self.long_descr}'
                            WHERE {Table.CP.Item.Column.binding_id} = '{self.binding_id}' and {Table.CP.Item.Column.is_parent} = 'Y'"""

                    # Single Product use sku
                    else:
                        query = f"""
                            UPDATE {Table.CP.Item.table}
                            SET {Table.CP.Item.Column.web_title} = '{self.long_descr}'
                            WHERE ITEM_NO = '{self.sku}'"""

                        db.query(query)
                        Product.logger.info(f'Web Title set to {self.web_title}')
                        self.web_title = self.long_descr

            # Test for dupicate web title
            if self.web_title is not None:
                if self.is_bound:
                    # For bound products, look for matching web titles OUTSIDE of the current binding id
                    query = f"""
                        SELECT COUNT(ITEM_NO)
                        FROM {Table.CP.Item.table}
                        WHERE {Table.CP.Item.Column.web_title} = '{self.web_title.replace("'", "''")}' AND {Table.CP.Item.Column.binding_id} != '{self.binding_id}' AND IS_ECOMM_ITEM = 'Y'"""

                else:
                    query = f"""
                        SELECT COUNT(ITEM_NO)
                        FROM {Table.CP.Item.table}
                        WHERE {Table.CP.Item.Column.web_title} = '{self.web_title.replace("'", "''")}' AND IS_ECOMM_ITEM = 'Y'"""

                response = db.query(query)

                if response:
                    if response[0][0] > 1:
                        Product.logger.warn(
                            f'Product {self.binding_id} has a duplicate web title. Will Append Sku to Web Title.'
                        )

                        if self.is_bound:
                            new_web_title = f'{self.web_title} - {self.binding_id}'
                        else:
                            new_web_title = f'{self.web_title} - {self.sku}'

                        self.web_title = new_web_title

                        Product.logger.info(f'New Web Title: {self.web_title}')
                        if self.is_bound:
                            # Update Parent Variant
                            query = f"""
                                UPDATE {Table.CP.Item.table}
                                SET {Table.CP.Item.Column.web_title} = '{self.web_title.replace("'", "''")}'
                                WHERE {Table.CP.Item.Column.binding_id} = '{self.binding_id}' and {Table.CP.Item.Column.is_parent} = 'Y'
                                
                                """
                        else:
                            # Update Single Product
                            query = f"""
                                UPDATE {Table.CP.Item.table}
                                SET {Table.CP.Item.Column.web_title} = '{self.web_title.replace("'", "''")}'
                                WHERE ITEM_NO = '{self.sku}'"""
                        db.query(query)

        # Test for missing html description
        if check_html_description:
            if len(self.html_description) < min_description_length:
                message = f'Product {self.binding_id} is missing an html description. Validation failed.'
                Product.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

        # Test for missing E-Commerce Categories
        if check_for_missing_categories:
            if not self.shopify_collections:
                message = f'Product {self.binding_id} is missing E-Commerce Categories. Validation failed.'
                Product.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

        # Test for missing cost
        if check_for_item_cost:
            if self.cost == 0:
                message = f'Product {self.sku} is missing a cost. Validation failed.'
                Product.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

        # Test for missing price 1
        if self.default_price == 0:
            if self.in_store_only:
                if self.verbose:
                    Product.logger.warn(f'In-Store-Only Product {self.sku} is missing a price 1. Will Pass.')
                pass
            else:
                message = f'Product {self.sku} is missing a price 1. Validation failed.'
                Product.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

        if check_html_description:
            # Test for missing html description
            if len(self.html_description) < min_description_length:
                message = f'Product {self.sku} is missing an html description. Validation failed.'
                Product.error_handler.add_error_v(error=message, origin='Input Validation')
                return False

        if check_missing_images:
            missing = False
            if len(self.images) == 0:
                missing = True

            elif len(self.images) == 1:
                if self.images[0].name == 'coming-soon.jpg':
                    missing = True

            if missing:
                if creds.Integrator.set_missing_image_active:
                    message = f'Product {self.binding_id} is missing images. Will use default image.'
                    if self.verbose:
                        Product.logger.warn(message)

                    default_image = Image(
                        image_name='coming-soon.jpg',
                        product_id=self.product_id,
                        sku=self.sku,
                        image_url=creds.Integrator.default_image_url,
                        verbose=self.verbose,
                    )
                    self.images.append(default_image)
                    self.media.append(default_image)
                    self.default_image = True

                else:
                    message = f'Product {self.binding_id} is missing images. Will set visibility to draft.'
                    if self.verbose:
                        Product.logger.warn(message)
                    self.visible = False

        # BOUND PRODUCTS
        if self.is_bound:
            if check_web_title:
                for child in self.variants:
                    if not child.is_parent:
                        if child.web_title == self.web_title:
                            Product.logger.warn(
                                f'Non-Parent Variant {child.sku} has a web title. Will remove from child.'
                            )
                            child.web_title = ''
                            query = f"""
                                UPDATE {Table.CP.Item.table}
                                SET {Table.CP.Item.Column.web_title} = NULL
                                WHERE ITEM_NO = '{child.sku}'"""
                            db.query(query)

        return True

    def get_current_collections(self):
        try:
            query = f"""
                SELECT CATEG_ID FROM SN_SHOP_PROD
                WHERE ITEM_NO = '{self.sku}'
                """

            response = db.query(query)

            try:
                return [int(x) for x in response[0][0].split(',')]
            except:
                if self.verbose:
                    Product.logger.warn(f'Error getting current collections for {self.sku}, Query: {query}')
                return []
        except:
            Product.error_handler.add_error_v(
                error='Error getting current collections', origin='Input Validation', traceback=tb()
            )
            return []

    def get_collections_to_leave(self):
        """
        - This assumes that shopify_collections is a
          list of collections we want the product to be in.
        - Also assuming that the current collections in
          SN_SHOP_PROD are the collections that the product is currently in.
        """

        try:
            collections_to_leave = self.get_current_collections()
            for desired_collection in self.shopify_collections:
                if desired_collection in collections_to_leave:
                    collections_to_leave.remove(desired_collection)
            return collections_to_leave
        except:
            return []

    def get_payload(self):
        """Build the payload for creating a product in Shopify.
        This will include all variants, images, and custom fields."""

        def get_metafields():
            result = []

            # Botanical Name
            if self.meta_botanical_name['value']:
                botantical_name_data = {'value': self.meta_botanical_name['value']}
                if self.meta_botanical_name['id']:
                    botantical_name_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_botanical_name['id']}'
                else:
                    botantical_name_data['namespace'] = Catalog.metafields['Botanical Name']['NAME_SPACE']
                    botantical_name_data['key'] = Catalog.metafields['Botanical Name']['META_KEY']
                    botantical_name_data['type'] = Catalog.metafields['Botanical Name']['TYPE']

                result.append(botantical_name_data)

            elif self.meta_botanical_name['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_botanical_name['id'])
                self.meta_botanical_name['id'] = None

            # Climate Zone
            if self.meta_climate_zone['value']:
                climate_zone_data = {'value': self.meta_climate_zone['value']}
                if self.meta_climate_zone['id']:
                    climate_zone_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_climate_zone['id']}'
                else:
                    climate_zone_data['namespace'] = Catalog.metafields['Growing Zone']['NAME_SPACE']
                    climate_zone_data['key'] = Catalog.metafields['Growing Zone']['META_KEY']
                    climate_zone_data['type'] = Catalog.metafields['Growing Zone']['TYPE']

                result.append(climate_zone_data)

            elif self.meta_climate_zone['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_climate_zone['id'])
                self.meta_climate_zone['id'] = None

            # Climate Zone List
            if self.meta_climate_zone_list['value']:
                climate_zone_list_data = {'value': json.dumps(self.meta_climate_zone_list['value'])}
                if self.meta_climate_zone_list['id']:
                    climate_zone_list_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_climate_zone_list["id"]}'
                else:
                    climate_zone_list_data['namespace'] = Catalog.metafields['Growing Zone List']['NAME_SPACE']
                    climate_zone_list_data['key'] = Catalog.metafields['Growing Zone List']['META_KEY']
                    climate_zone_list_data['type'] = Catalog.metafields['Growing Zone List']['TYPE']

                result.append(climate_zone_list_data)
            elif self.meta_climate_zone_list['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_climate_zone_list['id'])
                self.meta_climate_zone_list['id'] = None

            if self.meta_plant_type['value']:
                plant_type_data = {'value': self.meta_plant_type['value']}
                if self.meta_plant_type['id']:
                    plant_type_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_plant_type["id"]}'
                else:
                    plant_type_data['namespace'] = Catalog.metafields['Plant Type']['NAME_SPACE']
                    plant_type_data['key'] = Catalog.metafields['Plant Type']['META_KEY']
                    plant_type_data['type'] = Catalog.metafields['Plant Type']['TYPE']

                result.append(plant_type_data)

            elif self.meta_plant_type['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_plant_type['id'])
                self.meta_plant_type['id'] = None

            # Mature Height and Width
            if self.meta_height['value']:
                height_data = {'value': json.dumps(self.meta_height['value'])}
                if self.meta_height['id']:
                    height_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_height["id"]}'
                else:
                    height_data['namespace'] = Catalog.metafields['Mature Height']['NAME_SPACE']
                    height_data['key'] = Catalog.metafields['Mature Height']['META_KEY']
                    height_data['type'] = Catalog.metafields['Mature Height']['TYPE']

                result.append(height_data)

            elif self.meta_height['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_height['id'])
                self.meta_height['id'] = None

            if self.meta_width['value']:
                width_data = {'value': json.dumps(self.meta_width['value'])}
                if self.meta_width['id']:
                    width_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_width["id"]}'
                else:
                    width_data['namespace'] = Catalog.metafields['Mature Width']['NAME_SPACE']
                    width_data['key'] = Catalog.metafields['Mature Width']['META_KEY']
                    width_data['type'] = Catalog.metafields['Mature Width']['TYPE']

                result.append(width_data)
            elif self.meta_width['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_width['id'])
                self.meta_width['id'] = None

            # Light Requirements
            if self.meta_light_requirements['value']:
                light_requirements_data = {'value': json.dumps(self.meta_light_requirements['value'])}
                if self.meta_light_requirements['id']:
                    light_requirements_data['id'] = (
                        f'{Shopify.Metafield.prefix}{self.meta_light_requirements["id"]}'
                    )
                else:
                    light_requirements_data['namespace'] = Catalog.metafields['Light Requirements']['NAME_SPACE']
                    light_requirements_data['key'] = Catalog.metafields['Light Requirements']['META_KEY']
                    light_requirements_data['type'] = Catalog.metafields['Light Requirements']['TYPE']

                result.append(light_requirements_data)

            elif self.meta_light_requirements['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_light_requirements['id'])
                self.meta_light_requirements['id'] = None

            # Size - *This only applies to single products*
            if not self.binding_id:
                if self.meta_size['value']:
                    size_data = {'value': self.meta_size['value']}
                    if self.meta_size['id']:
                        size_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_size["id"]}'
                    else:
                        size_data['namespace'] = Catalog.metafields['Size']['NAME_SPACE']
                        size_data['key'] = Catalog.metafields['Size']['META_KEY']
                        size_data['type'] = Catalog.metafields['Size']['TYPE']

                    result.append(size_data)
                elif self.meta_size['id']:
                    Shopify.Metafield.delete(metafield_id=self.meta_size['id'])
                    self.meta_size['id'] = None

            # Bloom Season
            if self.meta_bloom_season['value']:
                bloom_season_data = {'value': json.dumps(self.meta_bloom_season['value'])}
                if self.meta_bloom_season['id']:
                    bloom_season_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_bloom_season["id"]}'
                else:
                    bloom_season_data['namespace'] = Catalog.metafields['Bloom Season']['NAME_SPACE']
                    bloom_season_data['key'] = Catalog.metafields['Bloom Season']['META_KEY']
                    bloom_season_data['type'] = Catalog.metafields['Bloom Season']['TYPE']

                result.append(bloom_season_data)

            elif self.meta_bloom_season['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_bloom_season['id'])
                self.meta_bloom_season['id'] = None

            # Features
            if self.meta_features['value']:
                features_data = {'value': json.dumps(self.meta_features['value'])}
                if self.meta_features['id']:
                    features_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_features["id"]}'
                else:
                    features_data['namespace'] = Catalog.metafields['Features']['NAME_SPACE']
                    features_data['key'] = Catalog.metafields['Features']['META_KEY']
                    features_data['type'] = Catalog.metafields['Features']['TYPE']

                result.append(features_data)
            elif self.meta_features['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_features['id'])
                self.meta_features['id'] = None

            # Colors
            if self.meta_colors['value']:
                colors_data = {'value': json.dumps(self.meta_colors['value'])}
                if self.meta_colors['id']:
                    colors_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_colors["id"]}'
                else:
                    colors_data['namespace'] = Catalog.metafields['Color']['NAME_SPACE']
                    colors_data['key'] = Catalog.metafields['Color']['META_KEY']
                    colors_data['type'] = Catalog.metafields['Color']['TYPE']
                result.append(colors_data)
            elif self.meta_colors['id']:
                try:
                    Shopify.Metafield.delete(metafield_id=self.meta_colors['id'])
                except Exception as e:
                    Product.error_handler.add_error_v(f'Error deleting metafield: {e}')
                    query = f"""
                        UPDATE {Table.Middleware.products}
                        SET {creds.Shopify.Metafield.Product.color} = NULL
                        WHERE PRODUCT_ID = {self.product_id}
                        """
                    db.query(query)

                self.meta_colors['id'] = None

            # Bloom Color
            if self.meta_bloom_color['value']:
                bloom_color_data = {'value': json.dumps(self.meta_bloom_color['value'])}
                if self.meta_bloom_color['id']:
                    bloom_color_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_bloom_color["id"]}'
                else:
                    bloom_color_data['namespace'] = Catalog.metafields['Bloom Color']['NAME_SPACE']
                    bloom_color_data['key'] = Catalog.metafields['Bloom Color']['META_KEY']
                    bloom_color_data['type'] = Catalog.metafields['Bloom Color']['TYPE']

                result.append(bloom_color_data)

            elif self.meta_bloom_color['id']:
                try:
                    Shopify.Metafield.delete(metafield_id=self.meta_bloom_color['id'])
                except Exception as e:
                    Product.error_handler.add_error_v(f'Error deleting metafield: {e}')
                    query = f"""
                        UPDATE {Table.Middleware.products}
                        SET {creds.Shopify.Metafield.Product.bloom_color} = NULL
                        WHERE PRODUCT_ID = {self.product_id}
                        """
                    db.query(query)
                self.meta_bloom_color['id'] = None

            # Preorder Status - All products are either preorder or not
            preorder_data = {'value': 'true' if self.is_preorder else 'false'}
            if self.meta_is_preorder['id']:
                preorder_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_is_preorder["id"]}'
            else:
                preorder_data['namespace'] = Catalog.metafields['Preorder Item']['NAME_SPACE']
                preorder_data['key'] = Catalog.metafields['Preorder Item']['META_KEY']
                preorder_data['type'] = Catalog.metafields['Preorder Item']['TYPE']

            result.append(preorder_data)

            # Preorder Release Date
            preorder_release_date = {'value': self.preorder_release_date}
            if self.meta_preorder_release_date['value']:
                if self.meta_preorder_release_date['id']:
                    preorder_release_date['id'] = (
                        f'{Shopify.Metafield.prefix}{self.meta_preorder_release_date["id"]}'
                    )
                else:
                    preorder_release_date['namespace'] = Catalog.metafields['Preorder Release Date']['NAME_SPACE']
                    preorder_release_date['key'] = Catalog.metafields['Preorder Release Date']['META_KEY']
                    preorder_release_date['type'] = Catalog.metafields['Preorder Release Date']['TYPE']

                result.append(preorder_release_date)

            elif self.meta_preorder_release_date['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_preorder_release_date['id'])
                self.meta_preorder_release_date['id'] = None

            # Preorder Message
            preorder_message = {'value': self.preorder_message}
            if self.meta_preorder_message['value']:
                if self.meta_preorder_message['id']:
                    preorder_message['id'] = f'{Shopify.Metafield.prefix}{self.meta_preorder_message["id"]}'
                else:
                    preorder_message['namespace'] = Catalog.metafields['Preorder Message']['NAME_SPACE']
                    preorder_message['key'] = Catalog.metafields['Preorder Message']['META_KEY']
                    preorder_message['type'] = Catalog.metafields['Preorder Message']['TYPE']

                result.append(preorder_message)
            elif self.meta_preorder_message['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_preorder_message['id'])
                self.meta_preorder_message['id'] = None

            # On Sale Status - All products are either on sale or not
            on_sale_data = {'value': self.meta_is_on_sale['value']}
            if self.meta_is_on_sale['id']:
                on_sale_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_is_on_sale["id"]}'
            else:
                on_sale_data['namespace'] = Catalog.metafields['On Sale']['NAME_SPACE']
                on_sale_data['key'] = Catalog.metafields['On Sale']['META_KEY']
                on_sale_data['type'] = Catalog.metafields['On Sale']['TYPE']
            result.append(on_sale_data)

            # On Sale Description
            if self.meta_sale_description['value']:
                on_sale_description_data = {'value': self.meta_sale_description['value']}
                if self.meta_sale_description['id']:
                    on_sale_description_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_sale_description["id"]}'
                else:
                    on_sale_description_data['namespace'] = Catalog.metafields['On Sale Description']['NAME_SPACE']
                    on_sale_description_data['key'] = Catalog.metafields['On Sale Description']['META_KEY']
                    on_sale_description_data['type'] = Catalog.metafields['On Sale Description']['TYPE']

                result.append(on_sale_description_data)

            elif self.meta_sale_description['id']:
                Shopify.Metafield.delete(metafield_id=self.meta_sale_description['id'])
                self.meta_sale_description['id'] = None

            # Featured Product Status - All products are either featured or not
            featured_data = {'value': 'true' if self.featured else 'false'}
            if self.meta_is_featured['id']:
                featured_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_is_featured["id"]}'
            else:
                featured_data['namespace'] = Catalog.metafields['Featured']['NAME_SPACE']
                featured_data['key'] = Catalog.metafields['Featured']['META_KEY']
                featured_data['type'] = Catalog.metafields['Featured']['TYPE']

            result.append(featured_data)

            # In Store Only Status - All products are either in store only or not
            in_store_only_data = {'value': 'true' if self.in_store_only else 'false'}
            if self.meta_in_store_only['id']:
                in_store_only_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_in_store_only["id"]}'
            else:
                in_store_only_data['namespace'] = Catalog.metafields['In Store Only']['NAME_SPACE']
                in_store_only_data['key'] = Catalog.metafields['In Store Only']['META_KEY']
                in_store_only_data['type'] = Catalog.metafields['In Store Only']['TYPE']

            result.append(in_store_only_data)

            # New Product Status - All products are either new or not
            new_data = {'value': 'true' if self.is_new else 'false'}
            if self.meta_is_new['id']:
                new_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_is_new["id"]}'
            else:
                new_data['namespace'] = Catalog.metafields['New']['NAME_SPACE']
                new_data['key'] = Catalog.metafields['New']['META_KEY']
                new_data['type'] = Catalog.metafields['New']['TYPE']

            result.append(new_data)

            # Back In Stock status - All products are either back in stock or not
            back_in_stock_data = {'value': 'true' if self.is_back_in_stock else 'false'}
            if self.meta_is_back_in_stock['id']:
                back_in_stock_data['id'] = f'{Shopify.Metafield.prefix}{self.meta_is_back_in_stock["id"]}'
            else:
                back_in_stock_data['namespace'] = Catalog.metafields['Back In Stock']['NAME_SPACE']
                back_in_stock_data['key'] = Catalog.metafields['Back In Stock']['META_KEY']
                back_in_stock_data['type'] = Catalog.metafields['Back In Stock']['TYPE']

            result.append(back_in_stock_data)

            return result

        def get_media_payload():
            count = 0
            for m in self.media:
                if count == 0:
                    m.is_thumbnail = True
                if not m.db_id:
                    # If new media is found, set flag to True to trigger correct mutation
                    self.has_new_media = True
                    # Set sort order for new media. Leave existing sort order for existing media
                    m.sort_order = count
                count += 1

            if not self.has_new_media or (self.default_image and self.images[0].db_id):
                for m in self.media:
                    m.temp_sort_order = m.sort_order

                return None

            result = []
            images = get_image_payload()
            videos = get_video_payload()

            for image in images:
                result.append(image)

            for video in videos:
                result.append(video)

            # Get Desired Sort Order of Images and Videos
            i = 0  # Desired Sort Order
            j = 0  # Temp Sort Order, used for created anticipated order of media in response

            for m in self.media:
                m.sort_order = i
                i += 1
                if m.db_id:
                    m.temp_sort_order = j
                    j += 1

            for m in self.media:
                if not m.db_id:
                    m.temp_sort_order = j
                    j += 1

            return result

        def get_image_payload():
            result = []
            file_list = []
            stagedUploadsCreateVariables = {'input': []}

            if len(self.images) == 1 and self.images[0].name == 'coming-soon.jpg':
                target_image = self.images[0]
                if not target_image.db_id:
                    uploaded_files = [{'file_path': target_image.file_path, 'url': target_image.image_url}]
            else:
                for image in self.images:
                    image_size = get_filesize(image.file_path)
                    if image_size != image.size:
                        if image.db_id:  # If image is in the database, delete the image from Shopify
                            Shopify.Product.Media.Image.delete(image=image)
                            db.Shopify.Product.Media.Image.delete(image_id=image.shopify_id)
                            image.shopify_id = None
                            image.image_url = None
                            image.db_id = None
                        image.size = get_filesize(image.file_path)
                        file_list.append(image.file_path)
                        stagedUploadsCreateVariables['input'].append(
                            {
                                'filename': image.name,
                                'mimeType': 'image/jpg',
                                'httpMethod': 'POST',
                                'resource': 'IMAGE',
                            }
                        )

                # Upload new images
                uploaded_files = Shopify.Product.Files.create(
                    variables=stagedUploadsCreateVariables, file_list=file_list, eh=Catalog.eh
                )

            for file in uploaded_files:
                for image in self.images:
                    if file['file_path'] == image.file_path:
                        image.image_url = file['url']
                        image_payload = {
                            'originalSource': image.image_url,
                            'alt': image.description,
                            'mediaContentType': image.type,
                        }
                        result.append(image_payload)
            # print(f'Image Payload - {result}')
            return result

        def get_video_payload():
            result = []
            for video in self.videos:
                if video.has_valid_url:  # filter out videos with invalid URLs
                    if not video.db_id:  # only add videos that are not already in the database
                        video_payload = {'originalSource': video.url, 'mediaContentType': video.type}

                        if video.shopify_id:
                            video_payload['id'] = f'{Shopify.Product.Media.Video.prefix}{video.shopify_id}'
                        if video.description:
                            video_payload['alt'] = video.description

                        result.append(video_payload)

            return result

        def get_brand_name(brand):
            """Takes the brand profile code and returns the brand name"""
            query = f"""
                SELECT DESCR
                FROM IM_ITEM_PROF_COD
                WHERE PROF_COD = '{brand}'"""
            response = db.query(query)
            if response:
                return response[0][0]
            else:
                return brand

        product_payload = {
            'input': {
                'title': self.web_title,
                'status': 'ACTIVE' if self.visible else 'DRAFT',
                'seo': {},
                'metafields': get_metafields(),
            }
        }

        media_payload = get_media_payload()
        if media_payload:
            product_payload['media'] = media_payload

        if self.product_id:
            product_payload['input']['id'] = f'gid://shopify/Product/{self.product_id}'

        if self.brand:
            product_payload['input']['vendor'] = get_brand_name(self.brand)

        if self.type:
            product_payload['input']['productType'] = self.type

        if self.tags:
            product_payload['input']['tags'] = self.tags.split(',')

        if self.shopify_collections:
            product_payload['input']['collectionsToJoin'] = [
                f'gid://shopify/Collection/{x}' for x in self.shopify_collections
            ]

            product_payload['input']['collectionsToLeave'] = [
                f'gid://shopify/Collection/{x}' for x in self.get_collections_to_leave()
            ]

        if self.meta_title:
            product_payload['input']['seo']['title'] = self.meta_title

        if self.html_description:
            product_payload['input']['descriptionHtml'] = self.html_description

        if self.meta_description:
            product_payload['input']['seo']['description'] = self.meta_description

        if not self.product_id:  # new product
            # If Add Standalone Variant Option - will be deleted later
            if self.is_bound:
                product_payload['input']['productOptions'] = [
                    {'name': 'Option', 'values': [{'name': '9999 Gallon'}]}
                ]

        return product_payload

    def get_variant_metafields(self, variant):
        result = []
        # Variant Size
        if variant.meta_variant_size['value']:
            variant_size = {'value': variant.meta_variant_size['value']}
            if variant.meta_variant_size['id']:
                variant_size['id'] = f'{Shopify.Metafield.prefix}{variant.meta_variant_size["id"]}'
            else:
                variant_size['namespace'] = Catalog.metafields['Variant Size']['NAME_SPACE']
                variant_size['key'] = Catalog.metafields['Variant Size']['META_KEY']
                variant_size['type'] = Catalog.metafields['Variant Size']['TYPE']

            result.append(variant_size)
        else:
            if variant.meta_variant_size['id']:
                Shopify.Metafield.delete(metafield_id=variant.meta_variant_size['id'])
                variant.meta_variant_size['id'] = None

        return result

    def get_bulk_variant_payload(self, variants=None):
        payload = {'media': [], 'strategy': 'REMOVE_STANDALONE_VARIANT', 'variants': []}
        # If product_id exists, this is an update
        if self.product_id:
            payload['productId'] = f'gid://shopify/Product/{self.product_id}'

        if variants:
            variant_list = variants
        else:
            variant_list = self.variants

        for child in variant_list:
            variant_payload = {
                'inventoryItem': {
                    'cost': child.cost,
                    'tracked': True
                    if self.track_inventory and not (self.is_preorder or self.is_workshop)
                    else False,
                    'requiresShipping': True,
                    'sku': child.sku,
                },
                'inventoryPolicy': 'DENY',  # Prevents overselling,
                'price': child.price_1,  # May be overwritten by price_2 (below)
                'compareAtPrice': 0,
                'optionValues': {'optionName': 'Option'},
                'taxable': child.taxable,
                'metafields': self.get_variant_metafields(child),
            }

            if child.variant_id:
                variant_payload['id'] = f'gid://shopify/ProductVariant/{child.variant_id}'
            else:
                if not self.is_preorder:
                    variant_payload['inventoryQuantities'] = {
                        'availableQuantity': child.buffered_quantity,
                        'locationId': creds.Shopify.Location.n2,
                    }

            if child.price_2:
                # If price_2 is set, use the lower of the two prices for sale price
                variant_payload['price'] = min(child.price_1, child.price_2)
                variant_payload['compareAtPrice'] = max(child.price_1, child.price_2)

            if len(self.variants) > 1:
                variant_payload['optionValues']['name'] = child.variant_name

            else:
                # if child.custom_size:
                #     variant_payload['optionValues']['name'] = child.variant_name
                # else:
                variant_payload['optionValues']['name'] = 'Default Title'

            if child.weight:
                variant_payload['inventoryItem']['measurement'] = {
                    'weight': {'unit': 'POUNDS', 'value': child.weight}
                }

            # Add Variant Image
            for image in child.images:
                if image.is_variant_image:
                    image_size = get_filesize(image.file_path)
                    if image_size != image.size:
                        file_list = [image.file_path]
                        stagedUploadsCreateVariables = {
                            'input': [
                                {
                                    'filename': image.name,
                                    'mimeType': 'image/jpg',
                                    'httpMethod': 'POST',
                                    'resource': 'IMAGE',
                                }
                            ]
                        }

                        uploaded_file = Shopify.Product.Files.create(
                            variables=stagedUploadsCreateVariables, file_list=file_list, eh=Catalog.eh
                        )
                        variant_payload['mediaSrc'] = uploaded_file[0]['url']
                        child.has_variant_image = True

            payload['variants'].append(variant_payload)

        return payload

    def get_single_variant_payload(self):
        payload = {
            'input': {
                'id': f'gid://shopify/ProductVariant/{self.variants[0].variant_id}',
                'inventoryItem': {
                    'cost': self.cost,
                    'measurement': {},
                    'requiresShipping': True,
                    'sku': self.sku,
                    'tracked': True
                    if self.track_inventory and not (self.is_preorder or self.is_workshop)
                    else False,
                },
                'inventoryPolicy': 'DENY',
                'price': self.default_price,
                'compareAtPrice': 0,
                'taxable': self.taxable,
            }
        }

        if not self.is_preorder:
            payload['input']['inventoryQuantities'] = {}
            payload['input']['inventoryQuantities']['availableQuantity'] = self.buffered_quantity
            payload['input']['inventoryQuantities']['locationId'] = creds.Shopify.Location.n2

        if self.weight:
            payload['input']['inventoryItem']['measurement'] = {'weight': {'unit': 'POUNDS', 'value': self.weight}}
        if self.sale_price:
            payload['input']['price'] = min(self.sale_price, self.default_price)
            payload['input']['compareAtPrice'] = max(self.sale_price, self.default_price)

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
                        'locationId': creds.Shopify.Location.n2,
                        'quantity': child.buffered_quantity,
                    }
                )
        return payload

    def get_variant_image_payload(self):
        # Add Variant Image
        variant_image_payload = []
        for child in self.variants:
            if not child.has_variant_image:
                if self.verbose:
                    Product.logger.info(f'Variant {child.sku} is missing an image. Checking images for variant')
                for image in child.images:
                    if self.verbose:
                        Product.logger.info(f'Checking Image: {image.name}')
                    if image.is_variant_image:
                        if self.verbose:
                            Product.logger.info(f'Image: {image.name} is the variant image')
                            Product.logger.info(f'Image ID: {image.shopify_id}')
                            Product.logger.info(f'Variant ID: {child.variant_id}')
                        variant_image_payload.append(
                            {
                                'id': f'{Shopify.Product.Variant.prefix}{child.variant_id}',
                                'imageId': f'{Shopify.Product.Variant.Image.prefix}{image.shopify_id}',
                            }
                        )

        return variant_image_payload

    def process(self):
        """Process Product Creation/Delete/Update in Shopify and Middleware."""

        def create():
            """Create new product in Shopify and Middleware."""
            # Create Base Product
            response = Shopify.Product.create(self.get_payload())
            self.get_product_meta_ids(response)

            # Assign Default Variant Properties
            self.variants[0].variant_id = response['variant_ids'][0]
            self.variants[0].option_id = self.option_id

            if len(self.variants) > 1:
                self.variants[0].option_value_id = response['option_value_ids'][0]

            self.variants[0].inventory_id = response['inventory_ids'][0]

            if len(self.variants) > 1:
                # Save Default Option Value ID for Deletion
                delete_target = self.variants[0].option_value_id
                # Create Variants in Bulk
                self.variants[0].variant_id = None
                variant_payload = self.get_bulk_variant_payload()
                variant_response = Shopify.Product.Variant.create_bulk(variant_payload)

                for variant in self.variants:
                    variant.option_id = self.option_id
                    variant.option_value_id = variant_response[variant.sku]['option_value_id']
                    variant.inventory_id = variant_response[variant.sku]['inventory_id']
                    variant.variant_id = variant_response[variant.sku]['variant_id']
                    variant.has_variant_image = variant_response[variant.sku]['has_image']

                # Remove Default Variant
                Shopify.Product.Option.update(
                    product_id=self.product_id, option_id=self.option_id, option_values_to_delete=[delete_target]
                )

                Shopify.Product.Option.reorder(self)

                # Wait for images to process
                time.sleep(3)
                Shopify.Product.Variant.Image.create(self.product_id, self.get_variant_image_payload())
                self.get_variant_meta_ids(variant_response)

            else:
                single_payload = self.get_single_variant_payload()
                variant_response = Shopify.Product.Variant.update_single(single_payload)

            # Add Product to Sales Channel - by default, all are turned on.
            Shopify.Product.publish(self.product_id)

        def update():
            """Will update existing product. Will clear out custom field data and reinsert."""

            product_payload = self.get_payload()
            response = Shopify.Product.update(product_payload)
            self.get_product_meta_ids(response)
            Shopify.Product.Media.reorder(self)  # Reorder media if necessary

            if self.is_bound:
                # Update the Variants
                variant_response = Shopify.Product.Variant.update_bulk(self.get_bulk_variant_payload())

                for variant in self.variants:
                    variant.option_id = self.option_id
                    variant.option_value_id = variant_response[variant.sku]['option_value_id']
                    variant.variant_id = variant_response[variant.sku]['variant_id']
                    variant.has_variant_image = variant_response[variant.sku]['has_image']

                Shopify.Product.Option.reorder(self)

                # Wait for images to process
                time.sleep(3)
                Shopify.Product.Variant.Image.create(self.product_id, self.get_variant_image_payload())
                self.get_variant_meta_ids(variant_response)

            else:
                variant_payload = self.get_single_variant_payload()
                variant_response = Shopify.Product.Variant.update_single(variant_payload)

        try:
            if not self.inventory_only:
                if self.product_id:
                    update()
                else:
                    create()
                # Update Middleware (Insert or Update)
                db.Shopify.Product.sync(product=self, eh=Catalog.eh, verbose=self.verbose)

            # Update Inventory
            if not self.is_preorder:
                Shopify.Inventory.update(self.get_inventory_payload())

        except Exception as e:
            msg = self.web_title + ' - ' + (self.binding_id if self.binding_id else self.sku)
            Product.error_handler.add_error_v(
                f'Error processing product {msg}: {e}', origin='Product.process', traceback=tb()
            )
            return False, self.product_data

        else:
            msg = self.web_title + ' - ' + (self.binding_id if self.binding_id else self.sku)
            Product.logger.success(f'Product {msg} processed successfully.')
            return True, self.product_data

    def get_product_meta_ids(self, response):
        """Used to get metafield IDs for products from response."""
        if 'product_id' in response:
            self.product_id = response['product_id']
        if 'option_ids' in response:
            self.option_id = response['option_ids'][0]

        def get_metafield_ids(response):
            for meta_id in response['meta_ids']:
                if meta_id['namespace'] == 'product-specification':
                    if meta_id['key'] == Catalog.metafields['Botanical Name']['META_KEY']:
                        self.meta_botanical_name['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Growing Zone']['META_KEY']:
                        self.meta_climate_zone['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Growing Zone List']['META_KEY']:
                        self.meta_climate_zone_list['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Plant Type']['META_KEY']:
                        self.meta_plant_type['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Mature Height']['META_KEY']:
                        self.meta_height['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Mature Width']['META_KEY']:
                        self.meta_width['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Light Requirements']['META_KEY']:
                        self.meta_light_requirements['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Size']['META_KEY']:
                        self.meta_size['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Bloom Season']['META_KEY']:
                        self.meta_bloom_season['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Features']['META_KEY']:
                        self.meta_features['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Color']['META_KEY']:
                        self.meta_colors['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Bloom Color']['META_KEY']:
                        self.meta_bloom_color['id'] = meta_id['id']

                elif meta_id['namespace'] == 'product-status':
                    if meta_id['key'] == Catalog.metafields['Preorder Item']['META_KEY']:
                        self.meta_is_preorder['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Preorder Release Date']['META_KEY']:
                        self.meta_preorder_release_date['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Preorder Message']['META_KEY']:
                        self.meta_preorder_message['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Featured']['META_KEY']:
                        self.meta_is_featured['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['In Store Only']['META_KEY']:
                        self.meta_in_store_only['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['On Sale']['META_KEY']:
                        self.meta_is_on_sale['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['On Sale Description']['META_KEY']:
                        self.meta_sale_description['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['New']['META_KEY']:
                        self.meta_is_new['id'] = meta_id['id']
                    elif meta_id['key'] == Catalog.metafields['Back In Stock']['META_KEY']:
                        self.meta_is_back_in_stock['id'] = meta_id['id']

        if 'meta_ids' in response:
            get_metafield_ids(response)

        def get_media_ids(response):
            # print(response['media_ids'])
            for x in self.media:
                # print(f'Media: {x}')
                # print(f'Temp sort order: {x.temp_sort_order}')
                x.product_id = self.product_id
                x.shopify_id = response['media_ids'][x.temp_sort_order]
                if x.temp_sort_order > x.sort_order:
                    # Newly added media will be at the end of the media response list.
                    # If that expected position is higher than the required position, add to job queue.
                    self.reorder_media_queue.append(x)

                elif response['media_ids'].index(x.shopify_id) != self.media.index(x):
                    # This block accounts for existing media that has changed position.
                    # Currently, this only applies to videos that are a comma separated list.
                    x.sort_order = self.media.index(x)
                    self.reorder_media_queue.append(x)

        if response['media_ids']:
            get_media_ids(response)

    def get_variant_meta_ids(self, response):
        """Used to get metafield IDs for variants."""
        for item in response:
            for variant in self.variants:
                if variant.sku == item:
                    if 'variant_meta_ids' in response[item]:
                        for variant_meta_id in response[item]['variant_meta_ids']:
                            # Variant Size
                            if (
                                variant_meta_id['key'] == 'variant_size'
                                and variant.meta_variant_size['value'] == variant_meta_id['value']
                            ):
                                variant.meta_variant_size['id'] = variant_meta_id['id']

    def get_shopify_collections(self):
        """Get Shopify Collection IDs from Middleware Category IDs"""
        result = []

        if self.cp_ecommerce_categories:
            for category in self.cp_ecommerce_categories:
                # Get Collection ID from Middleware Category ID
                q = f"""
                        SELECT COLLECTION_ID 
                        FROM {Table.Middleware.collections}
                        WHERE CP_CATEG_ID = '{category}'
                        """
                response = db.query(q)
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
                        parent_response = db.query(q)
                        try:
                            result.append(parent_response[0][0])
                        except:
                            top_level = True
                        else:
                            category = parent_response[0][0]

        return result

    def get_collection_names(self):
        """Get Collection Names from Middleware Category IDs"""
        result = []
        if self.cp_ecommerce_categories:
            for category in self.cp_ecommerce_categories:
                q = f"""
                        SELECT DESCR
                        FROM EC_CATEG
                        WHERE CATEG_ID = '{category}'
                        """
                response = db.query(q)
                try:
                    result.append(response[0][0])
                except:
                    continue
                else:
                    top_level = False
                    while not top_level:
                        q = f"""
                                SELECT DESCR
                                FROM EC_CATEG
                                WHERE CATEG_ID = (SELECT PARENT_ID
                                                    FROM EC_CATEG
                                                    WHERE CATEG_ID = '{category}')
                                """
                        parent_response = db.query(q)
                        try:
                            result.append(parent_response[0][0])
                        except:
                            top_level = True
                        else:
                            category = parent_response[0][0]
        return result

    @staticmethod
    def is_parent(sku):
        """Check if this product is a parent product."""
        query = f"""
            SELECT IS_PARENT
            FROM {Table.Middleware.products}
            WHERE ITEM_NO = '{sku}'
            """
        response = db.query(query)
        if response is not None:
            return response[0][0] == 1

    @staticmethod
    def get_product_id(sku):
        query = f"SELECT PRODUCT_ID FROM {Table.Middleware.products} WHERE ITEM_NO = '{sku}'"
        response = db.query(query)
        return response[0][0] if response is not None else None

    @staticmethod
    def get_binding_id(sku, middleware=False):
        if middleware:
            query = f"""
                SELECT BINDING_ID
                FROM {Table.Middleware.products}
                WHERE ITEM_NO = '{sku}'
                """
        else:
            query = f"""
                SELECT {Table.CP.Item.Column.binding_id}
                FROM {Table.CP.Item.table}
                WHERE ITEM_NO = '{sku}'
                """
        response = db.query(query)
        return response[0][0] if response and response[0][0] is not None else None

    @staticmethod
    def set_parent(binding_id, remove_current=False):
        # Get Family Members.
        family_members = Product.get_family_members(binding_id=binding_id, price=True)
        # Choose the lowest price family member as the parent.
        parent_sku = min(family_members, key=lambda x: x['price_1'])['sku']
        Product.logger.info(f'Family Members: {family_members}, Target new parent item: {parent_sku}')

        if remove_current:
            # Remove Parent Status from all children.
            remove_parent_query = f"""
                        UPDATE {Table.CP.Item.table} 
                        SET {Table.CP.Item.Column.is_parent} = 'N', 
                        LST_MAINT_DT = GETDATE()
                        WHERE {Table.CP.Item.Column.binding_id} = '{binding_id}'
                        """
            remove_parent_response = db.query(remove_parent_query)
            if remove_parent_response['code'] == 200:
                Product.logger.success(f'Parent status removed from all children of binding: {binding_id}.')
            else:
                Product.error_handler.add_error_v(
                    error=f'Error removing parent status from children of binding: {binding_id}. Response: {remove_parent_response}'
                )

        # Set Parent Status for new parent.
        query = f"""
            UPDATE {Table.CP.Item.table}
            SET {Table.CP.Item.Column.is_parent} = 'Y'
            WHERE ITEM_NO = '{parent_sku}'
            """
        set_parent_response = db.query(query)

        if set_parent_response['code'] == 200:
            Product.logger.success(f'Parent status set for {parent_sku}')
        else:
            Product.error_handler.add_error_v(
                error=f'Error setting parent status for {parent_sku}. Response {set_parent_response}'
            )

        return parent_sku

    @staticmethod
    def get_family_members(binding_id, count=False, price=False, counterpoint=False):
        """Get all items associated with a binding_id. If count is True, return the count."""
        # return a count of items in family
        if count:
            query = f"""
                SELECT COUNT(ITEM_NO)
                FROM {Table.Middleware.products}
                WHERE BINDING_ID = '{binding_id}'
                """
            response = db.query(query)
            if response:
                try:
                    return response[0][0]
                except:
                    Product.error_handler.add_error_v(
                        error=f'Error getting family member count for {binding_id}. Response: {response}'
                    )
                    return 0
            else:
                return 0

        else:
            if price:
                # include retail price for each item
                query = f"""
                    SELECT ITEM_NO, PRC_1
                    FROM {Table.CP.Item.table}
                    WHERE {Table.CP.Item.Column.binding_id} = '{binding_id}' AND 
                    {Table.CP.Item.Column.web_enabled} = 'Y'
                    """
                response = db.query(query)
                if response is not None:
                    return [{'sku': x[0], 'price_1': float(x[1])} for x in response]

            elif counterpoint:
                query = f"""
                    SELECT ITEM_NO
                    FROM {Table.CP.Item.table}
                    WHERE {Table.CP.Item.Column.binding_id} = '{binding_id}' AND
                    {Table.CP.Item.Column.web_enabled} = 'Y'
                    """
                response = db.query(query)
                if response is not None:
                    return [x[0] for x in response]

            else:
                query = f"""
                    SELECT ITEM_NO
                    FROM {Table.Middleware.products}
                    WHERE BINDING_ID = '{binding_id}'
                    """
                response = db.query(query)
                if response is not None:
                    return [x[0] for x in response]

    @staticmethod
    def add_variant(product_id: int, variant_sku: str):
        variant = Variant(sku=variant_sku)
        family_size = Product.get_family_members(binding_id=variant.binding_id, count=True)
        parent_sku = db.Shopify.Product.get_parent_item_no(binding_id=variant.binding_id)
        # For now...
        Catalog.logger.info('In Add Variant: Will delete product...')
        Product.delete(sku=parent_sku, update_timestamp=True)
        # Work in Progress
        # if family_size <= 2:
        #     # This is a formerly 'single' product. Delete the product so it can be recreated as a bound product.
        #     Product.delete(sku=parent_sku, update_timestamp=True)
        # else:
        #     data = {'sku': parent_sku, 'binding_id': variant.binding_id}

        #     product = Product(product_data=data, last_sync=variant.last_sync, verbose=variant.verbose)
        #     product.get(last_sync=variant.last_sync)
        #     # This is a bound product. Will run mutation to add variant to existing product.
        #     payload = product.get_bulk_variant_payload(variants=[variant])
        #     response = Shopify.Product.Variant.create_bulk(payload)

    @staticmethod
    def delete(sku, update_timestamp=False, verbose=False):
        """Delete Product from Shopify and Middleware."""
        if creds.Integrator.verbose_logging:
            Product.logger.info(f'Deleting Product {sku}.')

        def delete_product(sku, product_id, update_timestamp=False):
            """Helper function."""
            if product_id:
                Shopify.Product.delete(product_id)

            db.Shopify.Product.delete(product_id)

            if sku and update_timestamp:
                db.CP.Product.update_timestamp(sku, verbose=verbose)

        # Basic Delete Payload
        delete_payload = {'sku': sku}

        # Get Product ID
        product_id = db.Shopify.Product.get_id(item_no=sku)

        # Add Binding ID to Payload if it exists
        binding_id = db.Shopify.Product.get_binding_id(product_id=product_id)
        if binding_id is not None:
            delete_payload['binding_id'] = binding_id

        if binding_id:
            total_variants_in_mw = Product.get_family_members(binding_id=binding_id, count=True)
            # Delete Single Product/Variants
            if total_variants_in_mw <= 2:
                if creds.Integrator.verbose_logging:
                    Product.logger.info('Case 1')
                # Must have at least 3 Products before deletion to remain a bound product. Will delete product.
                delete_product(sku=sku, product_id=product_id, update_timestamp=update_timestamp)

            elif Product.is_parent(sku):
                if creds.Integrator.verbose_logging:
                    Product.logger.info('Case 2 - Parent Product. Will delete product.')
                delete_product(sku=sku, product_id=product_id, update_timestamp=update_timestamp)

            else:
                if creds.Integrator.verbose_logging:
                    Product.logger.info('Case 3 - Child Product. Will delete variant.')

                option_id = db.Shopify.Product.Variant.get_option_id(sku=sku)
                opt_val_id = db.Shopify.Product.Variant.get_option_value_id(sku=sku)
                variant_id = db.Shopify.Product.Variant.get_id(sku=sku)
                if creds.Integrator.verbose_logging:
                    Product.logger.info(f'Option ID: {option_id}')
                    Product.logger.info(f'Option Value ID: {opt_val_id}')
                    Product.logger.info(f'Variant ID: {variant_id}')

                Shopify.Product.Option.update(
                    product_id=product_id, option_id=option_id, option_values_to_delete=[opt_val_id]
                )

                # Delete Media Associated with Variant
                Shopify.Product.Variant.Image.delete_all(sku=sku, product_id=product_id)

                # Delete Variant from Middleware
                db.Shopify.Product.Variant.delete(variant_id)

        else:
            Product.logger.info('No Binding ID - Delete Single Product')
            delete_product(sku=sku, product_id=product_id, update_timestamp=update_timestamp)


class Variant:
    """Variant objects represent a individual SKU in NCR Counterpoint."""

    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, sku, last_sync=datetime(1970, 1, 1), verbose=False, inventory_only=False):
        self.verbose: bool = verbose
        self.sku: str = sku
        self.last_sync: datetime = last_sync
        self.inventory_only: bool = inventory_only

        # Product ID Info
        product_data: dict = self.get_variant_details()

        # Product Information
        self.mw_db_id = product_data['mw_db_id']
        self.binding_id = product_data['binding_id']
        self.type = product_data['type']
        self.mw_binding_id = product_data['mw_binding_id']
        self.is_parent = True if product_data['is_parent'] == 'Y' else False
        self.product_id: int = product_data['product_id'] if product_data['product_id'] else None
        self.variant_id: int = product_data['variant_id'] if product_data['variant_id'] else None
        self.inventory_id: int = product_data['inventory_id'] if product_data['inventory_id'] else None
        self.option_id = None
        self.option_value_id = None
        # E-Commerce Categories
        self.cp_ecommerce_categories = product_data['cp_ecommerce_categories']

        # Status
        self.visible: bool = product_data['web_visible']
        self.track_inventory: bool = product_data['track_inventory']
        self.featured: bool = product_data['is_featured']
        self.is_preorder: bool = product_data['is_preorder']
        self.is_new: bool = product_data['is_new']
        self.is_back_in_stock: bool = product_data['is_back_in_stock']
        self.preorder_release_date = product_data['preorder_release_date']
        self.preorder_message = product_data['preorder_message']
        self.is_on_sale: bool = product_data['custom_is_on_sale']
        self.sale_description = product_data['custom_sale_description']

        # Product Details
        self.web_title: str = product_data['web_title']
        self.long_descr: str = product_data['long_descr']
        self.variant_name: str = product_data['variant_name']
        self.status: str = product_data['status']
        self.brand = product_data['brand']
        self.html_description = product_data['html_description']
        self.tags = product_data['tags']
        self.meta_title = product_data['meta_title']
        self.meta_description = product_data['meta_description']

        # Pricing
        self.wholesale_price = float(product_data['reg_price'])
        self.price_1 = float(product_data['price_1'])
        self.price_2 = float(product_data['price_2']) if product_data['price_2'] else None
        self.cost = float(product_data['cost'])
        self.taxable = True if product_data['taxable'] == 'Y' else False

        # Inventory Levels
        self.quantity_available: int = product_data['quantity_available']
        self.buffer: int = product_data['buffer']
        self.buffered_quantity: int = self.quantity_available - self.buffer
        if self.buffered_quantity < 0:
            self.buffered_quantity = 0
        self.weight = float(product_data['weight']) if product_data['weight'] else None
        self.in_store_only: bool = product_data['in_store_only']
        self.sort_order: int = product_data['sort_order']

        # Alt Text
        self.alt_text_1: str = product_data['alt_text_1']
        self.alt_text_2: str = product_data['alt_text_2']
        self.alt_text_3: str = product_data['alt_text_3']
        self.alt_text_4: str = product_data['alt_text_4']

        # Custom Fields
        # if self.binding_id and self.is_parent or not self.binding_id:
        self.meta_plant_type = {
            'id': product_data['custom_plant_type_id'],
            'value': product_data['custom_plant_type'],
        }

        self.meta_botanical_name = {
            'id': product_data['custom_botanical_name_id'],
            'value': product_data['custom_botanical_name'],
        }

        # Climate Zone
        climate_zone_min = product_data['custom_climate_zone_min']
        if climate_zone_min:
            # regex for two characters number and a letter
            regex_res = re.match(r'\d{1,2}([a-zA-Z])?', climate_zone_min)
            if regex_res:
                climate_zone_min = regex_res.group() or None

        climate_zone_max = product_data['custom_climate_zone_max']
        if climate_zone_max:
            # regex for two characters number and a letter
            regex_res = re.match(r'\d{1,2}([a-zA-Z])?', climate_zone_max)
            if regex_res:
                climate_zone_max = regex_res.group() or None

        # '3B - 11a'
        if climate_zone_min and climate_zone_max:
            clim_value = f'{climate_zone_min} - {climate_zone_max}'
        elif climate_zone_min:
            clim_value = f'{climate_zone_min}'
        elif climate_zone_max:
            clim_value = f'{climate_zone_max}'
        else:
            clim_value = None
        self.meta_climate_zone = {'id': product_data['custom_climate_zone_id'], 'value': clim_value}

        # Climate Zone List
        # [3, 4, 5, 6, 7, 8, 9, 10, 11]
        self.meta_climate_zone_list = {
            'id': product_data['custom_climate_zone_list_id'],
            'value': self.get_size_range(climate_zone_min, climate_zone_max, ''),
        }

        # Height
        height_min = product_data['custom_height_min']
        height_max = product_data['custom_height_max']
        height_unit = product_data['custom_height_unit']
        height_list = self.get_size_range(height_min, height_max, height_unit)

        if height_list and height_unit:
            self.meta_height = {
                'id': product_data['custom_height_id'],
                'value': [f'{x} {height_unit.lower()}' for x in height_list],
            }
        else:
            self.meta_height = {'id': product_data['custom_height_id'], 'value': None}

        # Width
        width_min = product_data['custom_width_min']
        width_max = product_data['custom_width_max']
        width_unit = product_data['custom_width_unit']
        width_list = self.get_size_range(width_min, width_max, width_unit)
        if width_list and width_unit:
            self.meta_width = {
                'id': product_data['custom_width_id'],
                'value': [f'{x} {width_unit.lower()}' for x in width_list],
            }
        else:
            self.meta_width = {'id': product_data['custom_width_id'], 'value': None}

        # Light Requirements
        self.meta_light_requirements = {'id': product_data['custom_light_req_id'], 'value': []}
        if product_data['custom_full_sun']:
            self.meta_light_requirements['value'].append('Full Sun')
        if product_data['custom_part_sun']:
            self.meta_light_requirements['value'].append('Part Sun')
        if product_data['custom_part_shade']:
            self.meta_light_requirements['value'].append('Part Shade')
        if product_data['custom_full_shade']:
            self.meta_light_requirements['value'].append('Full Shade')

        # Features
        self.meta_features = {'id': product_data['custom_features_id'], 'value': []}
        if product_data['custom_low_maintenance']:
            self.meta_features['value'].append('Low Maintenance')
        if product_data['custom_evergreen']:
            self.meta_features['value'].append('Evergreen')
        if product_data['custom_deciduous']:
            self.meta_features['value'].append('Deciduous')
        if product_data['custom_privacy']:
            self.meta_features['value'].append('Privacy Option')
        if product_data['custom_specimen']:
            self.meta_features['value'].append('Specimen')
        if product_data['custom_shade']:
            self.meta_features['value'].append('Shade Option')
        if product_data['custom_drought_tolerance']:
            self.meta_features['value'].append('Drought Tolerant')
        if product_data['custom_heat_tolerance']:
            self.meta_features['value'].append('Heat Tolerant')
        if product_data['custom_cold_tolerance']:
            self.meta_features['value'].append('Cold Tolerant')
        if product_data['custom_fast_growth']:
            self.meta_features['value'].append('Fast Growing')
        if product_data['custom_attracts_pollinators']:
            self.meta_features['value'].append('Attracts Pollinators')
        if product_data['custom_attracts_wildlife']:
            self.meta_features['value'].append('Attracts Wildlife')
        if product_data['custom_native']:
            self.meta_features['value'].append('Native')
        if product_data['custom_fragrant']:
            self.meta_features['value'].append('Fragrant')
        if product_data['custom_deer_resistant']:
            self.meta_features['value'].append('Deer Resistant')
        if product_data['custom_easy_to_grow']:
            self.meta_features['value'].append('Easy to Grow')
        if product_data['custom_low_light']:
            self.meta_features['value'].append('Low Light')
        if product_data['custom_tropical']:
            self.meta_features['value'].append('Tropical')
        if product_data['custom_vining']:
            self.meta_features['value'].append('Vining')
        if product_data['custom_air_purifying']:
            self.meta_features['value'].append('Air Purifying')
        if product_data['custom_pet_friendly']:
            self.meta_features['value'].append('Pet Friendly')
        if product_data['custom_slow_growth']:
            self.meta_features['value'].append('Slow Growing')
        if product_data['custom_edible']:
            self.meta_features['value'].append('Edible')

        # Bloom Season
        self.meta_bloom_season = {'id': product_data['custom_bloom_season_id'], 'value': []}
        if product_data['custom_bloom_spring']:
            self.meta_bloom_season['value'].append('Spring')
        if product_data['custom_bloom_summer']:
            self.meta_bloom_season['value'].append('Summer')
        if product_data['custom_bloom_fall']:
            self.meta_bloom_season['value'].append('Fall')
        if product_data['custom_bloom_winter']:
            self.meta_bloom_season['value'].append('Winter')

        # If self.meta_bloom_season is not empty, add 'Flowering' as a feature
        if self.meta_bloom_season['value']:
            self.meta_features['value'].append('Flowering')

        # Colors / Bloom Colors
        # If this is a blooming item, color will be translated to Bloom Color
        color_list = []
        if product_data['custom_color_pink']:
            color_list.append('Pink')
        if product_data['custom_color_red']:
            color_list.append('Red')
        if product_data['custom_color_orange']:
            color_list.append('Orange')
        if product_data['custom_color_yellow']:
            color_list.append('Yellow')
        if product_data['custom_color_green']:
            color_list.append('Green')
        if product_data['custom_color_blue']:
            color_list.append('Blue')
        if product_data['custom_color_purple']:
            color_list.append('Purple')
        if product_data['custom_color_white']:
            color_list.append('White')
        if product_data['custom_color_custom']:
            for color in product_data['custom_color_custom']:
                color_list.append(color)

        if color_list:
            if 'Flowering' in self.meta_features['value']:
                self.meta_bloom_color = {'id': product_data['custom_bloom_color_id'], 'value': color_list}
                self.meta_colors = {'id': product_data['custom_color_id'], 'value': None}
            else:
                self.meta_colors = {'id': product_data['custom_color_id'], 'value': color_list}
                self.meta_bloom_color = {'id': product_data['custom_bloom_color_id'], 'value': None}
        else:
            self.meta_colors = {'id': product_data['custom_color_id'], 'value': None}
            self.meta_bloom_color = {'id': product_data['custom_bloom_color_id'], 'value': None}

        # Get Size
        custom_size = product_data['custom_size']
        custom_size_unit = product_data['custom_size_unit']
        if custom_size and custom_size_unit:
            if float(custom_size).is_integer():
                custom_size = int(custom_size)
            custom_size = f'{custom_size} {custom_size_unit.lower()}'
        else:
            custom_size = None

        # meta_size will be used on single products only.
        self.meta_size = {'id': product_data['custom_size_id'], 'value': f'{custom_size}' if custom_size else None}

        # meta_variant_size will be used on bound products only.
        self.meta_variant_size = {
            'id': product_data['custom__variant_size_id'],
            'value': f'{custom_size}' if custom_size else None,
        }

        self.meta_is_preorder = {
            'id': product_data['custom_is_preorder_id'],
            'value': 'true' if self.is_preorder else 'false',
        }

        self.meta_preorder_message = {
            'id': product_data['custom_preorder_message_id'],
            'value': self.preorder_message,
        }

        self.meta_preorder_release_date = {
            'id': product_data['custom_preorder_date_id'],
            'value': self.preorder_release_date,
        }

        self.meta_is_featured = {
            'id': product_data['custom_is_featured_id'],
            'value': 'true' if self.featured else 'false',
        }
        self.meta_in_store_only = {
            'id': product_data['custom_in_store_only_id'],
            'value': 'true' if self.in_store_only else 'false',
        }

        self.meta_is_on_sale = {
            'id': product_data['custom_is_on_sale_id'],
            'value': 'true' if self.is_on_sale else 'false',
        }

        self.meta_sale_description = {
            'id': product_data['custom_sale_description_id'],
            'value': self.sale_description,
        }

        self.meta_is_new = {'id': product_data['custom_is_new_id'], 'value': 'true' if self.is_new else 'false'}

        self.meta_is_back_in_stock = {
            'id': product_data['custom_is_back_in_stock_id'],
            'value': 'true' if self.is_back_in_stock else 'false',
        }

        # Product Images
        self.images: list[Image] = []
        self.has_variant_image = False

        # Dates
        self.lst_maint_dt = product_data['lst_maint_dt']

        # Initialize Images
        if not self.inventory_only:
            images = Catalog.get_local_product_images(self.sku)
            total_images = len(images)
            if total_images > 0:
                for image in images:
                    img = Image(image_name=image, product_id=self.product_id)
                    if img.validate():
                        self.images.append(img)

        # Initialize Variant Image URL
        if len(self.images) > 0:
            self.variant_image_url = self.images[0].image_url
        else:
            self.variant_image_url = ''

        self.videos = [
            Video(url=video, sku=self.sku, binding_id=self.binding_id) for video in product_data['videos']
        ]

    def __str__(self):
        result = ''
        for k, v in self.__dict__.items():
            result += f'{k}: {v}\n'
        return result

    def get_variant_details(self):
        """Get a list of all products that have been updated since the last run date.
        Will check IM_ITEM. IM_PRC, IM_INV, EC_ITEM_DESCR, EC_CATEG_ITEM, and Image tables
        have an after update Trigger implemented for updating IM_ITEM.LST_MAINT_DT."""

        query = f""" 
                
                SELECT MW.ID as 'mw_db_id(0)', 
                ITEM.USR_PROF_ALPHA_16 as 'Binding ID(1)', 
                ISNULL(ITEM.{Table.CP.Item.Column.is_parent}, 'N') as 'Is Parent(2)', 
                MW.PRODUCT_ID as 'Product ID (3)', 
                MW.VARIANT_ID as 'Variant ID(4)', 
                ITEM.USR_PROF_ALPHA_17 as 'VARIANT NAME(5)', 
                MW.INVENTORY_ID as 'INVENTORY_ID(6)', 


                                
                ITEM.{Table.CP.Item.Column.web_visible} as 'Web Visible(7)', 
                ITEM.{Table.CP.Item.Column.featured} as 'IS_FEATURED(8)', 
                ITEM.{Table.CP.Item.Column.in_store_only} as 'IN_STORE_ONLY(9)',
                ITEM.{Table.CP.Item.Column.is_preorder_item} as 'is_preorder(10)', 
                ITEM.{Table.CP.Item.Column.preorder_release_date} as 'preorder_release_date(11)', 
                ITEM.{Table.CP.Item.Column.preorder_message} as 'PREORDER_MESSAGE(12)', 

                                
                ITEM.{Table.CP.Item.Column.sort_order} as 'SORT ORDER(13)', 
                                
                ITEM.{Table.CP.Item.Column.web_title} as 'WEB_TITLE(14)', 
                ITEM.ADDL_DESCR_2 as 'META_TITLE(15)', 
                USR_PROF_ALPHA_21 as 'META_DESCRIPTION(16)', 
                EC_ITEM_DESCR.HTML_DESCR as 'HTML_DESCRIPTION(17)', 

                ITEM.STAT as 'STATUS(18)', 
                                
                ISNULL(ITEM.REG_PRC, 0) as 'REG_PRC(19)', 
                ISNULL(ITEM.PRC_1, 0) as 'PRC_1(20)', 
                PRC.PRC_2 as 'PRC_2(21)', 
                ISNULL(ITEM.LST_COST, 0) as 'COST(22)', 

                CAST(ISNULL(INV.QTY_AVAIL, 0) as INTEGER) as 'QUANTITY_AVAILABLE(23)', 
                CAST(ISNULL(ITEM.PROF_NO_1, 0) as INTEGER) as 'BUFFER(24)', 
                                
                stuff(( select ',' + EC_CATEG_ITEM.CATEG_ID from EC_CATEG_ITEM where 
                EC_CATEG_ITEM.ITEM_NO =ITEM.ITEM_NO for xml path('')),1,1,'') as 'categories(25)',
                ITEM.ITEM_TYP as 'ITEM_TYPE(26)', 
                ITEM.PROF_COD_1 as 'BRAND(27)', 
                ITEM.LONG_DESCR as 'LONG_DESCR(28)', 
                USR_PROF_ALPHA_26 as 'TAGS(29)', 
                                
                {Table.CP.Item.Column.alt_text_1} as 'ALT_TEXT_1(30)', 
                {Table.CP.Item.Column.alt_text_2} as 'ALT_TEXT_2(31)', 
                {Table.CP.Item.Column.alt_text_3} as 'ALT_TEXT_3(32)', 
                {Table.CP.Item.Column.alt_text_4} as 'ALT_TEXT_4(33)', 
                                
                {Table.CP.Item.Column.botanical_name} as 'BOTANICAL_NAM(34)', 
                ZONE_MIN as 'CLIMATE_ZONE MIN(35)',
                ZONE_MAX as 'CLIMATE_ZONE MAX(36)',
                PROF_ALPHA_3 as 'PLANT_TYPE(37)', 
                PROF_ALPHA_4 as 'TYPE(38)', 

                HEIGHT_MIN as 'HEIGHT_MIN(39)',
                HEIGHT_MAX as 'HEIGHT_MAX(40)',
                PROF_COD_3 as 'HEIGHT_UNIT(41)',
                
                WIDTH_MIN as 'WIDTH_MIN(42)',
                WIDTH_MAX as 'WIDTH_MAX(43)',
                PROF_COD_4 as 'WIDTH_UNIT(44)', 

                FULL_SUN as '45',
                PART_SUN as '46',
                PART_SHADE as '47',
                FULL_SHADE as '48',

                BLOOM_SPRING as 'BLOOM_SPRING(49)',
                BLOOM_SUMMER as 'BLOOM_SUMMER(50)',
                BLOOM_FALL as 'BLOOM_FALL(51)',
                BLOOM_WINTER as 'BLOOM_WINTER(52)',

                COLOR_PINK as 'COLOR_PINK(53)',
                COLOR_RED as 'COLOR_RED(54)',
                COLOR_ORANGE as 'COLOR_ORANGE(55)',
                COLOR_YELLOW as 'COLOR_YELLOW(56)',
                COLOR_GREEN as 'COLOR_GREEN(57)',
                COLOR_BLUE as 'COLOR_BLUE(58)',
                COLOR_PURPLE as 'COLOR_PURPLE(59)',
                COLOR_WHITE as 'COLOR_WHITE(60)',
                COLOR_CUSTOM as 'COLOR_CUSTOM(61)',

                LOW_MAINT as 'LOW_MAINT(62)',
                EVERGREEN as 'EVERGREEN(63)',
                DECIDUOUS as 'DECIDUOUS(64)',
                SHADE as 'SHADE(65)',
                PRIVACY as 'PRIVACY(66)',
                SPECIMEN as 'SPECIMENT(67)',
                DROUGHT_TOL as 'DROUGHT_TOL(68)',
                HEAT_TOL as 'HEAT_TOL(69)',
                COLD_TOL as 'COLD_TOL(70)',        
                FAST_GROW as 'FAST_GROW(71)',
                ATTRCT_BEES as 'ATTRCT_BEES(72)',
                ATTR_WILDLIFE as 'ATTR_WILDLIFE(73)',
                NATIVE as 'NATIVE(74)',
                FRAGRANT as 'FRAGRANT(75)',
                DEER_RESIST as 'DEER_RESIST(76)',
                EASY_TO_GROW as 'EASY_TO_GROW(77)',
                LOW_LIGHT as 'LOW_LIGHT(78)',
                TROPICAL as 'TROPICAL(79)',
                VINING as 'VINING(80)',
                AIR_PURE as 'AIR_PURE(81)',
                PET_FRIENDLY as 'PET_FRIENDLY(82)',
                SLOW_GROW as 'SLOW_GROW(83)',
                EDIBLE as 'EDIBLE(84)',

                ITEM.LST_MAINT_DT as 'LST_MAINT_DT(85)', 
                WEIGHT as 'WEIGHT(86)',
                IS_TXBL as 'TAXABLE(87)',
                MW.BINDING_ID as 'MW_BINDING_ID(88)',
                USR_SIZE as 'CUSTOM_SIZE(89)', 

                MW.CF_BOTAN_NAM as 'CUSTOM_BOTANICAL_NAME_ID(90)',
                MW.CF_PLANT_TYP as 'CUSTOM_PLANT_TYPE_ID(91)',
                MW.CF_HEIGHT as 'CUSTOM_HEIGHT_ID(92)',
                MW.CF_WIDTH as 'CUSTOM_WIDTH_ID(93)',
                MW.CF_COLOR as 'CUSTOM_COLOR_ID(94)',
                MW.CF_SIZE as 'CUSTOM_SIZE_ID(95)',
                MW.CF_BLOOM_SEAS as 'CUSTOM_BLOOM_SEASON_ID(96)',
                MW.CF_BLOOM_COLOR as 'CUSTOM_BLOOM_COLOR_ID(97)',
                MW.CF_LIGHT_REQ as 'CUSTOM_LIGHT_REQ_ID(98)',
                MW.CF_FEATURES as 'CUSTOM_FEATURES_ID(99)',
                MW.CF_CLIM_ZON as 'CUSTOM_CLIMATE_ZONE_ID(100)',
                MW.CF_CLIM_ZON_LST as 'CUSTOM_CLIMATE_ZONE_LIST_ID(101)',
                MW.CF_IS_PREORDER as 'CUSTOM_IS_PREORDER_ID(102)',
                MW.CF_PREORDER_DT as 'CUSTOM_PREORDER_DATE_ID(103)',
                MW.CF_PREORDER_MSG as 'CUSTOM_PREORDER_MESSAGE_ID(104)',
                MW.CF_IS_FEATURED as 'CUSTOM_IS_FEATURED_ID(105)',
                MW.CF_IN_STORE_ONLY as 'CUSTOM_IN_STORE_ONLY_ID(106)',
                ITEM.PROF_COD_2 as 'Size Unit(107)', 
                ITEM.USR_VIDEO as 'VIDEOS(108)',
                {Table.CP.Item.Column.is_on_sale} as 'IS_ON_SALE(109)',
                MW.CF_IS_ON_SALE as 'CUSTOM_IS_ON_SALE_ID(110)',
                {Table.CP.Item.Column.sale_description} as 'SALE_DESCRIPTION(111)',
                MW.CF_SALE_DESCR as 'CUSTOM_ON_SALE_DESCRIPTION_ID(112)',
                MW.CF_VAR_SIZE as 'CUSTOM_VARIANT_SIZE_ID(113)',
                ITEM.{Table.CP.Item.Column.is_new} as 'IS_NEW(114)',
                MW.CF_IS_NEW as 'CUSTOM_IS_NEW_ID(115)',
                ITEM.{Table.CP.Item.Column.is_back_in_stock} as 'IS_BACK_IN_STOCK(116)',
                MW.CF_IS_BACK_IN_STOCK as 'CUSTOM_IS_BACK_IN_STOCK_ID(117)',
                ITEM.TRK_INV as 'TRACK_INVENTORY(118)'

                FROM {Table.CP.Item.table} ITEM
                LEFT OUTER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
                LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
                LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
                LEFT OUTER JOIN  {Table.Middleware.products} MW ON ITEM.ITEM_NO=MW.ITEM_NO
                LEFT OUTER JOIN IM_ITEM_PROF_COD COD ON ITEM.PROF_COD_1 = COD.PROF_COD
                WHERE ITEM.ITEM_NO = '{self.sku}'"""

        item = db.query(query)
        if item is not None:
            try:
                details = {
                    # Product ID Info
                    'mw_db_id': item[0][0],
                    'binding_id': item[0][1],
                    'mw_binding_id': item[0][88],
                    'is_bound': True if item[0][1] else False,
                    'is_parent': item[0][2],
                    'product_id': item[0][3],
                    'variant_id': item[0][4],
                    'variant_name': item[0][5],
                    'inventory_id': item[0][6],
                    # Product Status
                    'web_visible': True if item[0][7] == 'Y' else False,
                    'is_featured': True if item[0][8] == 'Y' else False,
                    'in_store_only': True if item[0][9] == 'Y' else False,
                    'is_preorder': True if item[0][10] == 'Y' else False,
                    'preorder_release_date': convert_to_utc(item[0][11]) if item[0][11] else None,
                    'preorder_message': item[0][12],
                    'sort_order': int(item[0][13]) if item[0][13] else 0,
                    # Product Description
                    'web_title': item[0][14],
                    'meta_title': item[0][15],
                    'meta_description': item[0][16],
                    'html_description': item[0][17],
                    'status': item[0][18],
                    # Product Pricing
                    'reg_price': item[0][19],
                    'price_1': item[0][20],
                    'price_2': item[0][21],
                    'cost': item[0][22],
                    # # Inventory Levels
                    'quantity_available': item[0][23],
                    'buffer': item[0][24],
                    # E-Commerce Categories
                    'cp_ecommerce_categories': str(item[0][25]).split(',') if item[0][25] else [],
                    # Additional Details
                    'item_type': item[0][26],  # Inventory/Non-Inventory NOT USED CURRENTLY
                    'brand': item[0][27],
                    'long_descr': item[0][28],
                    'tags': item[0][29],
                    # Alt Text
                    'alt_text_1': item[0][30],
                    'alt_text_2': item[0][31],
                    'alt_text_3': item[0][32],
                    'alt_text_4': item[0][33],
                    # Custom Fields
                    'custom_botanical_name': item[0][34],
                    'custom_climate_zone_min': item[0][35],
                    'custom_climate_zone_max': item[0][36],
                    'custom_plant_type': item[0][37],
                    'type': item[0][38],  # Shows on Shopify Catalog Backend
                    'custom_height_min': item[0][39],
                    'custom_height_max': item[0][40],
                    'custom_height_unit': item[0][41],
                    'custom_width_min': item[0][42],
                    'custom_width_max': item[0][43],
                    'custom_width_unit': item[0][44],
                    'custom_size': item[0][89],
                    # Light Requirements
                    'custom_full_sun': True if item[0][45] == 'Y' else False,
                    'custom_part_sun': True if item[0][46] == 'Y' else False,
                    'custom_part_shade': True if item[0][47] == 'Y' else False,
                    'custom_full_shade': True if item[0][48] == 'Y' else False,
                    'custom_bloom_spring': True if item[0][49] == 'Y' else False,
                    'custom_bloom_summer': True if item[0][50] == 'Y' else False,
                    'custom_bloom_fall': True if item[0][51] == 'Y' else False,
                    'custom_bloom_winter': True if item[0][52] == 'Y' else False,
                    # Colors
                    'custom_color_pink': True if item[0][53] == 'Y' else False,
                    'custom_color_red': True if item[0][54] == 'Y' else False,
                    'custom_color_orange': True if item[0][55] == 'Y' else False,
                    'custom_color_yellow': True if item[0][56] == 'Y' else False,
                    'custom_color_green': True if item[0][57] == 'Y' else False,
                    'custom_color_blue': True if item[0][58] == 'Y' else False,
                    'custom_color_purple': True if item[0][59] == 'Y' else False,
                    'custom_color_white': True if item[0][60] == 'Y' else False,
                    'custom_color_custom': item[0][61].split(',') if item[0][61] else [],
                    # Features
                    'custom_low_maintenance': True if item[0][62] == 'Y' else False,
                    'custom_evergreen': True if item[0][63] == 'Y' else False,
                    'custom_deciduous': True if item[0][64] == 'Y' else False,
                    'custom_shade': True if item[0][65] == 'Y' else False,
                    'custom_privacy': True if item[0][66] == 'Y' else False,
                    'custom_specimen': True if item[0][67] == 'Y' else False,
                    'custom_drought_tolerance': True if item[0][68] == 'Y' else False,
                    'custom_heat_tolerance': True if item[0][69] == 'Y' else False,
                    'custom_cold_tolerance': True if item[0][70] == 'Y' else False,
                    'custom_fast_growth': True if item[0][71] == 'Y' else False,
                    'custom_attracts_pollinators': True if item[0][72] == 'Y' else False,
                    'custom_attracts_wildlife': True if item[0][73] == 'Y' else False,
                    'custom_native': True if item[0][74] == 'Y' else False,
                    'custom_fragrant': True if item[0][75] == 'Y' else False,
                    'custom_deer_resistant': True if item[0][76] == 'Y' else False,
                    'custom_easy_to_grow': True if item[0][77] == 'Y' else False,
                    'custom_low_light': True if item[0][78] == 'Y' else False,
                    'custom_tropical': True if item[0][79] == 'Y' else False,
                    'custom_vining': True if item[0][80] == 'Y' else False,
                    'custom_air_purifying': True if item[0][81] == 'Y' else False,
                    'custom_pet_friendly': True if item[0][82] == 'Y' else False,
                    'custom_slow_growth': True if item[0][83] == 'Y' else False,
                    'custom_edible': True if item[0][84] == 'Y' else False,
                    # Dates
                    'lst_maint_dt': item[0][85],
                    # Shipping
                    'weight': item[0][86],
                    'taxable': True if item[0][87] == 'Y' else False,
                    # Custom Fields
                    'custom_botanical_name_id': item[0][90],
                    'custom_plant_type_id': item[0][91],
                    'custom_height_id': item[0][92],
                    'custom_width_id': item[0][93],
                    'custom_color_id': item[0][94],
                    'custom_size_id': item[0][95],
                    'custom_bloom_season_id': item[0][96],
                    'custom_bloom_color_id': item[0][97],
                    'custom_light_req_id': item[0][98],
                    'custom_features_id': item[0][99],
                    'custom_climate_zone_id': item[0][100],
                    'custom_climate_zone_list_id': item[0][101],
                    'custom_is_preorder_id': item[0][102],
                    'custom_preorder_date_id': item[0][103],
                    'custom_preorder_message_id': item[0][104],
                    'custom_is_featured_id': item[0][105],
                    'custom_in_store_only_id': item[0][106],
                    'custom_size_unit': item[0][107],
                    'videos': item[0][108].replace(' ', '').split(',') if item[0][108] else [],
                    'custom_is_on_sale': True if item[0][109] == 'Y' else False,
                    'custom_is_on_sale_id': item[0][110],
                    'custom_sale_description': item[0][111],
                    'custom_sale_description_id': item[0][112],
                    'custom__variant_size_id': item[0][113],
                    'is_new': True if item[0][114] == 'Y' else False,
                    'custom_is_new_id': item[0][115],
                    'is_back_in_stock': True if item[0][116] == 'Y' else False,
                    'custom_is_back_in_stock_id': item[0][117],
                    'track_inventory': True if item[0][118] == 'Y' else False,
                }
            except KeyError:
                Variant.error_handler.add_error_v(
                    f'Error getting variant details for {self.sku}.\nResponse: {item}',
                    origin='Product Variant Details',
                )
                return None

            return details

    def validate(self):
        # Test for missing variant name
        if not self.variant_name:
            Variant.error_handler.add_error_v(
                f'Product {self.sku} is missing a variant name. Validation failed.', origin='Product Validation'
            )
            raise Exception(f'Product {self.sku} is missing a variant name. Validation failed.')
        # Test for missing price 1
        if self.price_1 == 0:
            Variant.error_handler.add_error_v(
                f'Product {self.sku} is missing a price 1. Validation failed.', origin='Product Validation'
            )
            raise Exception(f'Product {self.sku} is missing a price 1. Validation failed.')

        return True

    def get_size_range(self, min, max, unit='', string=False):
        """Return a list of size values between min and max"""
        if min and max:
            # use regex to extract numbers from beginning of string
            min = int(re.search(r'\d+', str(min)).group())
            max = int(re.search(r'\d+', str(max)).group())
            if string:
                return [f'{x} {unit}' for x in range(min, max + 1)]
            else:
                result = [x for x in range(min, max + 1)]
                return result
        elif min:
            if string:
                return [f'{min} {unit}']
            else:
                return [min]
        elif max:
            if string:
                return [f'{max} {unit}']
            else:
                return [max]


class Image:
    """Product Image class used for validating, resizing, and creating product image payloads."""

    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, image_name: str, product_id: int = None, verbose=False, image_url=None, sku=None):
        self.verbose = verbose
        self.type = 'IMAGE'
        self.db_id = None
        self.name = image_name  # This is the file name
        self.sku = sku
        self.file_path = f'{creds.Company.product_images}/{self.name}'
        self.image_url = image_url
        self.product_id = product_id
        self.variant_id = None
        self.shopify_id = None
        self.is_thumbnail = False
        self.number = 1
        self.sort_order = 0
        self.temp_sort_order = None
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
        if self.name == 'coming-soon.jpg':
            query = f"""SELECT * 
                            FROM {Table.Middleware.images} 
                            WHERE IMAGE_NAME = '{self.name}' AND ITEM_NO = '{self.sku}'
                            """
        else:
            query = f"""SELECT * 
                            FROM {Table.Middleware.images} 
                            WHERE IMAGE_NAME = '{self.name}'
                            """
        response = db.query(query)
        if response is not None:
            if len(response) == 1:
                self.db_id = response[0][0]
                self.name = response[0][1]
                self.sku = response[0][2]
                self.file_path = response[0][3]
                self.product_id = response[0][4]
                self.shopify_id = response[0][5]
                self.number = response[0][7]
                self.sort_order = response[0][8]
                self.is_binding_image = True if response[0][9] == 1 else False
                self.binding_id = response[0][10]
                self.is_variant_image = True if response[0][11] == 1 else False
                self.description = self.get_image_description()  # This will pull fresh data each sync.
                self.size = response[0][13]
                self.last_maintained_dt = response[0][14]

        else:
            if self.name != 'coming-soon.jpg':
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
            if self.name.lower().endswith('jpg'):
                # Resize files larger than 1.8 MB
                if self.size > 1800000:
                    Image.logger.warn(f'Found large file {self.name}. Attempting to resize.')
                    try:
                        im = PILImage.open(self.file_path)
                        im.thumbnail(size, PILImage.LANCZOS)
                        code = im.getexif().get(exif_orientation, 1)
                        if code and code != 1:
                            im = ImageOps.exif_transpose(im)
                        im.save(self.file_path, 'JPEG', quality=q)
                        im.close()
                        self.size = os.path.getsize(self.file_path)
                    except Exception as e:
                        Image.error_handler.add_error_v(f'Error resizing {self.name}: {e}', origin='Image Resize')
                        return False
                    else:
                        Image.logger.success(f'Image {self.name} was resized.')

            # Remove Alpha Layer and Convert PNG to JPG
            if self.name.lower().endswith('png'):
                Image.logger.warn(f'Found PNG file: {self.name}. Attempting to reformat.')
                try:
                    im = PILImage.open(self.file_path)
                    im.thumbnail(size, PILImage.LANCZOS)
                    # Preserve Rotational Data
                    code = im.getexif().get(exif_orientation, 1)
                    if code and code != 1:
                        im = ImageOps.exif_transpose(im)
                    rgb_im = im.convert('RGB')
                    new_image_name = self.name.split('.')[0] + '.jpg'
                    new_file_path = f'{creds.Company.product_images}/{new_image_name}'
                    rgb_im.save(new_file_path, 'JPEG', quality=q)
                    im.close()
                    os.remove(self.file_path)
                    self.file_path = new_file_path
                    self.name = new_image_name
                except Exception as e:
                    Image.error_handler.add_error_v(
                        error=f'Error converting {self.name}: {e}', origin='Reformat PNG'
                    )
                    return False
                else:
                    Image.logger.success(f'{self.name}: Conversion from PNG to JPG successful.')

            # replace .JPEG with .JPG
            if self.name.lower().endswith('jpeg'):
                Image.logger.warn('Found file ending with .JPEG. Attempting to reformat.')
                try:
                    im = PILImage.open(self.file_path)
                    im.thumbnail(size, PILImage.LANCZOS)
                    # Preserve Rotational Data
                    code = im.getexif().get(exif_orientation, 1)
                    if code and code != 1:
                        im = ImageOps.exif_transpose(im)
                    new_image_name = self.name.split('.')[0] + '.jpg'
                    new_file_path = f'{creds.Company.product_images}/{new_image_name}'
                    im.save(new_file_path, 'JPEG', quality=q)
                    im.close()
                    os.remove(self.file_path)
                    self.file_path = new_file_path
                    self.name = new_image_name
                except Exception as e:
                    Image.error_handler.add_error_v(
                        error=f'Error converting {self.name}: {e}', origin='Image Rename JPEG'
                    )
                    return False
                else:
                    Image.logger.success(f'Conversion successful for {self.name}')

            # check for description that is too long
            if len(self.description) >= 500:
                Image.error_handler.add_error_v(f'Description for {self.name} is too long. Validation failed.')
                return False

            # Check for images with words or trailing numbers in the name
            if '^' in self.name and not self.name.split('.')[0].split('^')[1].isdigit():
                Image.error_handler.add_error_v(f'Image {self.name} is not valid.', origin='Image Validation')
                return False

            # Valid Image
            return True

    def set_image_details(self):
        def get_item_no_from_image_name(image_name):
            def get_binding_id(item_no):
                query = f"""
                               SELECT {Table.CP.Item.Column.binding_id} FROM {Table.CP.Item.table}
                               WHERE ITEM_NO = '{item_no}'
                               """
                response = db.query(query)
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
            if '^' in self.name and self.name.split('.')[0].split('^')[1].isdigit():
                # secondary images
                for x in range(1, 100):
                    if int(self.name.split('.')[0].split('^')[1]) == x:
                        image_number = x + 1
                        break
            return image_number

        self.sku, self.binding_id = get_item_no_from_image_name(self.name)
        self.number = get_image_number()

        # self.size = os.path.getsize(self.file_path)

        # Image Description Only non-binding images have descriptions at this time. Though,
        # this could be handled with JSON reference in the future for binding images.
        self.description = self.get_image_description()

    def get_image_description(self):
        # currently there are only 4 counterpoint fields for descriptions.
        if self.number < 5:
            query = f"""
                           SELECT {str(f'USR_PROF_ALPHA_{self.number + 21}')} FROM {Table.CP.Item.table}
                           WHERE ITEM_NO = '{self.sku}'
                           """
            response = db.query(query)

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
        if self.name.endswith('jpg'):
            im = PILImage.open(self.file_path)
            im.thumbnail(size, PILImage.LANCZOS)
            code = im.getexif().get(exif_orientation, 1)
            if code and code != 1:
                im = ImageOps.exif_transpose(im)
            im.save(self.file_path, 'JPEG', quality=q)
            Image.logger.log(f'Resized {self.name}')

    @staticmethod
    def delete(image_name=None, image_id=None):
        """Takes in an image name and looks for matching image file in middleware. If found, delete."""
        Image.logger.info(f'Deleting {image_name or image_id} from Shopify and Middleware.')
        if not image_name and not image_id:
            Image.error_handler.add_error_v('No image name or ID provided for deletion.', origin='Image Deletion')
            return

        product_id = None
        variant_id = None
        image_id = image_id
        is_variant = False

        if not image_id:
            image_query = f"""
                    SELECT IMG.PRODUCT_ID, PROD.VARIANT_ID, IMAGE_ID, IS_VARIANT_IMAGE 
                    FROM {Table.Middleware.images} IMG
                    FULL OUTER JOIN {Table.Middleware.products} PROD on IMG.ITEM_NO = PROD.ITEM_NO
                    WHERE IMAGE_NAME = '{image_name}'
                    """
        else:
            image_query = f"""
                    SELECT IMG.PRODUCT_ID, PROD.VARIANT_ID, IMAGE_ID, IS_VARIANT_IMAGE 
                    FROM {Table.Middleware.images} IMG
                    FULL OUTER JOIN {Table.Middleware.products} PROD on IMG.ITEM_NO = PROD.ITEM_NO
                    WHERE IMAGE_ID = '{image_id}'
                    """

        res = db.query(image_query)
        if res is not None:
            product_id, variant_id, image_id, is_variant = (res[0][0], res[0][1], res[0][2], res[0][3])

        if is_variant:
            Shopify.Product.Variant.Image.delete(product_id=product_id, variant_id=variant_id, shopify_id=image_id)

        Shopify.Product.Media.Image.delete(product_id=product_id, shopify_id=image_id, variant_id=variant_id)
        db.Shopify.Product.Media.Image.delete(
            image_id=image_id, eh=Catalog.eh, verbose=creds.Integrator.verbose_logging
        )


class Video:
    """Product Video class used for validating, and creating product video payloads."""

    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, url, sku, binding_id, verbose=False):
        self.sku = sku
        self.url = url
        self.binding_id = binding_id
        self.verbose = verbose
        self.db_id = None
        self.shopify_id = None
        self.name = None
        self.sort_order = None
        self.temp_sort_order = None
        self.file_path = None
        self.size = None
        self.has_valid_url = True
        self.description = None

        if self.url and not (self.file_path and self.name):
            self.type = 'EXTERNAL_VIDEO'
        else:
            self.type = 'VIDEO'

        self.get_video_details()

    def __str__(self):
        result = ''
        for k, v in self.__dict__.items():
            result += f'{k}: {v}\n'
        return result

    def get_video_details(self):
        query = f"SELECT * FROM {Table.Middleware.videos} WHERE URL = '{self.url}' and ITEM_NO = '{self.sku}'"
        response = db.query(query)
        if response is not None:
            self.db_id = response[0][0]
            self.sku = response[0][1]
            self.url = response[0][2]
            self.name = response[0][3]
            self.file_path = response[0][4]
            self.product_id = response[0][5]
            self.shopify_id = response[0][6]
            self.sort_order = response[0][7]
            self.binding_id = response[0][8]
            self.description = response[0][9]
            self.size = response[0][10]
            self.last_maintained_dt = response[0][11]
        else:
            # All new videos will be checked for validity
            self.has_valid_url = Video.validate(url=self.url)

    @staticmethod
    def validate(url):
        # Check for valid URL
        try:
            response = requests.get(url)
            if response.status_code != 200:
                Video.error_handler.add_error_v(
                    f'Video {url} is not a valid URL. Validation failed.', origin='Video Validation'
                )
                return False

            if 'youtube.com' in url.lower():
                if 'video unavailable' in response.text.lower():
                    Video.error_handler.add_error_v(
                        f'Video {url} is unavailable. Validation failed.', origin='Video Validation'
                    )
                    return False

            elif 'vimeo' in url.lower():
                if 'sorry, this url is unavailable' in response.text.lower():
                    Video.error_handler.add_error_v(
                        f'Video {url} is unavailable. Validation failed.', origin='Video Validation'
                    )
                    return False

        except Exception as e:
            Video.error_handler.add_error_v(f'Error checking URL {url}: {e}', origin='Video Validation')
            return False

        return True

    @staticmethod
    def delete(sku, url):
        product_id = Product.get_product_id(sku)
        video_id = db.Shopify.Product.Media.Video.get(sku=sku, url=url, column='VIDEO_ID')
        if video_id:
            Shopify.Product.Media.delete(product_id=product_id, media_type='video', media_id=video_id)
            db.Shopify.Product.Media.Video.delete(
                video_id=video_id, eh=Catalog.eh, verbose=creds.Integrator.verbose_logging
            )

        else:
            # Shopify Video ID not found. Attempt MW cleanup with SKU and URL
            db.Shopify.Product.Media.Video.delete(
                url=url, sku=sku, eh=Catalog.eh, verbose=creds.Integrator.verbose_logging
            )


# if __name__ == '__main__':
    # from setup.date_presets import Dates

    # cat = Catalog(
    #     last_sync=datetime(2024, 9, 25, 16),
    #     dates=Dates(),
    #     # verbose=True,
    #     # test_mode=True,
    #     # test_queue=[
    #     #     # {'sku': '202944', 'binding_id': 'B9999'},
    #     #     {'sku': '202962'}
    #     # ],
    # )
    # cat.sync()
