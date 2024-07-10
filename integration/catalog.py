import asyncio
import os
import re
import json
from datetime import datetime

import time
import aiohttp

from integration.requests_handler import BCRequests as requests
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

	def __init__(self, last_sync=datetime(1970, 1, 1)):
		self.last_sync = last_sync
		self.db = Database.db
		self.category_tree = self.CategoryTree(last_sync=last_sync)
		self.brands = self.Brands(last_sync=last_sync)
		# Used to process preliminary deletions of products and images
		self.cp_items = []
		self.mw_items = []
		self.sync_queue = []
		self.binding_ids = set()
		if self.sync_queue:
			self.binding_ids = set(x['binding_id'] for x in self.sync_queue if 'binding_id' in x)

	def __str__(self):
		return f'Items to Process: {len(self.sync_queue)}\n'

	def get_products(self):
		# Get data for self.cp_items and self.mw_items
		counterpoint_items = self.db.query_db("SELECT ITEM_NO FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'")
		self.cp_items = [x[0] for x in counterpoint_items] if counterpoint_items else []

		middleware_items = self.db.query_db(f'SELECT ITEM_NO FROM {creds.bc_product_table}')
		self.mw_items = [x[0] for x in middleware_items] if middleware_items else []

		# Create the Sync Queue
		# ---------------------
		# Get all products that have been updated since the last sync

		query = f"""
        SELECT ITEM_NO, ITEM.{creds.cp_field_binding_id} as 'Binding ID'
        FROM IM_ITEM ITEM
        WHERE ITEM.LST_MAINT_DT > '{self.last_sync: %Y-%m-%d %H:%M:%S}' and
        ITEM.IS_ECOMM_ITEM = 'Y'
        ORDER BY [Binding ID] DESC
        """
		response = self.db.query_db(query)
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
                    FROM {creds.bc_product_table}
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
			query = f'SELECT IMAGE_NAME, SIZE FROM {creds.bc_image_table}'
			response = self.db.query_db(query)
			return [[x[0], x[1]] for x in response] if response else []

		def delete_image(image_name) -> bool:
			"""Takes in an image name and looks for matching image file in middleware. If found, deletes from BC and SQL."""
			Catalog.logger.info(f'Deleting {image_name}')
			image_query = f"SELECT PRODUCT_ID, IMAGE_ID, IS_VARIANT_IMAGE FROM {creds.bc_image_table} WHERE IMAGE_NAME = '{image_name}'"
			img_id_res = self.db.query_db(image_query)
			if img_id_res is not None:
				product_id, image_id, is_variant = (img_id_res[0][0], img_id_res[0][1], img_id_res[0][2])

			if is_variant == 1:
				# Get Variant ID
				item_number = image_name.split('.')[0].split('^')[0]
				variant_query = f"SELECT VARIANT_ID FROM {creds.bc_product_table} WHERE ITEM_NO = '{item_number}'"
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
					response = requests.post(url=url, json={'image_url': ''})
					if response.status_code == 200:
						Catalog.logger.success(f'Primary Variant Image {image_name} deleted from BigCommerce.')
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error deleting Primary Variant Image {image_name} from BigCommerce. {response.json()}',
							origin='process_images() -> delete_image()',
						)
						Catalog.logger.warn(f'Error deleting primary variant image: {response.json()}')

			delete_img_url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images/{image_id}'

			img_del_res = requests.delete(url=delete_img_url, timeout=10)

			if img_del_res.status_code == 204:
				Catalog.logger.success(f'Image {image_name} deleted from BigCommerce.')
			else:
				Catalog.error_handler.add_error_v(
					error=f'Error deleting image {image_name} from BigCommerce.', origin='Catalog.delete_image()'
				)
			delete_images_query = f'DELETE FROM {creds.bc_image_table} ' f"WHERE IMAGE_ID = '{image_id}'"
			response = self.db.query_db(delete_images_query, commit=True)
			if response['code'] == 200:
				Catalog.logger.success(f'Image {image_name} deleted from SQL.')
			else:
				Catalog.error_handler.add_error_v(
					error=f'Error deleting image {image_name} from SQL.', origin='Catalog.delete_image()'
				)
			# Delete image from WebDav directory
			web_dav_response = Catalog.delete_image_from_webdav(image_name)
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
				delete_image(x[0])
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
		# Sync Category Tree
		self.category_tree.sync()
		# Sync Product Brands
		self.brands.sync()

		# PRODUCTS
		# --------
		# Get Products
		self.get_products()
		if not initial:
			# Process Product Deletions and Images
			self.process_product_deletes()
			self.process_images()
		# Sync Products
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
					if prod.process():
						Catalog.logger.success(
							f'Product SKU: {prod.sku} Binding ID: {prod.binding_id}, Title: {prod.web_title} processed successfully.'
						)
						success_count += 1
					else:
						Catalog.error_handler.add_error_v(
							error=f'Product SKU: {prod.sku} Binding ID: {prod.binding_id}, Title: {prod.web_title} failed to process.'
						)
						fail_count += 1
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
			product.delete_product(sku=sku)

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
            FROM {creds.bc_product_table}
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
                FROM {creds.bc_product_table}
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
            FROM {creds.bc_product_table}
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
		query = f"SELECT PRODUCT_ID FROM {creds.bc_product_table} WHERE ITEM_NO = '{sku}'"
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
		url = f'{creds.web_dav_product_photos}/{image_name}.jpg'
		response = requests.delete(url, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw))
		return response

	@staticmethod
	def get_deletion_target(primary_source, secondary_source):
		return [element for element in secondary_source if element not in primary_source]

	@staticmethod
	def delete_categories():
		"""Find parent categories and delete them from Big. Delete all categories from the middleware."""

		def batch_delete_categories(category_list):
			while category_list:
				batch = []
				while len(batch) < 250:
					if not category_list:
						break
					batch.append(str(category_list.pop()))

				batch_string = ','.join(batch)

				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees/categories?category_id:in={batch_string}'

				bc_response = requests.delete(url=url)

				if bc_response.status_code == 204:
					Catalog.logger.success(f'Products {batch_string} deleted from BigCommerce.')
					query = f"""
                    DELETE FROM {creds.bc_category_table} WHERE BC_CATEG_ID in ({batch_string})
                    """
					sql_response = Database.db.query_db(query, commit=True)
					if sql_response['code'] == 200:
						Catalog.logger.success(f'Categories {batch_string} deleted from Middleware.')
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error deleting categories {batch_string} from Middleware.'
						)
				else:
					Catalog.error_handler.add_error_v(
						error=f'Error deleting categories {batch_string} from BigCommerce.'
					)
					Catalog.logger.log(f'Url: {url}')
					Catalog.logger.log(bc_response)

			query = f'DELETE FROM {creds.bc_category_table}'
			response = Database.db.query_db(query, commit=True)

			if response['code'] == 200:
				Catalog.logger.success(f'Category: {batch_string} deleted from Middleware.')
			else:
				Catalog.error_handler.add_error_v(error=f'Error deleting category: {batch_string} from Middleware.')

		# Get all parent categories from Middleware
		query = f'SELECT DISTINCT BC_CATEG_ID FROM {creds.bc_category_table} WHERE CP_PARENT_ID = 0'
		response = Database.db.query_db(query)
		parent_category_list = [x[0] for x in response] if response else []

		# Delete all categories from BigCommerce and Middleware
		if parent_category_list:
			batch_delete_categories(parent_category_list)
		else:
			Catalog.logger.warn('No categories found in Middleware. Will check BigCommerce.')

		# As a failsafe for broken mappings, get all categories from BigCommerce and delete them.
		category_id_list = []
		page = 1
		more_pages = True
		while more_pages:
			url = (
				f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/categories?limit=250&page={page}'
			)
			response = requests.get(url)
			for category in response.json()['data']:
				category_id_list.append(category['id'])
			count = response.json()['meta']['pagination']['count']
			if count == 0:
				more_pages = False
			page += 1

		# Delete all remaining categories from BigCommerce
		if category_id_list:
			batch_delete_categories(category_list=category_id_list)
		else:
			Catalog.logger.info('No categories found in BigCommerce.')

	@staticmethod
	def delete_products():
		"""Deletes all products from BigCommerce and Middleware."""

		def batch_delete_products(product_list):
			while product_list:
				batch = []
				while len(batch) < 250:
					if not product_list:
						break
					batch.append(str(product_list.pop()))

				batch_string = ','.join(batch)

				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?id:in={batch_string}'

				bc_response = requests.delete(url=url)

				if bc_response.status_code == 204:
					Catalog.logger.success(f'Products {batch_string} deleted from BigCommerce.')
					query = f"""
                    DELETE FROM {creds.bc_product_table} WHERE PRODUCT_ID in ({batch_string})
                    DELETE FROM {creds.bc_image_table} WHERE PRODUCT_ID in ({batch_string})
                    """
					sql_response = Database.db.query_db(query, commit=True)
					if sql_response['code'] == 200:
						Catalog.logger.success(f'Products {batch_string} deleted from Middleware.')
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error deleting products {batch_string} from Middleware.'
						)
				else:
					Catalog.error_handler.add_error_v(error=f'Error deleting product {batch_string} from BigCommerce.')
					Catalog.logger.log(f'Url: {url}')
					Catalog.logger.log(bc_response)

		# Get all product IDs from Middleware
		query = f'SELECT DISTINCT PRODUCT_ID FROM {creds.bc_product_table}'
		response = Database.db.query_db(query)
		product_id_list = [x[0] for x in response] if response else []

		if product_id_list:
			batch_delete_products(product_id_list)
		else:
			Catalog.logger.warn('No products found in Middleware. Will check BigCommerce.')

		# As a failsafe, get all products from BigCommerce and delete them.
		product_id_list = []
		page = 1
		more_pages = True
		while more_pages:
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?limit=250&page={page}'
			response = requests.get(url)
			for product in response.json()['data']:
				product_id_list.append(product['id'])
			count = response.json()['meta']['pagination']['count']
			if count == 0:
				more_pages = False
			page += 1
		if product_id_list:
			batch_delete_products(product_list=product_id_list)
		else:
			Catalog.logger.info('No products found in BigCommerce.')

	@staticmethod
	def delete_brands():
		"""Deletes all brands from Middleware."""

		def batch_delete_brands(brand_list):
			while brand_list:
				# Create Batch
				batch = []
				while len(batch) < 250:
					if not brand_list:
						break
					batch.append(str(brand_list.pop()))
				batch_string = ','.join(batch)
				# Delete Batch
				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/brands?id:in={batch_string}'
				)
				bc_response = requests.delete(url=url)
				if bc_response.status_code == 204:
					Catalog.logger.success(f'Brand:\n{batch_string}\ndeleted from BigCommerce.')
					query = f'DELETE FROM {creds.bc_brands_table} WHERE BC_BRAND_ID in ({batch_string})'
					response = Database.db.query_db(query, commit=True)
					if response['code'] == 200:
						Catalog.logger.success(f'Brand:\n{batch_string}\ndeleted from Middleware.')
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error deleting brand:\n{batch_string}\nfrom Middleware.'
						)
				else:
					Catalog.error_handler.add_error_v(error=f'Error deleting brand:\n{batch_string}\nfrom BigCommerce.')

		query = f'SELECT DISTINCT BC_BRAND_ID FROM {creds.bc_brands_table}'
		response = Database.db.query_db(query)
		brand_id_list = [x[0] for x in response] if response else []

		# Delete all brands from Middleware and BigCommerce
		if brand_id_list:
			batch_delete_brands(brand_id_list)
		else:
			Catalog.logger.warn('No brands found in Middleware. Will check BigCommerce.')
		# As a failsafe, get all brands from BigCommerce and delete them.
		brand_id_list = []
		page = 1
		more_pages = True
		while more_pages:
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/brands?limit=250&page={page}'
			response = requests.get(url)
			for brand in response.json()['data']:
				brand_id_list.append(brand['id'])
			count = response.json()['meta']['pagination']['count']
			if count == 0:
				more_pages = False
			page += 1
		if brand_id_list:
			batch_delete_brands(brand_id_list)
		else:
			Catalog.logger.info('No brands found in BigCommerce.')

	@staticmethod
	def delete_catalog():
		"""Deletes all products, categories, and brands from BigCommerce and Middleware."""
		Catalog.delete_products()
		Catalog.delete_categories()
		Catalog.delete_brands()

	class CategoryTree:
		def __init__(self, last_sync):
			self.db = Database.db
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
			result = ''
			for root in self.heads:
				result += print_category_tree(root)

			return result

		def get_cp_updates(self):
			query = """
            SELECT cp.CATEG_ID, ISNULL(cp.PARENT_ID, 0), cp.DESCR, cp.DISP_SEQ_NO, cp.HTML_DESCR, 
            cp.LST_MAINT_DT, sn.CP_CATEG_ID, sn.is_visible
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
					is_visible = x[7]

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
                            UPDATE {creds.bc_category_table}
                            SET CP_PARENT_ID = {cp_parent_id}, CATEG_NAME = '{category_name}',
                            SORT_ORDER = {sort_order}, DESCRIPTION = '{description}', 
                            IS_VISIBLE = {1 if is_visible else 0},
                            LST_MAINT_DT = '{lst_maint_dt:%Y-%m-%d %H:%M:%S}'
                            WHERE CP_CATEG_ID = {sn_cp_categ_id}
                            """
							update_res = self.db.query_db(query, commit=True)

							if update_res['code'] == 200:
								Catalog.logger.success(f'Category {category_name} updated in Middleware.')
							else:
								Catalog.error_handler.add_error_v(
									error=f'Error updating category {category_name} in Middleware. \n Query: {query}'
								)

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

			self.heads = [x for x in self.categories if x.cp_parent_id == '0']

		def sync(self):
			def build_tree(category):
				# Get BC Category ID and Parent ID
				# if category.lst_maint_dt > self.last_sync:
				if category.lst_maint_dt > datetime(2021, 1, 1):
					print(f'Updating: {category.category_name}')
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
					print(f'BigCommerce: DELETE {bc_category_id}')
					url = (
						f'https://api.bigcommerce.com/stores/{creds.big_store_hash}'
						f'/v3/catalog/trees/categories?category_id:in={bc_category_id}'
					)
					response = requests.delete(url=url)
					if 207 >= response.status_code >= 200:
						print(response.status_code)  # figure what code they are actually returning
						print(f'Category {bc_category_id} deleted from BigCommerce.')
						# Delete Category from Middleware
						print(f'Middleware: DELETE {cp_categ_id}')
						query = f"""
                                        DELETE FROM SN_CATEG
                                        WHERE CP_CATEG_ID = {cp_categ_id}
                                        """
						try:
							self.db.query_db(query, commit=True)
						except Exception as e:
							print(f'Error deleting category from middleware: {e}')
						else:
							print(f'Category {cp_categ_id} deleted from Middleware.')
					else:
						print(f'Error deleting category {bc_category_id} from BigCommerce.')
						print(response.json())
				else:
					Catalog.logger.warn(f'BC Category {cp_categ_id} not found in Middleware.')
					query = f'DELETE FROM {creds.bc_category_table} WHERE CP_CATEG_ID = {cp_categ_id}'
					del_res = self.db.query_db(query, commit=True)
					if del_res['code'] == 200:
						Catalog.logger.success(f'Category {cp_categ_id} deleted from Middleware.')
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error deleting category {cp_categ_id} from Middleware.'
						)

		class Category:
			def __init__(
				self,
				cp_categ_id,
				cp_parent_id,
				category_name,
				bc_categ_id=None,
				bc_parent_id=None,
				sort_order=0,
				description='',
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
				self.is_visible = self.get_visibility()
				self.lst_maint_dt = lst_maint_dt
				self.children = []

			def __str__(self):
				return (
					f'Category Name: {self.category_name}\n'
					f'---------------------------------------\n'
					f'Counterpoint Category ID: {self.cp_categ_id}\n'
					f'Counterpoint Parent ID: {self.cp_parent_id}\n'
					f'BigCommerce Category ID: {self.bc_categ_id}\n'
					f'BigCommerce Parent ID: {self.bc_parent_id}\n'
					f'Sort Order: {self.sort_order}\n'
					f'Last Maintenance Date: {self.lst_maint_dt}\n\n'
				)

			def add_child(self, child):
				self.children.append(child)

			def get_visibility(self):
				query = f"""
                SELECT IS_VISIBLE
                FROM SN_CATEG
                WHERE CP_CATEG_ID = {self.cp_categ_id}
                """
				response = query_engine.QueryEngine().query_db(query)
				if response:
					return response[0][0]
				else:
					return False

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

			def get_full_custom_url_path(self):
				parent_id = self.cp_parent_id
				url_path = []
				url_path.append(Catalog.parse_custom_url_string(self.category_name))
				while parent_id != 0:
					query = f'SELECT CATEG_NAME, CP_PARENT_ID FROM SN_CATEG WHERE CP_CATEG_ID = {parent_id}'
					response = query_engine.QueryEngine().query_db(query)
					if response:
						url_path.append(Catalog.parse_custom_url_string(response[0][0] or ''))
						parent_id = response[0][1]
					else:
						break
				return f"/{"/".join(url_path[::-1])}/"

			def get_category_payload(self):
				payload = [
					{
						'category_id': self.bc_categ_id,
						'name': self.category_name,
						'parent_id': self.bc_parent_id,
						'tree_id': 1,
						'page_title': self.category_name,
						'description': self.description,
						'is_visible': self.is_visible,
						'sort_order': self.sort_order,
						'url': {'path': self.get_full_custom_url_path(), 'is_customized': False},
					}
				]
				return payload

			def bc_create_category(self):
				url = f' https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees/categories'
				payload = self.get_category_payload()

				response = requests.post(url=url, json=payload)
				if response.status_code in [201, 207]:
					print(f'BigCommerce: POST: {self.category_name}: SUCCESS Code: {response.status_code}')
					category_id = response.json()['data'][0]['category_id']
					return category_id

				else:
					print(f'BigCommerce: POST: {self.category_name}: Failure Code: {response.status_code}')
					print(response.json())

			def bc_update_category(self):
				url = f' https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees/categories'
				payload = self.get_category_payload()

				response = requests.put(url=url, json=payload)
				if response.status_code in [200, 207]:
					print(f'BigCommerce: UPDATE: {self.category_name} Category: SUCCESS Code: {response.status_code}\n')

				else:
					print(
						f'BigCommerce: UPDATE: {self.category_name} '
						f'Category: FAILED Status: {response.status_code}'
						f'Payload: {payload}\n'
						f'Response: {response.text}\n'
					)

			def write_category_to_middleware(self):
				query = f"""
                INSERT INTO SN_CATEG (BC_CATEG_ID, CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, SORT_ORDER, DESCRIPTION, IS_VISIBLE)
                VALUES ({self.bc_categ_id}, {self.cp_categ_id}, {self.cp_parent_id}, 
                '{self.category_name}', {self.sort_order}, '{self.description}', {1 if self.is_visible else 0})
                """
				try:
					query_engine.QueryEngine().query_db(query, commit=True)
				except Exception as e:
					print(f'Middleware: INSERT {self.category_name}: FAILED')
					print(e)
				else:
					print(f'Middleware: INSERT {self.category_name}: SUCCESS')

			def update_category_in_middleware(self):
				query = f"""
                UPDATE SN_CATEG
                SET BC_CATEG_ID = {self.bc_categ_id}, CP_PARENT_ID = {self.cp_parent_id}, 
                CATEG_NAME = '{self.category_name}', 
                SORT_ORDER = {self.sort_order}, DESCRIPTION = '{self.description}',
                IS_VISIBLE = {1 if self.is_visible else 0} 
                WHERE CP_CATEG_ID = {self.cp_categ_id}
                """
				try:
					query_engine.QueryEngine().query_db(query, commit=True)
				except Exception as e:
					print(f'Middleware: UPDATE {self.category_name} Category: FAILED')
					print(e)
				else:
					print(f'Middleware: UPDATE {self.category_name} Category: SUCCESS')

			def delete_category(self):
				# Delete Category from BigCommerce
				print(f'BigCommerce: DELETE {self.category_name}')
				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/'
					f'catalog/trees/categories/{self.bc_categ_id}'
				)
				response = requests.delete(url=url)
				if response.status_code == 204:
					print(f'Category {self.category_name} deleted from BigCommerce.')
				else:
					print(f'Error deleting category {self.category_name} from BigCommerce.')
					print(response.json())

				# Delete Category from Middleware
				print(f'Middleware: DELETE {self.category_name}')
				query = f"""
                DELETE FROM SN_CATEG
                WHERE CP_CATEG_ID = {self.cp_categ_id}
                """
				try:
					query_engine.QueryEngine().query_db(query, commit=True)
				except Exception as e:
					print(f'Error deleting category from middleware: {e}')
				else:
					print(f'Category {self.category_name} deleted from Middleware.')

	class Brands:
		def __init__(self, last_sync=datetime(1970, 1, 1)):
			self.db = Database.db
			self.last_sync = last_sync
			self.cp_brands = set()
			self.mw_brands = set()
			# self.brands will be a set of Brand objects only created if the last_maint_dt is > than last sync
			self.brands = set()
			# get all brands from CP and MW
			self.update_brand_timestamps(last_run=self.last_sync)
			self.get_brands()
			self.process_deletes()

		def __str__(self):
			result = ''
			for brand in self.brands:
				result += f'Brand Name: {brand.name}\n'
				result += '---------------------------------------\n'
				result += f'Counterpoint Brand ID: {brand.cp_brand_id}\n'
				result += f'BigCommerce Brand ID: {brand.bc_brand_id}\n'
				result += f'Last Maintenance Date: {brand.last_maint_dt}\n\n'

			return result

		@staticmethod
		def update_brand_timestamps(last_run):
			"""Takes in a list of SKUs and updates the last maintenance date in input table for each product in the
			list"""

			def get_brand_photos_middleware():
				query = f"""
                SELECT IMAGE_NAME, IMAGE_SIZE FROM {creds.bc_brands_table} WHERE IMAGE_NAME IS NOT NULL
                """
				response = query_engine.QueryEngine().query_db(query)
				return [{'image_name': x[0], 'image_size': x[1]} for x in response] if response else []

			def get_brand_photos_local():
				result_list = []
				# Iterate over all files in the directory
				for filename in os.listdir(creds.brand_photo_path):
					if filename not in ['Thumbs.db', 'desktop.ini', '.DS_Store']:
						file_size = os.path.getsize(f'{creds.brand_photo_path}/{filename}')
						result_list.append({'image_name': filename, 'image_size': file_size})
				return result_list

			def update_timestamp(target):
				query = f"""
                UPDATE IM_ITEM_PROF_COD
                SET LST_MAINT_DT = GETDATE()
                WHERE PROF_COD = '{target}'"""
				response = query_engine.QueryEngine().query_db(query, commit=True)
				if response['code'] == 200:
					Catalog.logger.success(f'Brand {target} updated in Counterpoint.')
				else:
					Catalog.error_handler.add_error_v(error=f'Error updating brand {target} in Counterpoint.')

			local_brand_images = get_brand_photos_local()
			mw_brand_images = get_brand_photos_middleware()

			# Check for new or updated images
			for local_image in local_brand_images:
				if local_image['image_name'] not in [
					x['image_name'] for x in mw_brand_images
				]:  # A local image is found that is not in middleware.
					# Update Timestamp of brand in IM table
					profile_code = local_image['image_name'].split('.')[0]
					Catalog.logger.info(
						f'Local brand image not found in middleware. Updating LST_MAINT_DT for {profile_code} in Counterpoint.'
					)
					update_timestamp(profile_code)

				else:
					# A local image is found that is in middleware.
					if (
						local_image['image_size']
						!= [x['image_size'] for x in mw_brand_images if x['image_name'] == local_image['image_name']][0]
					):  # Image size mismatch
						# Update Timestamp of brand in IM table
						profile_code = local_image['image_name'].split('.')[0]
						Catalog.logger.info(
							f'Brand Image Size Mismatch. Updating LST_MAINT_DT for {profile_code} in Counterpoint.'
						)
						update_timestamp(profile_code)

			# Check for deleted images
			for mw_image in mw_brand_images:
				if mw_image['image_name'] not in [x['image_name'] for x in local_brand_images]:
					# Update Timestamp of brand in IM table
					profile_code = mw_image['image_name'].split('.')[0]
					Catalog.logger.info(
						f'Brand Image Deleted. Updating LST_MAINT_DT for {profile_code} in Counterpoint.'
					)
					update_timestamp(profile_code)

		def sync(self):
			for brand in self.brands:
				brand.process()

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
						Catalog.mw_brands.add((x[0], x[1], x[2]))

			get_cp_brands()
			get_mw_brands()

			for cp_brand in self.cp_brands:
				# Filter out brands that are not new or updated
				if cp_brand[2] > self.last_sync:
					brand = self.Brand(cp_brand[0], cp_brand[1], cp_brand[2], self.last_sync)
					self.brands.add(brand)
					print(f'Brand {brand.name} added to sync queue.')

		def process_deletes(self):
			delete_count = 0
			mw_brand_ids = [x[0] for x in list(Catalog.mw_brands)]
			cp_brand_ids = [x[0] for x in self.cp_brands]
			delete_targets = Catalog.get_deletion_target(secondary_source=mw_brand_ids, primary_source=cp_brand_ids)

			def bc_delete_brand(target):
				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/' f'catalog/brands/{target}'
				response = requests.delete(url=url)
				if response.status_code == 204:
					print(f'BigCommerce: Brand {x} DELETE: SUCCESS. Code: {response.status_code}')
				elif response.status_code == 404:
					print(f'BigCommerce: Brand {x} DELETE: Brand Not Found.')
				else:
					print(f'BigCommerce: Brand {x} DELETE: FAILED! Status Code: {response.status_code}')
					print(response.json())

			def middleware_delete_brand(target):
				query = f"""
                DELETE FROM {creds.bc_brands_table}
                WHERE CP_BRAND_ID = '{target}'
                """
				mw_brand_del_res = self.db.query_db(query, commit=True)
				if mw_brand_del_res['code'] != 200:
					Catalog.error_handler.add_error_v(
						error=f'Brand {target} deleted from middleware.',
						origin='process_deletes() -> delete_from_middleware()',
					)

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
					Catalog.logger.log(f'Deleting Brand {x}.')
					bc_delete_brand(bc_brand_id)
					middleware_delete_brand(x)
					delete_count += 1
				else:
					Catalog.logger.warn(f'Brand {x} not found in middleware.')

		class Brand:
			def __init__(self, cp_brand_id, description, last_maint_dt, last_sync):
				self.db = query_engine.QueryEngine()
				self.db_id = None
				self.cp_brand_id = cp_brand_id
				self.bc_brand_id = None
				self.name = description
				self.page_title = description
				self.meta_keywords = None
				self.meta_description = None
				self.search_keywords = None
				self.image_name = None
				self.image_url = None
				self.image_filepath = f'{creds.brand_photo_path}/{self.cp_brand_id}.jpg'
				self.image_size = None
				self.is_custom_url = True
				self.custom_url = '-'.join(str(re.sub('[^A-Za-z0-9 ]+', '', self.name)).split(' '))
				self.last_maint_dt = last_maint_dt
				self.get_brand_details(last_sync)

			def get_brand_details(self, last_sync):
				query = f"""SELECT *
                FROM {creds.bc_brands_table}
                INNER JOIN IM_ITEM_PROF_COD 
                ON {creds.bc_brands_table}.CP_BRAND_ID = IM_ITEM_PROF_COD.PROF_COD
                WHERE CP_BRAND_ID = '{self.cp_brand_id}'"""

				response = self.db.query_db(query)
				if response is not None:
					self.db_id = response[0][0]
					self.bc_brand_id = response[0][2]
					self.name = response[0][16]  # This pulls fresh from CP each time
					self.page_title = response[0][4]
					self.meta_keywords = response[0][5]
					self.meta_description = response[0][6]
					self.search_keywords = response[0][7]
					self.image_name = response[0][8]
					self.image_url = response[0][9]
					self.image_filepath = f'{creds.brand_photo_path}/{self.cp_brand_id}.jpg'
					self.image_size = response[0][11]
					image_size = Catalog.get_filesize(self.image_filepath)
					self.is_custom_url = True if response[0][12] == 1 else False
					self.custom_url = response[0][13]
					self.last_maint_dt = response[0][14]

					# Image Exists in DB
					if self.image_name is not None:
						if image_size is not None:
							# Image exists locally. Check for size match.
							if self.image_size != image_size:
								self.image_url, self.image_size = self.upload_brand_image()
						else:
							# Image does not exist locally. Reset image values.
							self.image_url = None
							self.image_filepath = None
							self.image_size = None
							self.image_name = None

					# Image Does Not Exist in DB
					else:
						self.image_name, self.image_size = self.get_brand_image()

						if self.image_name is not None:
							self.image_url, self.image_size = self.upload_brand_image()
						else:
							self.image_url = None
							self.image_size = None
				else:
					# Brand does not exist in Middleware
					self.image_name, self.image_size = self.get_brand_image()
					if self.image_name is not None:
						self.image_url, self.image_size = self.upload_brand_image()

			def process(self):
				query = f"""
                SELECT * FROM {creds.bc_brands_table}
                WHERE CP_BRAND_ID = '{self.cp_brand_id}'"""
				response = self.db.query_db(query)
				if response:
					self.update()
				else:
					self.create()

			def create(self):
				def create_bc_brand():
					url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/brands'
					response = requests.post(url=url, json=self.construct_payload())
					if response.status_code in [200, 207]:
						print(f'BigCommerce: Brand {self.name} POST: SUCCESS. Code: {response.status_code}')
						return response.json()['data']['id']
					else:
						print(f'BigCommerce: Brand {self.name} POST: FAILED! Status Code: {response.status_code}')
						print(response.json())  # figure out what they are actually returning

				self.bc_brand_id = create_bc_brand()

				def insert_to_middleware():
					print('insert')
					query = f"""
                    INSERT INTO {creds.bc_brands_table} (CP_BRAND_ID, BC_BRAND_ID, NAME, PAGE_TITLE, META_KEYWORDS, 
                    META_DESCR, SEARCH_KEYWORDS, IMAGE_NAME, IMAGE_URL, IMAGE_FILEPATH, IMAGE_SIZE, IS_CUSTOMIZED, 
                    CUSTOM_URL)
                    VALUES ('{self.cp_brand_id}', 
                    {self.bc_brand_id}, 
                    {f"'{self.name.replace("'", "''")}'"}, 
                    {f"'{self.page_title.replace("'", "''")}'"}, 
                    {f"'{self.meta_keywords.replace("'", "''")}'" if self.meta_keywords else "NULL"}, 
                    {f"'{self.meta_description.replace("'", "''")}'" if self.meta_description else "NULL"}, 
                    {f"'{self.search_keywords.replace("'", "''")}'" if self.search_keywords else "NULL"}, 
                    {f"'{self.image_name}'" if self.image_name else "NULL"},
                    {f"'{self.image_url}'" if self.image_url else "NULL"}, 
                    {f"'{self.image_filepath}'" if self.image_name else "NULL"}, 
                    {self.image_size if self.image_size else "NULL"},
                    {1 if self.is_custom_url else 0},
                    {f"'{self.custom_url}'" if self.custom_url else "NULL"})
                    """
					try:
						response = self.db.query_db(query, commit=True)
					except Exception as e:
						print(f'MIDDLEWARE: Brand {self.name} INSERT: FAILED.\n')
						print(e)
					else:
						if response['code'] == 200:
							print('Brand: MIDDLEWARE UPDATE sent.')
						else:
							print(response)

				insert_to_middleware()

			def update(self):
				def update_bc_brand():
					url = (
						f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3'
						f'/catalog/brands/{self.bc_brand_id}'
					)

					response = requests.put(url=url, json=self.construct_payload())
					if response.status_code in [200, 207]:
						print(f'BigCommerce: Brand {self.name} PUT: SUCCESS. Code: {response.status_code}')
					else:
						print(f'BigCommerce: Brand {self.name} PUT: FAILED! Status Code: {response.status_code}')
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
                    IMAGE_NAME = {f"'{self.image_name}'" if self.image_name else "NULL"}, 
                    IMAGE_URL = {f"'{self.image_url}'" if self.image_url else "NULL"}, 
                    IMAGE_FILEPATH = {f"'{self.image_filepath}'" if self.image_name else "NULL"},
                    IMAGE_SIZE = {self.image_size if self.image_size else "NULL"},
                    IS_CUSTOMIZED = {1 if self.is_custom_url else 0}, 
                    CUSTOM_URL = {f"'{self.custom_url}'" if self.custom_url else "NULL"}, LST_MAINT_DT = GETDATE()
                    WHERE CP_BRAND_ID = '{self.cp_brand_id}'
                    """
					try:
						response = self.db.query_db(query, commit=True)
					except Exception as e:
						print(f'MIDDLEWARE: Brand {self.name} UPDATE: FAILED.\n')
						print(e)
					else:
						if response['code'] == 200:
							print('Brand: MIDDLEWARE UPDATE sent.')
						else:
							print(response)

				update_middleware()

			def get_brand_image(self):
				"""Get image file name from directory"""
				for filename in os.listdir(creds.brand_photo_path):
					if filename.split('.')[0].lower() == self.cp_brand_id.lower():
						file_size = os.path.getsize(f'{creds.brand_photo_path}/{filename}')
						return filename, file_size
				return None, None

			def construct_payload(self):
				payload = {
					'name': self.name,
					'page_title': self.page_title,
					'custom_url': {'url': f'/{self.custom_url}/', 'is_customized': self.is_custom_url},
				}

				payload['image_url'] = self.image_url if self.image_url else ''

				payload['meta_description'] = self.meta_description if self.meta_description else ''

				payload['search_keywords'] = self.search_keywords if self.search_keywords else ''

				return payload

			def get_image_last_modified(self):
				"""Get last modified date of image file"""
				try:
					return datetime.fromtimestamp(os.path.getmtime(self.image_filepath))
				except FileNotFoundError:
					return datetime(1970, 1, 1)

			def upload_brand_image(self) -> str:
				"""Upload brand file to import folder on webDAV server and turn public url"""
				try:
					data = open(self.image_filepath, 'rb')
				except FileNotFoundError:
					return None, None
				file_size = os.path.getsize(self.image_filepath)
				random_int = random.randint(1000, 9999)
				new_name = f"{self.image_name.split(".")[0]}-{random_int}.jpg"
				url = f'{creds.web_dav_product_photos}/{new_name}'
				try:
					img_upload_res = requests.put(
						url, data=data, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw)
					)
				except Exception as e:
					Catalog.error_handler.add_error_v(error=f'Error uploading image: {e}')
				else:
					# return public url of image
					if img_upload_res.status_code == 201:
						return f'{creds.public_web_dav_photos}/{new_name}', file_size
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error uploading brand image: {img_upload_res.status_code} - {img_upload_res.text}'
						)

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
			self.custom_botanical_name = ''
			self.custom_climate_zone = ''
			self.custom_plant_type = ''
			self.custom_type = ''
			self.custom_height = ''
			self.custom_width = ''
			self.custom_sun_exposure = ''
			self.custom_bloom_time = ''
			self.custom_bloom_color = ''
			self.custom_attracts_pollinators = ''
			self.custom_growth_rate = ''
			self.custom_deer_resistant = ''
			self.custom_soil_type = ''
			self.custom_color = ''
			self.custom_size = ''
			self.custom_field_response = []  # Will be list of dictionaries from BC API
			self.custom_field_ids = ''

			self.lst_maint_dt = datetime(1970, 1, 1)

			# E-Commerce Categories
			self.cp_ecommerce_categories = []
			self.bc_ecommerce_categories = []

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
						self.custom_field_ids = bound.custom_field_ids
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

			self.bc_ecommerce_categories = self.get_bc_ecomm_categories()

			# Now all images are in self.images list and are in order by binding img first then variant img

		def validate_inputs(self):
			"""Validate product inputs to check for errors in user input"""
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
						message = (
							f'Product {self.binding_id} is missing a web title and long description. Validation failed.'
						)
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
				if not self.bc_ecommerce_categories:
					message = f'Product {self.binding_id} is missing E-Commerce Categories. Validation failed.'
					Catalog.error_handler.add_error_v(error=message, origin='Input Validation')
					return False

			# Test for missing brand
			if check_for_invalid_brand:
				# Test for missing brand
				if self.brand:
					bc_brands = [x[0] for x in list(Catalog.mw_brands)]
					if self.brand not in bc_brands:
						message = (
							f'Product {self.binding_id} has a brand, but it is not valid. Will delete invalid brand.'
						)
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

		def validate_outputs(self):
			# print(f"Validating Outputs for Product {self.sku}...")
			# if len(self.variants) > 1:
			#     print(f"Product {self.sku} is a bound product. Validating Variants...")
			#     for variant in self.variants:
			#         if variant.mw_binding_id is None:
			#             if not self.bc_post_variant(variant):
			#                 return False
			#             else:
			#                 self.get_product_details(last_sync=self.last_sync)
			#                 return self.process()

			# else:
			#     Catalog.logger.success(
			#         f"Output Validation Passed. Ready to Process Variants: {[x.sku for x in self.variants]}"
			#     )

			#     return True

			return True

		def construct_product_payload(self):
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
					result.append({'name': 'Botanical Name', 'value': self.custom_botanical_name})
				if self.custom_climate_zone:
					result.append({'name': 'Climate Zone', 'value': self.custom_climate_zone})
				if self.custom_plant_type:
					result.append({'name': 'Plant Type', 'value': self.custom_plant_type})
				if self.custom_type:
					result.append({'name': 'Type', 'value': self.custom_type})
				if self.custom_height:
					result.append({'name': 'Height', 'value': self.custom_height})
				if self.custom_width:
					result.append({'name': 'Width', 'value': self.custom_width})
				if self.custom_sun_exposure:
					result.append({'name': 'Sun Exposure', 'value': self.custom_sun_exposure})
				if self.custom_bloom_time:
					result.append({'name': 'Bloom Time', 'value': self.custom_bloom_time})
				if self.custom_bloom_color:
					result.append({'name': 'Bloom Color', 'value': self.custom_bloom_color})
				if self.custom_attracts_pollinators:
					result.append({'name': 'Attracts Pollinators', 'value': self.custom_attracts_pollinators})
				if self.custom_growth_rate:
					result.append({'name': 'Growth Rate', 'value': self.custom_growth_rate})
				if self.custom_deer_resistant:
					result.append({'name': 'Deer Resistant', 'value': self.custom_deer_resistant})
				if self.custom_soil_type:
					result.append({'name': 'Soil Type', 'value': self.custom_soil_type})
				if self.custom_color:
					result.append({'name': 'Color', 'value': self.custom_color})
				if self.custom_size:
					result.append({'name': 'Size', 'value': self.custom_size})
				return result

			def construct_image_payload():
				sort_order = 0
				for x in self.images:
					if sort_order == 0:
						x.is_thumbnail = True
					x.sort_order = sort_order
					sort_order += 1

				result = []
				# Child Images
				for image in self.images:
					image_payload = {
						'product_id': self.product_id,
						'is_thumbnail': image.is_thumbnail,
						'sort_order': image.sort_order,
						'description': f"""{image.description}""",
						'image_url': image.image_url,
					}
					if image.image_id:
						image_payload['id'] = image.image_id

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
				if self.is_bound:
					for child in self.variants:
						variant_payload = {
							'cost_price': child.cost,
							'price': child.price_1,
							'image_url': child.variant_image_url,
							'sale_price': child.price_2,
							'retail_price': child.price_1,
							'weight': child.weight,
							'width': child.width,
							'height': child.height,
							'depth': child.depth,
							'is_free_shipping': child.is_free_shipping,
							'purchasing_disabled': True if child.buffered_quantity < 1 else False,
							'purchasing_disabled_message': child.purchasing_disabled_message,
							'inventory_level': child.buffered_quantity,
							# "inventory_warning_level": 2147483647,
							'sku': child.sku,
							'option_values': [{'option_display_name': 'Option', 'label': child.variant_name}],
							'calculated_price': 0.1,
							'calculated_weight': 0.1,
						}

						if self.product_id:
							variant_payload['product_id'] = self.product_id

						if child.variant_id:
							variant_payload['id'] = child.variant_id

						result.append(variant_payload)

				return result

			payload = {
				'name': self.web_title,
				'type': 'physical',
				'sku': self.binding_id if self.binding_id else self.sku,
				'description': f"""{self.html_description}""",
				'weight': self.weight,
				'width': self.width,
				'depth': self.depth,
				'height': self.height,
				'price': self.default_price,
				'cost_price': self.cost,
				'retail_price': self.default_price,
				'sale_price': self.sale_price,
				'map_price': 0,
				'tax_class_id': 0,
				'brand_id': get_brand_id(),
				'brand_name': self.brand,
				'inventory_level': self.buffered_quantity,
				'inventory_warning_level': 10,
				'inventory_tracking': 'variant' if self.is_bound else 'product',
				'is_free_shipping': False,
				'is_visible': self.visible,
				'is_featured': self.featured,
				'sort_order': self.sort_order,
				'search_keywords': self.search_keywords,
				'gift_wrapping_options_type': 'none' if not self.gift_wrap else 'any',
				'condition': 'New',
				'is_condition_shown': True,
				'page_title': self.meta_title,
				'meta_description': self.meta_description,
				'is_price_hidden': self.is_price_hidden,
				'custom_fields': construct_custom_fields(),
				'videos': construct_video_payload(),
			}

			if self.is_preorder and not self.in_store_only:
				payload['availability'] = 'preorder'
				payload['preorder_release_date'] = self.preorder_release_date
				payload['preorder_message'] = self.preorder_message
				payload['is_preorder_only'] = True

			elif self.in_store_only:
				payload['availability'] = 'disabled'
				payload['purchasing_disabled_message'] = 'This product is only available in-store.'
			else:
				payload['availability'] = 'available'

			# If the product has a product_id, it is an update
			if self.product_id:
				payload['id'] = self.product_id

			# Add child products
			if len(self.variants) >= 1:
				payload['variants'] = construct_variant_payload()

			# Add images
			if self.images:
				payload['images'] = construct_image_payload()

			# Add custom URL

			if self.custom_url:
				payload['custom_url'] = {'url': f'/{self.custom_url}/', 'is_customized': True, 'create_redirect': True}
			else:
				fall_back_url = '-'.join(str(re.sub('[^A-Za-z0-9 ]+', '', self.web_title)).split(' ')).lower()

				payload['custom_url'] = {'url': f'/{fall_back_url}/', 'is_customized': False, 'create_redirect': True}

			# Add E-Commerce Categories
			if self.bc_ecommerce_categories:
				payload['categories'] = self.bc_ecommerce_categories
			return payload

		def process(self, retries=3):
			"""Process Product Creation/Delete/Update in BigCommerce and Middleware."""

			def create(retry=1):
				"""Create new product in BigCommerce and Middleware."""
				if retry >= 0:
					create_response = self.bc_post_product()
					if create_response.status_code in [200, 207]:
						self.get_product_data_from_bc(bc_response=create_response)
						if self.insert_product():
							if self.insert_images():
								return True
							else:
								return False
						else:
							return False

					elif create_response.status_code == 409:
						# Product already exists in BigCommerce. This is a conflict. Delete from BigCommerce and try again.
						if self.is_bound:
							message = (
								f'Deleting Bound Product, self.sku: {self.sku}, self.binding_id: {self.binding_id}'
							)
							Catalog.logger.info(message)
							self.delete_product(sku=self.sku, binding_id=self.binding_id)
						else:
							message = f'Deleting Single Product, self.sku: {self.sku}'
							Catalog.logger.info(message)
							self.delete_product(sku=self.sku)

						return create(retry=retry - 1)

				else:
					Catalog.logger.warn(
						f'Product {self.sku} failed to create in BigCommerce. Response: {create_response}'
					)
					return False

			def update():
				"""Will update existing product. Will clear out custom field data dand reinsert."""
				update_payload = self.construct_product_payload()
				# print("Update Payload")
				# print(json.dumps(update_payload, indent=4))
				self.bc_delete_custom_fields(asynchronous=True)
				update_response = self.bc_update_product(update_payload)

				if update_response.status_code in [200, 201, 207]:
					self.get_product_data_from_bc(bc_response=update_response)
					sync_product_response = self.middleware_sync_product()
					if sync_product_response:
						sync_image_response = self.middleware_sync_images()
						if sync_image_response:
							return True
						else:
							Catalog.logger.warn(
								f'Images for {self.sku} failed to sync with middleware. Response: {sync_product_response}'
							)
							return False
					else:
						Catalog.logger.warn(
							f'Product {self.sku} failed to sync with middleware. Response: {sync_product_response}'
						)
						return False

				elif update_response.status_code == 404:
					# Product was not found. This is a conflict. Delete from BigCommerce and try again.
					if self.is_bound:
						message = f'Deleting Bound Product, self.sku: {self.sku}, self.binding_id: {self.binding_id}'
						Catalog.logger.info(message)
						self.delete_product(sku=self.sku, binding_id=self.binding_id)
					else:
						message = f'Deleting Single Product, self.sku: {self.sku}'
						Catalog.logger.info(message)
						self.delete_product(sku=self.sku)

					Catalog.logger.info('Trying to create product again.')
					# Reset property IDs for a successful POST
					self.product_id = None
					self.custom_field_ids = None
					for image in self.images:
						image.image_id = None
					for variant in self.variants:
						self.reset_variant_properties(variant)

					return create()

				else:
					Catalog.logger.warn(
						f'Product {self.sku} failed to update in BigCommerce. Response: {update_response}'
					)
					return False

			query = f"""SELECT *
                    FROM {creds.bc_product_table}
                    WHERE ITEM_NO = '{self.sku}'"""

			response = self.db.query_db(query)

			if response is None:
				# Product Not Found, Create New Product
				return create()
			else:
				# Product Found, Update Product
				return update()

		def replace_image(self, image) -> bool:
			"""Replace image in BigCommerce and SQL."""
			delete_response = self.delete_image(image.image_name)
			if delete_response.status_code == 204:
				post_response = self.bc_post_image(image)
				if post_response.status_code == 200:
					insert_response = self.insert_image(image)
					print(insert_response)
				else:
					print(f'Replace Image: Error posting {image.image_name} to BigCommerce')
					print(post_response.status_code, post_response.content)
					return False
			else:
				print(f'Error deleting {image.image_name} from BigCommerce')
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
			self.product_id = bc_response.json()['data']['id']
			custom_field_response = bc_response.json()['data']['custom_fields']

			if custom_field_response:
				self.custom_field_ids = ','.join([str(x['id']) for x in custom_field_response])

			for x, variant in enumerate(self.variants):
				variant.binding_id = self.binding_id
				variant.product_id = self.product_id

			variant_data = bc_response.json()['data']['variants']
			# Get Variant ID from SKU
			if variant_data:
				for variant in bc_response.json()['data']['variants']:
					for child in self.variants:
						if child.sku == variant['sku']:
							child.variant_id = variant['id']

			# Option Value IDs
			option_data = bc_response.json()['data']['options']
			if option_data:
				option_id = option_data[0]['id']
				for child in self.variants:
					child.option_id = option_id
					for option in option_data[0]['option_values']:
						if child.variant_name == option['label']:
							child.option_value_id = option['id']

			# Update Image IDs
			image_response = bc_response.json()['data']['images']
			if image_response and self.images:
				for bc_image in image_response:
					for image in self.images:
						if bc_image['sort_order'] == image.sort_order:
							image.image_id = bc_image['id']

			for image in self.images:
				image.product_id = self.product_id

		def bc_post_product(self):
			"""Create product in BigCommerce. For this implementation, this is a single product with no
			variants"""
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?include=custom_fields,options,variants,images'
			payload = self.construct_product_payload()
			bc_response = requests.post(url=url, json=payload)
			if bc_response.status_code in [200, 201, 207]:
				if self.is_bound:
					message = f'POST Code: {bc_response.status_code}. Product: {self.sku} Binding ID: {self.binding_id} Create Success'
				else:
					message = f'POST Code: {bc_response.status_code}. POST Product: {self.sku} Create Success'
				Catalog.logger.success(message)

			else:
				Catalog.error_handler.add_error_v(
					error=f'POST SKU: {self.sku} Binding ID: {self.binding_id} to BigCommerce. Response Code: {bc_response.status_code}\n'
					f'Code {bc_response.status_code}: POST SKU: {self.sku} Binding ID: {self.binding_id}\n'
					f'Payload: {payload}\n'
					f'Response: {json.dumps(bc_response.json(), indent=4)}',
					origin='Catalog --> bc_post_product',
				)
			return bc_response

		def bc_post_image(self, image):
			# Post New Image to Big Commerce

			image_payload = {
				'is_thumbnail': image.is_thumbnail,
				'sort_order': image.sort_order,
				'description': image.description,
				'image_url': image.image_url,
			}

			if self.product_id:
				image_payload['product_id'] = self.product_id

			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}/images'

			bc_response = requests.post(url=url, json=image_payload)

			if bc_response.status_code == 200:
				Catalog.logger.success(f'Image: {image.image_name} Posted to BigCommerce')
			else:
				Catalog.error_handler.add_error_v(
					error=f'POST Image: {image.image_name} to BigCommerce. Response Code: {bc_response.status_code}\n'
					f'Code {bc_response.status_code}: POST Image: {image.image_name}\n'
					f'Payload: {image_payload}\n'
					f'Response: {json.dumps(bc_response.json(), indent=4)}',
					origin='Catalog --> bc_post_image',
				)
			return bc_response

		def bc_update_product(self, payload):
			url = (
				f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/'
				f'catalog/products/{self.product_id}?include=custom_fields,options,variants,images'
			)

			bc_response = requests.put(url=url, json=payload)

			if bc_response.status_code in [200, 201, 207]:
				if self.is_bound:
					message = f'PUT Code: {bc_response.status_code}. Product: {self.sku} Binding ID: {self.binding_id} Update Success'
				else:
					message = f'PUT Code: {bc_response.status_code}. Product: {self.sku} Update Success'
				Catalog.logger.success(message)
			else:
				Catalog.error_handler.add_error_v(
					error=f'POST SKU: {self.sku} Binding ID: {self.binding_id} to BigCommerce. Response Code: {bc_response.status_code}'
				)
				Catalog.logger.warn(
					f'Code {bc_response.status_code}: POST SKU: {self.sku} Binding ID: {self.binding_id}'
				)

				Catalog.logger.info(f'Payload: {payload}')
				Catalog.logger.info(f'Response: {json.dumps(bc_response.json(), indent=4)}')

			return bc_response

		def bc_get_custom_fields(self):
			custom_fields = []

			cf_url = (
				f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/'
				f'catalog/products/{self.product_id}/custom-fields'
			)
			cf_response = requests.get(url=cf_url)
			if cf_response.status_code == 200:
				custom_field_data = cf_response.json()['data']
				for field in custom_field_data:
					custom_fields.append(field['id'])
			else:
				Catalog.error_handler.add_error_v(
					f'Error getting custom fields from BigCommerce. Response: {cf_response.status_code}\n{cf_response.content}'
				)
				return None

			return custom_fields

		def bc_delete_custom_fields(self, asynchronous=False):
			if self.custom_field_ids:
				id_list = self.custom_field_ids.split(',')
			else:
				# If there are no custom fields in the middleware, get them from BigCommerce
				id_list = self.bc_get_custom_fields()
				# If there are no custom fields in BigCommerce, return
				if not id_list:
					return

			async def bc_delete_custom_fields_async():
				async with aiohttp.ClientSession() as session:
					for field_id in id_list:
						async_url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}/custom-fields/{field_id}'
						rate_limited = True
						while rate_limited:
							rate_limited = False
							async with session.delete(url=async_url, headers=creds.bc_api_headers) as resp:
								text_response = await resp.text()
								status_code = resp.status
								if status_code == 204:
									Catalog.logger.success(f'Custom Field {field_id} Deleted from BigCommerce.')
								elif status_code == 429:
									rate_limited = True
									ms_to_wait = int(resp.headers['X-Rate-Limit-Time-Reset-Ms'])
									seconds_to_wait = (ms_to_wait / 1000) + 1
									VirtualRateLimiter.pause_requests(seconds_to_wait)
									time.sleep(seconds_to_wait)
								else:
									Catalog.error_handler.add_error_v(
										f'Error deleting custom field {field_id} from BigCommerce. Response: {status_code}\n{text_response}'
									)

			asyncio.run(bc_delete_custom_fields_async())

			update_cf1_query = f"""
                UPDATE {creds.bc_product_table} 
                SET CUSTOM_FIELDS = NULL, LST_MAINT_DT = GETDATE() 
                WHERE PRODUCT_ID = '{self.product_id}' AND IS_PARENT = 1
                """
			self.db.query_db(update_cf1_query, commit=True)

		def bc_get_option_id(self):
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}/options'
			response = requests.get(url)
			if response.status_code == 200:
				return response.json()['data'][0]['id']

		def bc_delete_product_option_value(self, product_id, option_id, value_id):
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/options/{option_id}/values/{value_id}'
			response = requests.delete(url)
			return response

		def bc_create_product_variant_option_value(self, variant_name, product_id, option_id):
			url = (
				f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
				f'products/{product_id}/options/{option_id}/values'
			)
			print(f'Option Value ID URL: {url}')
			value_payload = {'is_default': False, 'label': variant_name, 'value_data': {}, 'sort_order': 0}
			print(f'OPTION ID Value Payload: {value_payload}')
			response = requests.post(url=url, json=value_payload)
			if response.status_code == 200:
				return response.json()['data']['id']

			else:
				print(f'Error creating option value for {product_id}. Response: {response.status_code}')
				print(response.content)
				return None

		def reset_variant_properties(self, variant):
			variant.db_id = None
			variant.product_id = None
			variant.variant_id = None
			variant.custom_field_ids = None

			for image in variant.images:
				image.id = None
				image.image_id = None

			variant.variant_id = None
			variant.option_value_id = None

		def delete_product(self, sku, binding_id=None):
			"""Delete Product from BigCommerce and Middleware."""

			# CHECK THIS OUT.
			self.db_id = None
			if binding_id:
				product_id = self.get_product_id(binding_id=binding_id)
			else:
				product_id = self.get_product_id(item_no=sku)

			if product_id is None:
				if binding_id:
					delete_url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?sku={binding_id}'
				else:
					delete_url = (
						f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?sku={sku}'
					)
			else:
				delete_url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?id={product_id}'
				)

			del_product_response = requests.delete(url=delete_url)
			if del_product_response.status_code == 204:
				print(f'Product {sku} deleted from BigCommerce.')

				if product_id is None:
					if binding_id:
						delete_query = f"""DELETE FROM {creds.bc_product_table} WHERE BINDING_ID = '{binding_id}' 
                        DELETE FROM {creds.bc_image_table} WHERE BINDING_ID = '{binding_id}'"""
					else:
						delete_query = f"""DELETE FROM {creds.bc_product_table} WHERE ITEM_NO = '{sku}' 
                        DELETE FROM {creds.bc_image_table} WHERE ITEM_NO = '{sku}'"""
				else:
					delete_query = f"""DELETE FROM {creds.bc_product_table} WHERE PRODUCT_ID = '{product_id}' 
                    DELETE FROM {creds.bc_image_table} WHERE PRODUCT_ID = '{product_id}'"""

				sql_del_res = self.db.query_db(delete_query, commit=True)
				if sql_del_res['code'] == 200:
					Catalog.logger.success(f'Product {sku} deleted from BigCommerce and Middleware.')
					return True
				else:
					Catalog.error_handler.add_error_v(
						error=f'Error deleting product {sku} from Middleware. Response: {sql_del_res}'
					)
					return False
			else:
				Catalog.error_handler.add_error_v(
					error=f'Error deleting product {sku} from BigCommerce. Response: {del_product_response} \n URL: {delete_url}'
				)
				return False

		def delete_variant(self, sku, binding_id=None):
			print(f'IN Delete Variant. sku is {sku}, binding_id is {binding_id}')
			"""Delete Variant from BigCommerce and Middleware. This will also delete the option value from BigCommerce."""

			if self.is_last_variant(binding_id=binding_id):
				print('Last Variant in Product. Will delete product.')
				self.delete_product(sku=sku, binding_id=binding_id)
			elif self.is_parent(sku):
				print('Parent Product. Will delete product.')
				self.delete_product(sku=sku, binding_id=binding_id)
			else:
				self.db_id = None
				# Get Variant Details Required for Deletion
				item_query = (
					f'SELECT PRODUCT_ID, VARIANT_ID, OPTION_ID, OPTION_VALUE_ID '
					f"FROM {creds.bc_product_table} WHERE ITEM_NO = '{sku}'"
				)
				response = self.db.query_db(item_query)
				if response is not None:
					product_id = response[0][0]
					variant_id = response[0][1]
					option_id = response[0][2]
					variant_option_value_id = response[0][3]

				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/'
					f'v3/catalog/products/{product_id}/variants/{variant_id}'
				)
				rate_limited = True
				while rate_limited:
					rate_limited = False
					# Step 1: Delete Variant from Big Commerce
					del_variant_response = requests.delete(url=url)
					# print(f"Delete Variant Response: {del_variant_response.status_code}")
					if del_variant_response.status_code == 204:
						# print(f"Variant {sku} deleted from BigCommerce.")

						# Step 2: Delete Option Value from Big Commerce so an orphan option value isn't left behind.
						del_option_res = self.bc_delete_product_option_value(
							product_id, option_id, variant_option_value_id
						)
						# print(f"Delete Option Value Response: {del_option_res.status_code}")
						if del_option_res.status_code == 204:
							# print(
							#     f"Variant Option Value {variant_option_value_id} deleted from BigCommerce."
							# )
							# Step 3: Get Image ID's associated with variant and delete them from parent product.
							image_query = f"""
							SELECT IMAGE_ID
							FROM {creds.bc_image_table}
							WHERE ITEM_NO = '{sku}'
							"""
							image_response = self.db.query_db(image_query)
							if image_response is not None:
								for image_id in image_response:
									image_id = image_id[0]
									self.delete_image(
										# product ID is new here.
										product_id=product_id,
										image_id=image_id,
									)

							# Step 4: Delete Variant from Middleware (Product and Image Tables)
							delete_query = f"DELETE FROM {creds.bc_product_table} WHERE VARIANT_ID = '{variant_id}'"
							response = self.db.query_db(delete_query, commit=True)

							if response['code'] == 200:
								Catalog.logger.success(f'Variant {sku} deleted from Big Commerce and Middleware.')
								return True

						else:
							Catalog.error_handler.add_error_v(
								error=f'Error deleting option value {variant_option_value_id} from BigCommerce. Response: {del_option_res} \n URL: {url}'
							)
							return False

					else:
						print(
							f'Error deleting product {sku} from BigCommerce. Response: {del_variant_response} \n URL: {url}'
						)
						return False

		def delete_image(self, image_id=None, product_id=None, image_name=None):
			"""Delete image from BigCommerce and SQL."""
			if image_id is None:
				image_id = self.get_image_id(image_name)

			if product_id is None:
				# if self.product_id:
				#     product_id = self.product_id
				# else:
				product_id = self.get_product_id(image_id=image_id)

			delete_img_url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images/{image_id}'

			rate_limited = True
			while rate_limited:
				rate_limited = False
				img_del_res = requests.delete(url=delete_img_url)
				if img_del_res.status_code == 204:
					Catalog.logger.success(f'Image {image_id} deleted from BigCommerce.')
					delete_images_query = f'DELETE FROM {creds.bc_image_table} ' f"WHERE IMAGE_ID = '{image_id}'"
					sql_res = self.db.query_db(delete_images_query, commit=True)
					if sql_res['code'] == 200:
						Catalog.logger.success(f'Image {image_id} deleted from SQL.')
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error deleting image {image_id} from SQL. Response: {sql_res}'
						)

				else:
					Catalog.error_handler.add_error_v(
						error=f'Error deleting image {image_id} from BigCommerce.\n'
						f'Response: {img_del_res.status_code}\n'
						f'Content: {img_del_res.content}\n'
						f'URL: {delete_img_url}'
					)
				return img_del_res

		# Middleware Methods
		def middleware_sync_product(self):
			success = True
			for variant in self.variants:
				if variant.db_id is None:
					# If variant.db_id is None, this is a new product to be inserted into SQL
					insert_response = self.insert_variant(variant)
					if insert_response['code'] != 200:
						success = False
				else:
					update_response = self.update_variant(variant)
					if update_response['code'] != 200:
						success = False
			return success

		def middleware_sync_images(self):
			"""Sync images to middleware."""
			success = True
			for image in self.images:
				if image.id is None:
					insert_image_response = self.insert_image(image=image)
					if insert_image_response['code'] != 200:
						success = False
				else:
					update_image_response = self.update_image(image)
					if update_image_response['code'] != 200:
						success = False
			return success

		def insert_product(self):
			"""Insert product into middleware"""
			status = True
			for variant in self.variants:
				response = self.insert_variant(variant)
				if response['code'] != 200:
					status = False
			return status

		def insert_variant(self, variant):
			custom_field_string = self.custom_field_ids
			if not variant.is_parent:
				custom_field_string = None

			if self.bc_ecommerce_categories:
				categories_string = ','.join(str(x) for x in self.bc_ecommerce_categories)
			else:
				categories_string = None

			insert_query = (
				f"INSERT INTO {creds.bc_product_table} (ITEM_NO, BINDING_ID, IS_PARENT, "
				f"PRODUCT_ID, VARIANT_ID, VARIANT_NAME, OPTION_ID, OPTION_VALUE_ID, CATEG_ID, CUSTOM_FIELDS) VALUES ('{variant.sku}', "
				f"{f"'{self.binding_id}'" if self.binding_id else 'NULL'}, "
				f"{1 if variant.is_parent else 0}, {self.product_id if self.product_id else "NULL"}, "
				f"{variant.variant_id if variant.variant_id else "NULL"}, "
				f"{f"'{variant.variant_name}'" if variant.variant_id else "NULL"}, "
				f"{variant.option_id if variant.option_id else "NULL"}, "
				f"{variant.option_value_id if variant.option_value_id else "NULL"}, "
				f"{f"'{categories_string}'" if categories_string else "NULL"}, "
				f"{f"'{custom_field_string}'" if custom_field_string else "NULL"})"
			)

			insert_product_response = self.db.query_db(insert_query, commit=True)
			if insert_product_response['code'] == 200:
				# Catalog.logger.success(
				#     f"SKU: {variant.sku}, Binding ID: {variant.binding_id} - INSERT Variant {self.sku}: Success"
				# )
				pass
			else:
				Catalog.error_handler.add_error_v(
					error=f'INSERT{insert_product_response}',
					origin=f'SKU: {variant.sku} Bind: {variant.binding_id} - insert_variant',
				)
			return insert_product_response

		def update_variant(self, variant):
			custom_field_string = self.custom_field_ids
			if not variant.is_parent:
				custom_field_string = None

			if self.bc_ecommerce_categories:
				categories_string = ','.join(str(x) for x in self.bc_ecommerce_categories)
			else:
				categories_string = None

			update_query = (
				f"UPDATE {creds.bc_product_table} "
				f"SET ITEM_NO = '{variant.sku}', "
				f"BINDING_ID = "
				f"{f"'{self.binding_id}'" if self.binding_id else 'NULL'}, "
				f"IS_PARENT = {1 if variant.is_parent else 0}, "
				f"PRODUCT_ID = {self.product_id if self.product_id else 'NULL'}, "
				f"VARIANT_ID = {variant.variant_id if variant.variant_id else 'NULL'}, "
				f"VARIANT_NAME = {f"'{variant.variant_name}'" if variant.variant_id else "NULL"}, "
				f"OPTION_ID = {variant.option_id if variant.option_id else "NULL"}, "
				f"OPTION_VALUE_ID = {variant.option_value_id if variant.option_value_id else "NULL"}, "
				f"CATEG_ID = {f"'{categories_string}'" if categories_string else "NULL"}, "
				f"CUSTOM_FIELDS = {f"'{custom_field_string}'" if custom_field_string else "NULL"}, "
				f"LST_MAINT_DT = GETDATE() "
				f"WHERE ID = {variant.db_id}"
			)

			update_product_response = self.db.query_db(update_query, commit=True)
			if update_product_response['code'] == 200:
				# Catalog.logger.success(
				#     f"SKU: {variant.sku}, Binding ID: {variant.binding_id} - UPDATE Variant {self.sku}: Success"
				# )
				pass
			else:
				Catalog.error_handler.add_error_v(
					error=f'UPDATE{update_product_response}',
					origin=f'SKU: {variant.sku} Bind: {variant.binding_id} - update_variant',
				)
			return update_product_response

		def reset_brand(self):
			"""Delete brand from item in counterpoint. Used as a corrective measure when an item has a prof_cod_1 that doesn't exist in
			ITEM_PROF_COD"""
			if self.is_bound:
				reset_brand_query = f"""
                UPDATE IM_ITEM
                SET PROF_COD_1 = "SETTLEMYRE", LST_MOD_DT = GETDATE()
                WHERE {creds.cp_field_binding_id} = '{self.binding_id}' AND IS_ADM_TKT = 'Y'
                """
			else:
				reset_brand_query = f"""
                UPDATE IM_ITEM
                SET PROF_COD_1 = NULL, LST_MOD_DT = GETDATE()
                WHERE ITEM_NO = '{self.sku}'
                """
			self.db.query_db(reset_brand_query, commit=True)

		def get_product_id(self, item_no=None, binding_id=None, image_id=None):
			"""Get product ID from SQL using image ID. If not found, return None."""
			if item_no:
				product_query = f"SELECT PRODUCT_ID FROM {creds.bc_product_table} WHERE ITEM_NO = '{item_no}'"
			if image_id:
				product_query = f"SELECT PRODUCT_ID FROM {creds.bc_image_table} WHERE IMAGE_ID = '{image_id}'"
			if binding_id:
				product_query = f"SELECT PRODUCT_ID FROM {creds.bc_product_table} WHERE BINDING_ID = '{binding_id}'"

			if item_no or image_id or binding_id:
				prod_id_res = self.db.query_db(product_query)
				if prod_id_res is not None:
					return prod_id_res[0][0]

		def get_image_id(self, target):
			"""Get image ID from SQL using filename. If not found, return None."""
			image_query = f"SELECT IMAGE_ID FROM {creds.bc_image_table} WHERE IMAGE_NAME = '{target}'"
			img_id_res = self.db.query_db(image_query)
			if img_id_res is not None:
				return img_id_res[0][0]

				print('No images to delete.')

		def insert_images(self):
			"""Insert images into SQL."""
			success = True
			for image in self.images:
				insert_image_response = self.insert_image(image)
				if insert_image_response['code'] != 200:
					success = False
			return success

		def insert_image(self, image) -> bool:
			"""Insert image into SQL."""
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
            {f"'{image.binding_id}'" if image.binding_id else 'NULL'},
            '{image.is_variant_image}',
            {f"'{image.description.replace("'", "''")}'" if image.description != '' else 'NULL'},
            {image.size})"""

			insert_img_response = self.db.query_db(img_insert, commit=True)
			if insert_img_response['code'] == 200:
				# Catalog.logger.success(f"SQL INSERT Image {image.image_name}: Success")
				pass
			else:
				Catalog.error_handler.add_error_v(
					error=f'{insert_img_response}', origin=f'SQL INSERT Image {image.image_name}'
				)
			return insert_img_response

		def update_image(self, image) -> bool:
			"""Update image in SQL."""
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
                BINDING_ID = {f"'{image.binding_id}'" if image.binding_id else 'NULL'},
                IS_VARIANT_IMAGE = '{image.is_variant_image}',
                DESCR = {f"'{image.description.replace("'", "''")}'" if
                            image.description != '' else 'NULL'},
                SIZE = '{image.size}'
                WHERE ID = {image.id}"""

			update_img_response = self.db.query_db(img_update, commit=True)

			if update_img_response['code'] == 200:
				# Catalog.logger.success(f"SQL UPDATE Image {image.image_name}: Success")
				pass
			else:
				Catalog.error_handler.add_error_v(
					error=f'{update_img_response}', origin=f'SQL UPDATE Image {image.image_name}'
				)

			return update_img_response

		def rollback_product(self, retries=3):
			"""Delete product from BigCommerce and Middleware."""
			print('Skipping rollback function')
			# print(
			#     f"\n\n!!! Rolling back product SKU: {self.sku}, Binding: {self.binding_id}!!! \n\n"
			# )

			# if self.is_bound:
			#     sku = self.binding_id

			# else:
			#     sku = self.sku
			#     binding_id = self.binding_id
			#     prod_data = {"sku": sku, "binding_id": binding_id}

			# print(f"Deleting Product from BigCommerce. self.sku is {sku}")

			# if self.delete_product(sku):
			#     print("\n\nReinitializing Product!\n\n")
			#     if self.is_bound:
			#         prod_data = {
			#             "sku": self.parent[0].sku,
			#             "binding_id": self.binding_id,
			#         }
			#     else:
			#         prod_data = {"sku": self.sku, "binding_id": self.binding_id}

			#     self.__init__(
			#         product_data=prod_data,
			#         last_sync=date_presets.business_start_date,
			#     )
			#     self.get_product_details(last_sync=date_presets.business_start_date)
			#     if self.validate_product_inputs:
			#         self.process(retries)

		def is_parent(self, sku):
			"""Check if this product is a parent product."""
			query = f"""
            SELECT IS_PARENT
            FROM {creds.bc_product_table}
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
            FROM {creds.bc_product_table} 
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
				self.option_id = None
				self.option_value_id = None
				self.web_title: str = product_data['web_title']
				self.long_descr = product_data['long_descr']
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
				self.custom_field_ids = product_data['custom_field_ids']

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

                BC_PROD.ID as 'db_id(50)', BC_PROD.CUSTOM_FIELDS as 'custom_field_ids(51)', ITEM.LONG_DESCR as 'long_descr(52)',
                BC_PROD.BINDING_ID as 'mw_binding_id(53)', ITEM.USR_IS_PREORDER as 'is_preorder(54)', 
                ITEM.USR_PREORDER_REL_DT as 'preorder_release_date(55)', ITEM.USR_PROF_ALPHA_18 as 'CUSTOM_URL(56)'

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
						'cp_ecommerce_categories': str(item[0][49]).split(',') if item[0][49] else [],
						'custom_url': item[0][56],
						'custom_field_ids': item[0][51],
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

			def bc_get_option_id(self, product_id=None):
				if product_id is None:
					product_id = self.product_id
				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/options'
				response = requests.get(url)
				if response.status_code == 200:
					return response.json()['data'][0]['id']

			def bc_delete_product_option_value(self, product_id, option_id, value_id):
				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/options/{option_id}/values/{value_id}'
				response = requests.delete(url)
				return response

			def bc_create_product_variant_option_value(self, variant_name, product_id, option_id):
				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
					f'products/{product_id}/options/{option_id}/values'
				)
				print(f'Option Value ID URL: {url}')
				value_payload = {'is_default': False, 'label': variant_name, 'value_data': {}, 'sort_order': 0}

				print(f'OPTION ID Value Payload: {value_payload}')
				response = requests.post(url=url, json=value_payload)
				if response.status_code == 200:
					return response.json()['data']['id']

				else:
					print(f'Error creating option value for {product_id}. Response: {response.status_code}')
					print(response.content)
					return None

			def bc_post_variant(self, product_id=None):
				"""Create variant in BigCommerce."""
				if product_id is None:
					product_id = self.product_id

				self.option_id = self.bc_get_option_id(product_id)

				self.option_value_id = self.bc_create_product_variant_option_value(
					variant_name=self.variant_name, product_id=product_id, option_id=self.option_id
				)

				variant_payload = {
					'product_id': product_id,
					'cost_price': self.cost,
					'price': self.price_1,
					'image_url': self.variant_image_url,
					'sale_price': self.price_2,
					'retail_price': self.price_1,
					'weight': self.weight,
					'width': self.width,
					'height': self.height,
					'depth': self.depth,
					'is_free_shipping': self.is_free_shipping,
					'purchasing_disabled': True if self.buffered_quantity < 1 else False,
					'purchasing_disabled_message': self.purchasing_disabled_message,
					'inventory_level': self.buffered_quantity,
					'sku': self.sku,
					'option_values': [
						{
							'option_id': self.option_id,
							'id': self.option_value_id,
							'option_display_name': 'Option',
							'label': self.variant_name,
						}
					],
				}

				if self.images:
					print('I have images!')
					variant_payload['image_url'] = self.images[0].image_url

				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/variants'
				print(f'self.product_id: {self.product_id}')
				print(f'self.option_id: {self.option_id}')
				print(f'self.option_value_id: {self.option_value_id}')
				print(f'Variant URL: {url}')
				print('Variant Payload: \n')
				print(json.dumps(variant_payload, indent=4))

				bc_response = requests.post(url=url, json=variant_payload)

				if bc_response.status_code in [200, 207]:
					self.product_id = bc_response.json()['data']['product_id']
					self.variant_id = bc_response.json()['data']['id']
					option_data = bc_response.json()['data']['option_values']
					self.option_id = option_data[0]['option_id']
					self.option_value_id = option_data[0]['id']
					print(f'Product ID: {self.product_id}, Variant ID: {self.variant_id}')
					print(f'Option ID: {self.option_id}, Option Value ID: {self.option_value_id}')
					self.insert_variant(self)

					Catalog.logger.success(f'Variant: {self.sku} Posted to BigCommerce. Variant ID: {self.variant_id}')

					return True

				else:
					Catalog.error_handler.add_error_v(
						f'POST VARIANT SKU: {self.sku} Binding ID: {self.binding_id} to BigCommerce. Response Code: {bc_response.status_code}'
					)
					Catalog.logger.warn(
						f'Code {bc_response.status_code}: POST SKU: {self.sku} Binding ID: {self.binding_id}'
					)
					Catalog.logger.info(f'Payload: {variant_payload}')
					Catalog.logger.info(f'Response: {json.dumps(bc_response.json(), indent=4)}')
					return False

			def insert_variant(self, variant):
				custom_field_string = self.custom_field_ids
				if not variant.is_parent:
					custom_field_string = None

				# if self.bc_ecommerce_categories:
				#     categories_string = ",".join(
				#         str(x) for x in self.bc_ecommerce_categories
				#     )
				# else:
				#     categories_string = None

				insert_query = (
					f"INSERT INTO {creds.bc_product_table} (ITEM_NO, BINDING_ID, IS_PARENT, "
					f"PRODUCT_ID, VARIANT_ID, VARIANT_NAME, OPTION_ID, OPTION_VALUE_ID, CUSTOM_FIELDS) VALUES ('{variant.sku}', "
					f"{f"'{self.binding_id}'" if self.binding_id else 'NULL'}, "
					f"{1 if variant.is_parent else 0}, {self.product_id if self.product_id else "NULL"}, "
					f"{variant.variant_id if variant.variant_id else "NULL"}, "
					f"{f"'{variant.variant_name}'" if variant.variant_id else "NULL"}, "
					f"{variant.option_id if variant.option_id else "NULL"}, "
					f"{variant.option_value_id if variant.option_value_id else "NULL"}, "
					f"{f"'{custom_field_string}'" if custom_field_string else "NULL"})"
				)

				insert_product_response = self.db.query_db(insert_query, commit=True)
				if insert_product_response['code'] == 200:
					# Catalog.logger.success(
					#     f"SKU: {variant.sku}, Binding ID: {variant.binding_id} - INSERT Variant {self.sku}: Success"
					# )
					pass
				else:
					Catalog.error_handler.add_error_v(
						error=f'INSERT{insert_product_response}',
						origin=f'SKU: {variant.sku} Bind: {variant.binding_id} - insert_variant',
					)
				return insert_product_response

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
						if x.split('.')[0].split('^')[0].lower() == self.sku.lower():
							product_images.append(x)
				total_images = len(product_images)
				if total_images > 0:
					# print(f"Found {total_images} product images for item: {self.sku}")
					for image in product_images:
						img = Catalog.Product.Image(image_name=image)
						if img.validate():
							self.images.append(img)

			def get_bc_product_images(self):
				"""Get BigCommerce image information for product's images"""
				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/'
					f'catalog/products/{self.product_id}/images'
				)
				response = requests.get(url=url)
				if response is not None:
					for x in response.json():
						# Could use this to back-fill database with image id and sort order info
						pass

			def construct_image_payload(self):
				result = []
				for image in self.images:
					image_payload = {
						'is_thumbnail': image.is_thumbnail,
						'sort_order': image.sort_order,
						'description': image.alt_text_1,
						'image_url': f'{creds.public_web_dav_photos}/{image.image_name}',
						'date_modified': image.modified_date,
					}

					if self.product_id is not None:
						image_payload['product_id'] = self.product_id
					if image.image_id is not None:
						image_payload['id'] = image.image_id

					result.append(image_payload)

				return result

			@staticmethod
			def get_lst_maint_dt(file_path):
				return (
					datetime.fromtimestamp(os.path.getmtime(file_path))
					if os.path.exists(file_path)
					else datetime(1970, 1, 1)
				)

			def hard_reset_variant(self):
				"""Hard reset for single item (variant). Used in pathological case of single item being turned into a merged item."""
				print('Performing hard reset on variant. Sku is ', self.sku)
				url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products?sku={self.sku}'
				delete_response = requests.delete(url=url)

				if delete_response.status_code == 204:
					print(f'Product {self.sku} deleted from BigCommerce.')

					delete_product_query = f"DELETE FROM {creds.bc_product_table} WHERE ITEM_NO = '{self.sku}'"

					print('Deleting Product from SQL')
					self.db.query_db(delete_product_query, commit=True)
					print('Deleting Product Images from SQL')
					delete_images_query = f"DELETE FROM {creds.bc_image_table} WHERE ITEM_NO = '{self.sku}'"
					self.db.query_db(delete_images_query, commit=True)

				self.variant_id = None
				self.product_id = None
				for image in self.images:
					image.image_id = 0

		class Video:
			"""Placeholder for video class"""

			pass

		class Modifier:
			"""Placeholder for modifier class"""

			pass

		class Image:
			def __init__(self, image_name: str):
				self.db = Database.db

				self.id = None
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

				query = f"SELECT * FROM SN_IMAGES WHERE IMAGE_NAME = '{self.image_name}'"
				response = self.db.query_db(query)
				if response is not None:
					self.id = response[0][0]
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
					self.image_url = self.upload_product_image()
					self.set_image_details()

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

			def upload_product_image(self) -> str:
				"""Upload file to import folder on webDAV server and return public url"""
				data = open(self.file_path, 'rb')
				random_int = random.randint(1000, 9999)
				new_name = f"{self.image_name.split(".")[0].replace("^", "-")}-{random_int}.jpg"

				url = f'{creds.web_dav_product_photos}/{new_name}'
				try:
					img_upload_res = requests.put(
						url, data=data, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw)
					)
				except Exception as e:
					Catalog.error_handler.add_error_v(error=f'Error uploading image: {e}')
				else:
					# return public url of image
					if img_upload_res.status_code == 201:
						return f'{creds.public_web_dav_photos}/{new_name}'
					else:
						Catalog.error_handler.add_error_v(
							error=f'Error uploading image: {img_upload_res.status_code} - {img_upload_res.text}'
						)

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

			def bc_get_image(self):
				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
					f'products/{self.product_id}/images/{self.image_id}'
				)
				response = requests.get(url=url)
				return response.content

			def bc_delete_image(self):
				"""Photos can either be variant images or product images. Two flows in this function"""
				if self.is_variant_image:
					url = (
						f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
						f'products/{self.product_id}/variants/{self.variant_id}/images/{self.image_id}'
					)
					response = requests.delete(url=url)

				url = (
					f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
					f'products/{self.product_id}/images/{self.image_id}'
				)
				response = requests.delete(url=url)

				return response.content

			def sql_post_photo(self):
				pass

			def sql_update_photo(self):
				pass

			def sql_delete_photo(self):
				query = f"DELETE FROM {creds.bc_image_table} WHERE IMAGE_NAME = '{self.image_name}'"
				query_engine.QueryEngine().query_db(query, commit=True)
				print(f'Photo {self.image_name} deleted from database.')


if __name__ == '__main__':
	pass
