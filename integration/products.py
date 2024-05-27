import os
import re
from datetime import datetime

import requests
from PIL import Image, ImageOps

from setup import creds
from setup import query_engine
from setup import date_presets
from requests.auth import HTTPDigestAuth


# ------------------------
# CATEGORY TREES
# -----------------------

class CounterpointCategoryTree:
    def __init__(self):
        self.db = query_engine.QueryEngine()
        self.categories = set()
        self.heads = []
        self.create_tree()

    def __str__(self):
        result = ""
        for k in self.heads:
            result += k.category_name + "\n"
            for x in k.children:
                result += f"    {x.category_name}\n"
                for y in x.children:
                    result += f"        {y.category_name}\n"
                    for z in y.children:
                        result += f"            {z.category_name}\n"
        return result

    def get_categories(self):
        query = f"""
        SELECT CATEG_ID, PARENT_ID, DESCR, LST_MAINT_DT
        FROM EC_CATEG
        """
        response = self.db.query_db(query)
        if response is not None:
            for y in response:
                category = CounterpointCategory(y[0], y[1], y[2], y[3])
                self.categories.add(category)

    def create_tree(self):
        self.get_categories()

        for x in self.categories:
            for y in self.categories:
                if y.parent_category == x.category_id:
                    x.add_child(y)

        self.heads = [x for x in self.categories if x.parent_category is None]


class CounterpointCategory:
    def __init__(self, category_id, parent_category, category_name, description):
        self.category_id = category_id
        self.category_name = category_name
        self.parent_category = parent_category
        self.description = description
        self.children = []

    def add_child(self, child):
        self.children.append(child)


class MiddlewareCategoryTree:
    def __init__(self):
        self.categories = []
        self.get_category_tree()

    def get_category_tree(self):
        pass


class MiddlewareCategory:
    def __init__(self, category_id, parent_id, name, is_visible, depth, path, children, url):
        self.id = category_id
        self.parent_id = parent_id
        self.name = name
        self.is_visible = is_visible
        self.depth = depth
        self.path = path
        self.children = []
        self.url = url
        if children:
            self.instantiate_children(children)

    def instantiate_children(self, children):
        for x in children:
            self.children.append(MiddlewareCategory(category_id=x['id'],
                                                    parent_id=x['parent_id'],
                                                    name=x['name'],
                                                    is_visible=x['is_visible'], depth=x['depth'], path=x['path'],
                                                    children=x['children'], url=x['url']))


def create_category(category):
    # Middleware DB Entry
    db = query_engine.QueryEngine()
    query = f"""
    INSERT INTO SN_CATEG (CATEG_ID, PARENT_ID, DESCR, LST_MAINT_DT)
    VALUES ({category.id}, {category.parent_id}, '{category.name}', GETDATE())
    """
    db.query_db(query, commit=True)

    # Big Commerce API
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/categories"
    payload = {
        "name": category.name,
        "parent_id": category.parent_id,
        "description": category.description,
        "is_visible": category.is_visible,
        "depth": category.depth,
        "path": category.path
    }
    response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
    if response.status_code == 201:
        print(f"Category {category.name} created successfully.")
        print(response.json())
    else:
        print(f"Error creating category {category.name}.")
        print(response.json())


def update_category(category):
    # Middleware DB Entry
    db = query_engine.QueryEngine()
    query = f"""
    UPDATE SN_CATEG
    SET DESCR = '{category.name}', LST_MAINT_DT = GETDATE()
    WHERE CATEG_ID = {category.id}
    """
    db.query_db(query, commit=True)

    # Big Commerce API
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/categories/{category.id}"
    payload = {
        "name": category.name,
        "parent_id": category.parent_id,
        "description": category.description,
        "is_visible": category.is_visible,
        "depth": category.depth,
        "path": category.path
    }
    response = requests.put(url=url, headers=creds.bc_api_headers, json=payload)
    if response.status_code == 200:
        print(f"Category {category.name} updated successfully.")
        print(response.json())
    else:
        print(f"Error updating category {category.name}.")
        print(response.json())


def delete_category(category):
    # Middleware DB Entry
    db = query_engine.QueryEngine()
    query = f"""
    DELETE FROM SN_CATEG
    WHERE CP_CATEG_ID = {category}
    """
    db.query_db(query, commit=True)

    # Big Commerce API
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/categories/{category.id}"
    response = requests.delete(url=url, headers=creds.bc_api_headers)
    if response.status_code == 204:
        print(f"Category {category.name} deleted successfully.")
    else:
        print(f"Error deleting category {category.name}.")
        print(response.json())


def compare_category_trees(counterpoint_tree, middleware_tree) -> dict:
    result = {
        "create": [],
        "update": [],
        "delete": []
    }
    for category in counterpoint_tree.categories:
        if category not in middleware_tree.categories:
            result['create'].append(category)
        else:
            result['update'].append(category)
    for category in middleware_tree.categories:
        if category not in counterpoint_tree.categories:
            result['delete'].append(category)
    return result


def update_category_tree():
    counterpoint_tree = CounterpointCategoryTree()
    middleware_tree = MiddlewareCategoryTree()
    queue = compare_category_trees(counterpoint_tree, middleware_tree)
    for category in queue['create']:
        create_category(category)
    for category in queue['update']:
        update_category(category)
    for category in queue['delete']:
        delete_category(category)


# ------------------------
# PRODUCTS
# -----------------------
class MiddlewareCatalog:
    def __init__(self):
        self.products = []
        self.get_products()

    def get_products(self):
        query = f"""
        SELECT ITEM_NO
        FROM SN_PRODUCTS
        """
        response = query_engine.QueryEngine().query_db(query)
        if response is not None:
            for item in response:
                self.products.append(item[0])


class BoundProduct:
    def __init__(self, binding_id: str):
        self.db = query_engine.QueryEngine()
        self.binding_id: str = binding_id
        self.product_id: int = 0
        self.total_children: int = 0
        # self.children will be list of child products
        self.children: list = []
        # self.parent will be a list of parent products. If length of list > 1, product validation will fail
        self.parent: list = []

        # Product Information
        self.web_title: str = ""
        self.default_price = 0
        self.cost = 0
        self.sale_price = 0
        self.is_price_hidden = False
        self.brand: str = ""
        self.html_description: str = ""
        self.search_keywords: str = ""
        self.meta_title: str = ""
        self.meta_description: str = ""
        self.visible: bool = False
        self.featured: bool = False
        self.gift_wrap: bool = False

        # Product Details
        self.search_keywords: str = ""
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

        self.processing_method = ""

        # Get Bound Product Family
        self.get_bound_product_family()

        # Validate Product
        self.validation_retries = 10

    def __str__(self):
        result = ""
        line = "-" * 25 + "\n\n"
        result += f"Printing Bound Product Details for: {self.binding_id}\n"
        for k, v in BoundProduct(self.binding_id).__dict__.items():
            result += f"{k}: {v}\n"
        result += line
        result += "Printing Child Product Details\n"
        child_index = 1
        for child in BoundProduct(self.binding_id).children:
            result += f"Child: {child_index}\n"
            result += line
            for k, v in child.__dict__.items():
                result += f"{k}: {v}\n"
            result += line
        return result

    def get_bound_product_family(self):
        # clear children list
        self.children = []
        query = f"""
        SELECT ITEM_NO
        FROM IM_ITEM
        WHERE USR_PROF_ALPHA_16 = '{self.binding_id}' and IS_ECOMM_ITEM = 'Y'
        """
        response = self.db.query_db(query)
        if response is not None:
            for item in response:
                self.children.append(Product(item[0]))
        # Set parent
        self.parent = [x.sku for x in self.children if x.is_parent]
        # Set total children
        self.total_children = len(self.children)

        # Product Information
        for x in self.children:
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

    def validate_product(self):
        while self.validation_retries > 0:
            print(f"Validating {self.binding_id}")

            # Test for missing binding ID. Potentially add corrective action (i.e. generate binding ID or remove product
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
                    self.set_parent()
                    self.validation_retries -= 1
                    return self.validate_product()

                # Test for multiple parents
                if len(self.parent) > 1:
                    print(f"Product {self.binding_id} has multiple parents. Will reestablish parent.")
                    self.remove_parent()
                    self.set_parent()
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
            for child in self.children:
                if child.variant_name == "":
                    print(f"Product {child.sku} is missing a variant name. Validation failed.")
                    return False
            print(f"Product {self.binding_id} is valid.")
            print("\n")
            return True

    def get_processing_method(self, middleware_catalog):
        if self.binding_id in middleware_catalog.products:
            self.processing_method = "update"
        else:
            self.processing_method = "create"

    def process(self, mode: str):
        if mode == "create":
            self.create_bound_product()
        elif mode == "update":
            self.update_bound_product()

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

    def construct_image_payload(self):
        result = []
        # Parent Images

        # Child Images
        for child in self.children:
            for image in child.images:
                result.append({
                    "image_file": image.name,
                    "is_thumbnail": image.is_thumbnail,
                    "sort_order": image.sort_order,
                    "description": image.alt_text_1,
                    "image_url": f"{creds.public_web_dav_photos}/{image.name}",
                    "id": 0,
                    "product_id": child.product_id,
                    "date_modified": image.modified_date
                })

    def construct_variant_payload(self):
        result = []
        id_index = 1
        for child in self.children:
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

    def construct_product_payload(self):
        payload = {
            "name": self.web_title,
            "type": "physical",
            "sku": self.binding_id,
            "description": self.html_description,
            "weight": 9999999999,
            "width": 9999999999,
            "depth": 9999999999,
            "height": 9999999999,
            "price": self.default_price,
            "cost_price": self.cost,
            "retail_price": self.default_price,
            "sale_price": self.sale_price,
            "map_price": 0,
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
            "is_visible": self.visible,
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
            "is_preorder_only": self.is_preorder,
            "is_price_hidden": self.is_price_hidden,
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
            "images": [
                # {
                #   "image_file": "string",
                #   "is_thumbnail": true,
                #   "sort_order": -2147483648,
                #   "description": "string",
                #   "image_url": "string",
                #   "id": 0,
                #   "product_id": 0,
                #   "date_modified": "2019-08-24T14:15:22Z"
                # }
            ],
            "videos": [
                {
                    "title": "Writing Great Documentation",
                    "description": "A video about documenation",
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

    def create_bound_product(self):
        self.bc_create_product()
        self.middleware_create_product()

    def bc_create_product(self):
        payload = self.construct_product_payload()
        url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products"
        response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
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
        url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}"
        response = requests.put(url=url, headers=creds.bc_api_headers, json=payload)
        if response.status_code == 200:
            print(f"Product {self.binding_id} updated successfully.")
        else:
            print(f"Error updating product {self.binding_id}.")
            print(response.json())

    def middleware_update_product(self):
        pass

    def bc_update_variant(self, child):
        payload = self.construct_variant_payload(child)

    def remove_parent(self):
        """Remove parent status from all children"""
        query = f"""
                UPDATE IM_ITEM 
                SET IS_ADM_TKT = 'N', LST_MAINT_DT = GETDATE()
                WHERE USR_PROF_ALPHA_16 = '{self.binding_id}'
                """
        self.db.query_db(query, commit=True)
        print("Parent status removed from all children.")

    def set_parent(self, status: bool = True) -> None:
        """Target lowest price item in family to set as parent."""
        # Reestablish parent relationship
        flag = 'Y' if status else 'N'

        target_item = min(self.children, key=lambda x: x.price_1).sku

        query = f"""
        UPDATE IM_ITEM
        SET IS_ADM_TKT = '{flag}', LST_MAINT_DT = GETDATE()
        WHERE ITEM_NO = '{target_item}'
        """
        self.db.query_db(query, commit=True)
        print(f"Parent status set to {flag} for {target_item}")
        print("Reestablishing products.")
        return self.get_bound_product_family()


class Product:
    def __init__(self, sku: str):
        # Product ID Info
        self.sku: str = sku
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

        query = f"""
        select ITEM.USR_PROF_ALPHA_16, ITEM.IS_ECOMM_ITEM, ITEM.IS_ADM_TKT, ITEM.USR_CPC_IS_ENABLED, 
        ITEM.USR_ALWAYS_ONLINE, ITEM.IS_FOOD_STMP_ITEM, ITEM.PROF_COD_1, ITEM.ECOMM_NEW, ITEM.USR_IN_STORE_ONLY, 
        ITEM.USR_PROF_ALPHA_27, ITEM.ADDL_DESCR_1, ITEM.USR_PROF_ALPHA_17, ITEM.REG_PRC, ITEM.PRC_1, PRC.PRC_2,
        ISNULL(INV.QTY_AVAIL, 0), ISNULL(ITEM.PROF_NO_1, 0), ITEM.ITEM_TYP,ITEM.CATEG_COD, ITEM.SUBCAT_COD, ITEM.DESCR, 
        ITEM.LONG_DESCR, ITEM.USR_PROF_ALPHA_26, ITEM.USR_PROF_ALPHA_19, ITEM.ADDL_DESCR_2, USR_PROF_ALPHA_21, 
        EC_ITEM_DESCR.HTML_DESCR, ITEM.STAT, USR_PROF_ALPHA_22, USR_PROF_ALPHA_23, USR_PROF_ALPHA_24, USR_PROF_ALPHA_25,
        PROF_ALPHA_1, PROF_ALPHA_2, PROF_ALPHA_3, PROF_ALPHA_4, PROF_ALPHA_5, USR_PROF_ALPHA_6, USR_PROF_ALPHA_7, 
        USR_PROF_ALPHA_8, USR_PROF_ALPHA_9, USR_PROF_ALPHA_10, USR_PROF_ALPHA_11, USR_PROF_ALPHA_12, USR_PROF_ALPHA_13, 
        USR_PROF_ALPHA_14, USR_PROF_ALPHA_15, 
        ITEM.LST_MAINT_DT, INV.LST_MAINT_DT, PRC.LST_MAINT_DT,EC_ITEM_DESCR.LST_MAINT_DT, EC_CATEG_ITEM.LST_MAINT_DT,
        EC_CATEG_ITEM.CATEG_ID, ITEM.LST_COST, COD.DESCR
        
        FROM IM_ITEM ITEM
        
        INNER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
        
        LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
        LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
        LEFT OUTER JOIN EC_CATEG_ITEM ON ITEM.ITEM_NO=EC_CATEG_ITEM.ITEM_NO
        LEFT OUTER JOIN EC_CATEG ON EC_CATEG.CATEG_ID=EC_CATEG_ITEM.CATEG_ID
        INNER JOIN IM_ITEM_PROF_COD COD ON ITEM.PROF_COD_1 = COD.PROF_COD

        
        WHERE ITEM.ITEM_NO = '{self.sku}' and ITEM.IS_ECOMM_ITEM = 'Y'
        """
        response = db.query_db(query)
        if response is not None:
            self.binding_id: str = response[0][0] if response[0][0] else ""
            self.is_bound: bool = True if self.binding_id != "" else False

            # self.product_id: int = 0 This could be in separate table or would need to add columns to IM_ITEM
            # self.variant_id: int = 0 This could be in separate table or would need to add columns to IM_ITEM

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
        print(f"Validating product {self.sku}")

        # Test for missing web title
        if self.web_title == "":
            print(f"Product {self.sku} is missing a web title. Validation failed.")
            return False

        # Test for missing html description
        if self.html_description == "":
            print(f"Product {self.sku} is missing an html description. Validation failed.")
            return False

        # Test for missing E-Commerce Categories
        if len(self.ecommerce_categories) == 0:
            print(f"Product {self.sku} is missing E-Commerce Categories. Validation failed.")
            return False

        # Test for missing brand
        if self.brand == "":
            print(f"Product {self.sku} is missing a brand. Validation failed.")
            return False

        # Test for missing cost
        if self.cost == 0:
            print(f"Product {self.sku} is missing a cost. Validation failed.")
            return False

        # Test for missing price 1
        if self.price_1 == 0:
            print(f"Product {self.sku} is missing a price 1. Validation failed.")
            return False

        # Test for missing weight
        if self.weight == 0:
            print(f"Product {self.sku} is missing a weight. Validation failed.")
            return False

        return True

    def construct_variant_payload(self):
        payload = {
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
            "sku": self.sku,
            "option_values": [
                {
                    "option_display_name": "Option",
                    "label": self.variant_name,
                }
            ]
        }
        return payload

    def get_processing_method(self, middleware_catalog) -> None:
        if self.sku not in middleware_catalog.products:
            self.processing_method = "create"
        else:
            self.processing_method = "update"

    def process(self, mode: str) -> None:
        if mode == "create":
            self.create_product()

        elif mode == "update":
            self.update_product()

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
                if x.split(".")[0].split("^")[0] == self.sku:
                    self.images.append(ProductImage(x))

    def get_bc_product_images(self):
        """Get BigCommerce image information for product"""
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}/images'
        response = requests.get(url=url, headers=creds.bc_api_headers)
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
                    "description": "A video about documenation",
                    "sort_order": 1,
                    "type": "youtube",
                    "video_id": "z3fRu9pkuXE",
                    "id": 0,
                    "product_id": 0,
                    "length": "string"
                }
            ],
            # "variants": [
            #     {
            #         "cost_price": 0.1,
            #         "price": 0.1,
            #         "sale_price": 0.1,
            #         "retail_price": 0.1,
            #         "weight": 0.1,
            #         "width": 0.1,
            #         "height": 0.1,
            #         "depth": 0.1,
            #         "is_free_shipping": False,
            #         "fixed_cost_shipping_price": 0.1,
            #         "purchasing_disabled": False,
            #         "purchasing_disabled_message": "string",
            #         "upc": "string",
            #         "inventory_level": 2147483647,
            #         "inventory_warning_level": 2147483647,
            #         "bin_picking_number": "string",
            #         "mpn": "string",
            #         "gtin": "012345678905",
            #         "product_id": 0,
            #         "id": 0,
            #         "sku": "string",
            #         "option_values": [
            #             {
            #                 "option_display_name": "Color",
            #                 "label": "Beige"
            #             }
            #         ],
            #         "calculated_price": 0.1,
            #         "calculated_weight": 0
            #     }
            # ],
        }
        return payload

    def bc_create_product(self):
        """Create product in BigCommerce. For this implementation, this is a single product with no variants"""
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products'
        payload = self.construct_product_payload()
        response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
        return response.json()

    def middleware_create_product(self):
        """NOT DONE"""
        query = f"""
        INSERT INTO SN_PRODUCTS
        (ITEM_NO, WEB_TITLE, DESCRIPTION, HTML_DESCRIPTION)
        VALUES ('{self.sku}', '{self.web_title}', '{self.description}', '{self.html_description}'),
        """
        query_engine.QueryEngine().query_db(query, commit=True)

    def bc_update_product(self):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}'
        payload = self.construct_product_payload()
        response = requests.put(url=url, headers=creds.bc_api_headers, json=payload)
        return response.json()

    def middleware_update_product(self):
        query = f"""
        UPDATE SN_PRODUCTS
        SET WEB_TITLE = '{self.web_title}', DESCRIPTION = '{self.description}', HTML_DESCRIPTION = '{self.html_description}'
        WHERE ITEM_NO = '{self.sku}'
        """
        query_engine.QueryEngine().query_db(query, commit=True)

    def create_product(self):
        self.bc_create_product()
        self.middleware_create_product()

    def update_product(self):
        self.bc_update_product()
        self.middleware_update_product()

    def bc_delete_product(self):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}'
        response = requests.delete(url=url, headers=creds.bc_api_headers)
        return response.json()


# ------------------------
# IMAGES
# -----------------------

class ProductImage:
    def __init__(self, image_name: str, item_no="", file_path="", image_url="", product_id=0, variant_id=0, image_id=0,
                 is_thumbnail=False, sort_order=0, is_binding_image=False, is_binding_id=None, is_variant_image=False,
                 description="", lst_maint_dt=datetime(1970, 1, 1),
                 lst_run_time=datetime(1970, 1, 1)):

        self.image_name = image_name
        self.item_no = item_no
        self.file_path = file_path
        self.image_url = image_url
        self.product_id = product_id
        self.variant_id = variant_id
        self.image_id = image_id
        self.is_thumbnail = is_thumbnail
        self.sort_order = sort_order
        self.is_binding_image = is_binding_image
        self.binding_id = is_binding_id
        self.is_variant_image = is_variant_image
        self.description = description
        self.lst_maint_dt = lst_maint_dt

        if self.lst_maint_dt > lst_run_time:
            # Image has been updated since last run. Check image for valid size and format.
            if self.validate_image():
                self.initialize_image_details()

        else:
            # Image has not been updated since last run. Get image details from database.
            self.get_image_details_from_db()

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

    def get_image_details_from_db(self):
        db = query_engine.QueryEngine()
        query = f"""
        SELECT ITEM_NO, FILE_PATH, IMAGE_URL, PRODUCT_ID, VARIANT_ID, IMAGE_ID, THUMBNAIL, SORT_ORDER, 
        IS_BINDING_IMAGE, BINDING_ID, IS_VARIANT_IMAGE, DESCR, LST_MAINT_DT FROM SN_IMAGES WHERE IMAGE_NAME = '{self.image_name}'
        """
        response = db.query_db(query)
        if response is not None:
            self.item_no = response[0][1] if response[0][1] else ""
            self.file_path = response[0][2]
            self.image_url = response[0][3]
            self.product_id = response[0][4]
            self.variant_id = response[0][5]
            self.image_id = response[0][6]
            self.is_thumbnail = True if response[0][7] == 1 else False
            self.sort_order = response[0][8]
            self.is_binding_image = True if response[0][9] == 1 else False
            self.binding_id = response[0][10] if response[0][10] else None
            self.is_variant_image = True if response[0][11] == 1 else False
            self.description = response[0][12]
            self.lst_maint_dt = response[0][13]

    def initialize_image_details(self):
        # Path
        self.file_path = f"{creds.photo_path}/{self.image_name}"

        # URL
        self.image_url = self.upload_product_image()

        # Sort Order
        if "^" not in self.image_name.split(".")[0]:
            self.sort_order = 1
        else:
            self.sort_order = int(self.file_path.split(".")[0].split("^")[1]) + 1

        # Dates
        self.lst_maint_dt = datetime.fromtimestamp(os.path.getmtime(self.file_path))

        binding_ids = get_all_binding_ids()
        # Binding Image Flow
        if self.image_name.split(".")[0].split("^")[0] in binding_ids:
            self.is_binding_image = True
            self.binding_id = self.image_name.split(".")[0].split("^")[0]
            self.is_thumbnail = True if "^" not in self.image_name.split(".")[0] else False

        # Non-Binding Image Flow
        else:
            self.is_binding_image = False
            # SKU
            self.item_no = self.image_name.split(".")[0].split("^")[0]
            self.binding_id = self.get_binding_id()

            # No Binding ID
            if self.binding_id == "":
                self.is_thumbnail = True if "^" not in self.image_name.split(".")[0] else False
            # Binding ID
            else:
                if "^" not in self.image_name.split(".")[0]:
                    self.is_variant_image = True

            # Image Description
            # Only non-binding images have descriptions at this time. Though, this could be handled with JSON reference
            # in the future for binding images.
            for x in range(1, 5):
                if self.image_name.split(".")[0].split("^")[1] == x:
                    self.description = self.get_image_description(x)

        self.product_id, self.variant_id = self.get_product_and_variant_ids()
        self.image_id = self.bc_post_image()
        self.write_image_to_db()

    def write_image_to_db(self):
        query = f"""
        INSERT INTO SN_IMAGES
        (IMAGE_NAME, ITEM_NO, FILE_PATH, IMAGE_URL, PRODUCT_ID, VARIANT_ID, IMAGE_ID, THUMBNAIL, SORT_ORDER, 
        IS_BINDING_IMAGE, BINDING_ID, IS_VARIANT_IMAGE, DESCR, LST_MAINT_DT)
        VALUES ('{self.image_name}', '{self.item_no}', '{self.file_path}', '{self.image_url}', {self.product_id}, 
        {self.variant_id}, {self.image_id}, {1 if self.is_thumbnail else 0}, {self.sort_order}, 
        {1 if self.is_binding_image else 0}, '{self.binding_id if self.binding_id else None}', 
        {1 if self.is_variant_image else 0}, '{self.description}', '{self.lst_maint_dt}')
        """
        try:
            query_engine.QueryEngine().query_db(query, commit=True)
        except Exception as e:
            print(f"Error writing image to db: {e}")
        else:
            print(f"Image {self.image_name} written to db.")

    def get_product_and_variant_ids(self):
        query = f"""
        SELECT PRODUCT_ID, VARIANT_ID FROM {creds.bc_product_table} WHERE ITEM_NO = '{self.item_no}'
        """
        response = query_engine.QueryEngine().query_db(query)
        if response is not None:
            return response[0][0], response[0][1]
        else:
            return 0, 0

    def get_binding_id(self):
        query = f"""
        SELECT USR_PROF_ALPHA_16 FROM IM_ITEMS
        WHERE ITEM_NO = '{self.item_no}'
        """
        response = query_engine.QueryEngine().query_db(query)
        if response is not None:
            return response[0][0] if response[0][0] else ""

    def get_image_description(self, image_number):
        query = f"""
        SELECT USR_PROF_ALPHA_{image_number + 21} FROM IM_ITEMS
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
        url = creds.public_web_dav_photos + self.image_name
        response = requests.put(url, data=data, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw))
        print(response.status_code)
        # return public url of image
        return f"{creds.public_web_dav_photos}/{self.image_name}"

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
        url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
               f'products/{self.product_id}/images/{self.bc_image_id}')
        response = requests.get(url=url, headers=creds.bc_api_headers)
        return response.content

    def bc_post_image(self) -> int:
        if self.is_variant_image:
            url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
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
            response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
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
            url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog'
                   f'products/{self.product_id}/images')
            payload = {
                "product_id": self.product_id,
                "is_thumbnail": self.is_thumbnail,
                "sort_order": self.sort_order,
                "description": self.description,
                "image_url": self.image_url
            }
            print(f"Posting image {self.image_name} to item {self.item_no}.")
            response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
            if response.status_code == 200:
                return response.json()["data"]["id"]

            elif response.status_code == 404:
                print("Product not found.")
                return 0
            else:
                print(f"Error posting image: {response.content}")
                return 0

    def bc_update_product_image(self, source_url, description=""):
        url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
               f'products/{self.product_id}/images/{self.bc_image_id}')
        payload = {
            "product_id": self.product_id,
            "is_thumbnail": self.is_thumbnail,
            "sort_order": self.sort_order,
            "description": description,
            "image_url": source_url
        }
        response = requests.put(url=url, headers=creds.bc_api_headers, json=payload)
        return response.content

    def bc_delete_image(self):
        """Photos can either be variant images or product images. Two flows in this function"""
        if self.is_variant_image:
            url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
                   f'products/{self.product_id}/variants/{self.variant_id}/images/{self.bc_image_id}')
            response = requests.delete(url=url, headers=creds.bc_api_headers)

        else:
            url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
                   f'products/{self.product_id}/images/{self.bc_image_id}')
            response = requests.delete(url=url, headers=creds.bc_api_headers)

        return response.content

    def sql_post_photo(self):
        # query = f"""
        # INSERT INTO EC_PHOTO (PHOTO_NAME, PHOTO_SIZE, PHOTO_RESOLUTION, PHOTO_ORIENTATION, PHOTO_QUALITY,
        # PHOTO_EXIF, PHOTO_EXIF_ORIENTATION, PHOTO_MODIFIED_DATE, PHOTO_CREATED_DATE, PRODUCT_ID, VARIANT_ID,
        # BC_IMAGE_ID, SORT_ORDER, IS_THUMBNAIL, IS_BOUND, IS_BINDING_IMAGE)
        # VALUES ('{self.name}', {self.size}, '{self.image_resolution}', {self.image_orientation}, {self.image_quality},
        # '{self.image_exif}', {self.image_exif_orientation}, '{self.modified_date}', '{self.created_date}',
        # {self.product_id}, {self.variant_id}, {self.bc_image_id}, {self.sort_order}, {self.is_thumbnail},
        # {self.is_bound}, {self.is_binding_image})
        # """
        # self.db.query_db(query, commit=True)
        # print(f"Photo {self.name} added to database.")
        pass

    def sql_update_photo(self):
        # query = f"""
        # UPDATE EC_PHOTO
        # SET PHOTO_SIZE = {self.size}, PHOTO_RESOLUTION = '{self.image_resolution}', PHOTO_ORIENTATION = {self.image_orientation},
        # PHOTO_QUALITY = {self.image_quality}, PHOTO_EXIF = '{self.image_exif}', PHOTO_EXIF_ORIENTATION = {self.image_exif_orientation},
        # PHOTO_MODIFIED_DATE = '{self.modified_date}', PHOTO_CREATED_DATE = '{self.created_date}', PRODUCT_ID = {self.product_id},
        # VARIANT_ID = {self.variant_id}, BC_IMAGE_ID = {self.bc_image_id}, SORT_ORDER = {self.sort_order}, IS_THUMBNAIL = {self.is_thumbnail},
        # IS_BOUND = {self.is_bound}, IS_BINDING_IMAGE = {self.is_binding_image}
        # WHERE PHOTO_NAME = '{self.name}'
        # """
        # self.db.query_db(query, commit=True)
        # print(f"Photo {self.name} updated in database.")
        pass

    def sql_delete_photo(self):
        query = f"DELETE FROM {creds.image_table} WHERE IMAGE_NAME = '{self.name}'"
        query_engine.QueryEngine().query_db(query, commit=True)
        print(f"Photo {self.name} deleted from database.")


# ------------------------
# STATIC METHODS
# -----------------------
def get_updated_photos(date):
    result = set()
    # Iterate over all files in the directory
    for filename in os.listdir(creds.photo_path):
        file_path = os.path.join(creds.photo_path, filename)

        # Get the last modified date of the file
        modified_date = datetime.fromtimestamp(os.path.getmtime(file_path))

        # If the file has been modified since the input date, print its name
        if modified_date > date:
            sku = filename.split(".")[0].split("^")[0]
            if sku != "":
                result.add(sku)
    return result


def get_all_images(mode="local"):
    if mode == "local":
        return [x for x in os.listdir(creds.photo_path) if x not in ["", ".DS_Store"]]
    elif mode == "middleware":
        query = f"SELECT IMAGE_NAME FROM SN_IMAGES"
        response = query_engine.QueryEngine().query_db(query)
        if response is not None:
            return [x[0] for x in response]


def update_product_timestamps(sku_list, table_name):
    """Takes in a list of SKUs and updates the last maintenance date in input table for each product in the list"""
    tuple_list = tuple(sku_list)
    db = query_engine.QueryEngine()

    query = f"""
    UPDATE '{table_name}
    SET LST_MAINT_DT = GETDATE()
    WHERE ITEM_NO in {tuple_list}
    """

    try:
        db.query_db(query, commit=True)
    except Exception as e:
        print(f"Error updating product timestamps: {e}")
    else:
        print("Product timestamps updated: ", sku_list)


def get_updated_products(last_run_date):
    """Get a list of all products that have been updated since the last run date. Will check IM_ITEM, IM_PRC, IM_INV,
    EC_ITEM_DESCR, and EC_CATEG_ITEM tables for updates."""

    updated_products = set()
    db = query_engine.QueryEngine()
    query = f"""
    SELECT ITEM.ITEM_NO
    FROM IM_ITEM ITEM
    LEFT OUTER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
    LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
    LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
    LEFT OUTER JOIN EC_CATEG_ITEM ON ITEM.ITEM_NO=EC_CATEG_ITEM.ITEM_NO
    WHERE ITEM.LST_MAINT_DT > '{last_run_date}' or PRC.LST_MAINT_DT > '{last_run_date}' 
    or INV.LST_MAINT_DT > '{last_run_date}' or EC_ITEM_DESCR.LST_MAINT_DT > '{last_run_date}'
    or EC_CATEG_ITEM.LST_MAINT_DT > '{last_run_date}'
    """
    response = db.query_db(query)
    if response is not None:
        for x in response:
            updated_products.add(x[0])

    return list(updated_products)


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


def get_binding_ids_to_process(product_list):
    binding_ids = set()
    for product in product_list:
        binding_id = get_binding_id_from_sku(product)
        if binding_id:
            binding_ids.add(binding_id)
    return list(binding_ids)


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


def get_deletion_target(counterpoint_list, middleware_list):
    return [element for element in counterpoint_list if element not in middleware_list]


def process_product_deletions():
    # Products
    # ------------------------
    # Step 1 - Get list of all products in Counterpoint
    cp_products = get_all_products()

    # Step 2 - Get list of all products in Middleware
    middleware_products = get_all_middleware_products()

    # Step 3 - Get list of products to delete
    deletions = get_deletion_target(cp_products, middleware_products)

    # Step 4 - Delete products
    for product in deletions:
        delete_product(product)


def process_image_deletions(last_run_date):
    # Step 1 - Get list of all images in ItemImages Folder
    images = get_all_images(mode="local")

    # Step 2 - Get list of all images in Middleware
    middleware_images = get_all_images(mode="middleware")

    # Step 3 - Get list of images to delete
    deletions = get_deletion_target(images, middleware_list=middleware_images)

    # Step 4 - Delete images
    for image in deletions:
        im = ProductImage(image)
        # delete image from BigCommerce
        im.bc_delete_product_image()
        # delete image from Middleware
        im.sql_delete_photo()


def get_all_categories(mode="counterpoint"):
    db = query_engine.QueryEngine()
    if mode == "counterpoint":
        query = f"""
        SELECT CP_CATEG_ID
        FROM SN_CATEGORIES
        """
    else:
        query = f"""
        SELECT CP_CATEG_ID
        FROM SN_CATEGORIES
        """
    response = db.query_db(query)
    if response is not None:
        return [x[0] for x in response]


def process_category_deletions():
    # Step 1 - Get list of all categories in Counterpoint
    cp_categories = get_all_categories(mode="counterpoint")

    # Step 2 - Get list of all categories in Middleware
    middleware_categories = get_all_categories(mode="middleware")

    # Step 3 - Get list of categories to delete
    deletions = get_deletion_target(cp_categories, middleware_list=middleware_categories)

    # Step 4 - Delete categories
    for category in deletions:
        delete_category(category)


def process_deletions(last_run_date):
    process_image_deletions(last_run_date)
    process_product_deletions(last_run_date)
    process_category_deltions(last_run_date)


# ------------------------
# DATABASE UTILITIES
# -----------------------
def create_image_table(table_name):
    db = query_engine.QueryEngine()
    query = f"""
        CREATE TABLE {table_name} (
        IMAGE_NAME nvarchar(255) NOT NULL PRIMARY KEY,
        ITEM_NO varchar(50),
        FILE_PATH nvarchar(255) NOT NULL,
        IMAGE_URL nvarchar(255),
        PRODUCT_ID int NOT NULL,
        VARIANT_ID int,
        IMAGE_ID int NOT NULL,
        THUMBNAIL BIT NOT NULL,
        SORT_ORDER int NOT NULL,
        IS_BINDING_IMAGE BIT NOT NULL,
        BINDING_ID varchar(50),
        IS_VARIANT_IMAGE BIT NOT NULL,
        DESCR nvarchar(100),
        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
        );
        """
    db.query_db(query, commit=True)


def drop_table(table_name):
    db = query_engine.QueryEngine()
    query = f"DROP TABLE {table_name}"
    db.query_db(query, commit=True)


def create_product_table(table_name):
    db = query_engine.QueryEngine()
    query = f"""
    CREATE TABLE {table_name} (
    
    ID int NOT NULL PRIMARY KEY,
    ITEM_NO varchar(50) NOT NULL,
    BINDING_ID varchar(10),
    IS_PARENT BIT,
    PRODUCT_ID int NOT NULL,
    VARIANT_ID int,
    BC_CATEG_ID int NOT NULL FOREIGN KEY REFERENCES {creds.category_table}(BC_CATEG_ID),
    LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
    );
    """
    db.query_db(query, commit=True)


def create_category_table(table_name):
    db = query_engine.QueryEngine()
    query = f"""
    CREATE TABLE {table_name} (
    BC_CATEG_ID int NOT NULL PRIMARY KEY,
    BC_PARENT_CATEG int,
    CP_CATEG_ID int NOT NULL,
    LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
    );
    """
    db.query_db(query, commit=True)


# ------------------------
# DRIVER
# -----------------------
def process_bound_products(binding_ids, middleware_catalog):
    """Takes in a list of binding ids and creates BoundProduct objects for each one. If the product passes input
    validation, it will compare the product/image/category details against those in the middleware database and update
    Returns a list of all BoundProduct objects that have been successfully updated."""
    result = []
    for x in binding_ids:
        bound_product = BoundProduct(x)
        if bound_product.validate_product():
            bound_product.get_processing_method(middleware_catalog)
            bound_product.process(mode=bound_product.processing_method)
            # Build Result List
            for child in bound_product.children:
                result.append(child.sku)
    return result


def process_single_items(product_list, middleware_catalog):
    """Takes in a list of product SKUs and creates Product objects for each one. If the product passes input validation,
    it will compare the product/image/category details against those in the middleware database and update. Returns a list
    of all products that have been successfully updated."""
    result = []

    while len(product_list) > 0:
        product = Product(product_list.pop())
        if product.validate_product():
            product.get_processing_method(middleware_catalog)
            product.process(mode=product.processing_method)
        # Bear in mind: Putting result append here will put successes and failures in the same list.
        result.append(product.sku)
    return result


def product_integration(last_run_date):
    # Step 0
    # Open Time-Stamped Log File to Pass into other functions
    # log_file = open(f"logs/product_integration_{datetime.now():%Y_%m_%d_%H_%M_%S}.txt", "w")

    # Step 1
    updated_photos = get_updated_photos(last_run_date)
    # update LST_MAINT_DT for all updated images in image table
    update_product_timestamps(updated_photos, creds.image_table)

    # Step 2 - Update E-Commerce Category Tree
    update_category_tree()

    # Step 3 - Get list of all modified e-commerce items
    e_commerce_items = get_updated_products(last_run_date)

    # Step 4 - Get list of valid binding keys associated with these items
    binding_ids = get_binding_ids_to_process(e_commerce_items)

    # Step 5 - Generate snapshot of middleware database
    middleware_catalog = MiddlewareCatalog()

    # Step 5 - Create BoundProduct objects for each binding key. This includes all child products and will validate
    # the inputs from Counterpoint. If the product passes input validation (through initial validation or through
    # repair and reattempt), update product details in database and BigCommerce or post product if it is new.
    if len(binding_ids) > 0:
        finished_products = process_bound_products(binding_ids, middleware_catalog)
        # Step 6 - Remove finished products from e_commerce_items to prevent reprocessing
        e_commerce_items.remove(finished_products)
    else:
        print("No bound products to process.")

    # Step 7 - Process single items
    if len(e_commerce_items) > 0:
        process_single_items(e_commerce_items, middleware_catalog)
    else:
        print("No single items to process.")

    # Step 8 - Process Deletions
    process_deletions()
    # Step 8 - Close Log File
    # log_file.close()
