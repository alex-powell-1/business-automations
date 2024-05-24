import os
import re
from datetime import datetime

import requests
from PIL import Image

import product_tools.products
from setup import creds
from setup import query_engine
from setup import date_presets

"""


For each binding key, create a BoundProduct object which will contain all child products and will validate the inputs
from counterpoint. If the product passes input validation (through initial validation or through repair and reattempt), 
update product details in database. BigCommerce or post product if it is new.

If it fails validation, log the product and the reason for failure.

All SKUs associated with this binding key will be removed from the list of e-commerce items to process. When all bound
products are done, any remaining items in the list of e-commerce items will be processed as single items (Product class)

Script will run on a schedule and will check ItemImages folder for images updated since last run time. Affected items
will have their lst_maint_dt updated.

For images, save a local copy of the image_id, modified date, and sort order for each image. 

"""


# ------------------------
# CATEGORY TREES
# -----------------------

class CategoryTree:
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
                category = Category(y[0], y[1], y[2], y[3])
                self.categories.add(category)

    def create_tree(self):
        self.get_categories()

        for x in self.categories:
            for y in self.categories:
                if y.parent_category == x.category_id:
                    x.add_child(y)

        self.heads = [x for x in self.categories if x.parent_category is None]


class Category:
    def __init__(self, category_id, parent_category, category_name, description):
        self.category_id = category_id
        self.category_name = category_name
        self.parent_category = parent_category
        self.description = description
        self.children = []

    def add_child(self, child):
        self.children.append(child)


class BigCommerceCategoryTree:
    def __init__(self):
        self.categories = []
        self.get_category_tree()

    def __str__(self):
        result = ""
        for category in self.categories:
            result += f"{category.id}: {category.name}\n"
            for child in category.children:
                result += f"    {child.id}: {child.name}\n"
                for grandchild in child.children:
                    result += f"        {grandchild.id}: {grandchild.name}\n"
                    for great_grandchild in grandchild.children:
                        result += f"            {great_grandchild.id}: {great_grandchild.name}\n"
        return result

    def get_category_tree(self):
        url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees/1/categories"
        response = requests.get(url=url, headers=creds.bc_api_headers)
        for x in response.json()['data']:
            self.categories.append(BigCommerceCategory(category_id=x['id'],
                                                       parent_id=x['parent_id'],
                                                       name=x['name'],
                                                       is_visible=x['is_visible'], depth=x['depth'], path=x['path'],
                                                       children=x['children'], url=x['url']))


class BigCommerceCategory:
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
            self.children.append(BigCommerceCategory(category_id=x['id'],
                                                     parent_id=x['parent_id'],
                                                     name=x['name'],
                                                     is_visible=x['is_visible'], depth=x['depth'], path=x['path'],
                                                     children=x['children'], url=x['url']))


# ------------------------
# PRODUCTS
# -----------------------
class BoundProduct:
    def __init__(self, binding_id: str):
        self.db = query_engine.QueryEngine()
        self.binding_id: str = binding_id
        self.product_id: int = 0
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

            # Test for valid Binding ID Schema
            # example: B0001
            pattern = r'B\d{4}'
            if not bool(re.fullmatch(pattern, self.binding_id)):
                print(f"Product {self.binding_id} has an invalid binding ID. Validation failed.")
                return False

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

    def update_bound_product(self):
        payload = self.construct_product_payload()

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
                    self.images.append(ProductImage(x, bound=self.is_bound))

    def get_bc_product_images(self):
        """Get BigCommerce image information for product"""
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{self.product_id}/images'
        response = requests.get(url=url, headers=creds.bc_api_headers)
        if response is not None:
            for x in response.json():
                # Could use this to back-fill database with image id and sort order info
                pass


# ------------------------
# IMAGES
# -----------------------

class ProductImage:
    def __init__(self, image_name: str, bound=False):
        self.in_database = False
        self.db_id = 0
        self.item_no = ""
        self.name = image_name
        self.path = ""
        self.url = ""
        self.type = image_name.split(".")[-1].lower()
        self.size = 0
        self.modified_date = 0
        self.created_date = 0
        self.product_id = 0
        self.variant_id = 0
        self.bc_image_id = 0
        self.sort_order = 0
        self.is_thumbnail = False
        self.is_bound = bound
        self.is_binding_image = False
        self.description = ""
        # self.image_resolution = (0, 0)
        # self.image_orientation = 0
        # self.image_quality = 0
        # self.image_exif = {}
        # self.image_exif_orientation = 0
        # self.image_exif_transpose = False
        # self.image_exif_transpose_code = 0
        # self.image_exif_transpose_orientation = 0
        # self.image_exif_transpose_flip = False
        # self.image_exif_transpose_flip_code = 0
        try:
            self.get_image_details()
        except Exception as e:
            print("No entry in db for image.")
            self.initialize_image_details()

    def get_image_details(self):
        db = query_engine.QueryEngine()
        query = f"""
        SELECT ID, ITEM_NO, FILE_PATH, IMAGE_URL, WEB_ID, PRODUCT_ID, VARIANT_ID, THUMBNAIL, SORT_ORDER, DESCR
        FROM SN_IMAGES
        WHERE IMAGE_NAME = '{self.name}'
        """
        response = db.query_db(query)
        if response is not None:
            self.db_id = response[0][0]
            self.item_no = response[0][1]
            self.path = response[0][2]
            self.url = response[0][3]
            self.bc_image_id = response[0][4]
            self.product_id = response[0][5]
            self.variant_id = response[0][6]
            self.is_thumbnail = True if response[0][7] == 1 else False
            self.sort_order = response[0][8]
            self.description = response[0][9]
            self.in_database = True

    def initialize_image_details(self):
        # Path
        self.path = f"{creds.photo_path}/{self.name}"
        try:
            self.size = os.path.getsize(self.path)
        except FileNotFoundError:
            print(f"File {self.name} not found.")
            return
        else:
            # Dates
            self.modified_date = datetime.fromtimestamp(os.path.getmtime(self.path))
            self.created_date = datetime.fromtimestamp(os.path.getctime(self.path))

            # Sort Order
            if "^" not in self.name.split(".")[0]:
                self.sort_order = 1
            else:
                self.sort_order = int(self.name.split(".")[0].split("^")[1]) + 1

            # response = self.bc_post_image(self.product_id, self.url, self.description, self.is_thumbnail)

    def resize_image(self):
        # im = Image.open(self.path)
        # self.image_resolution = im.size
        # self.image_quality = im.info.get("quality", 0)
        # self.image_exif = im.getexif()
        # self.image_exif_orientation = self.image_exif.get(0x0112, 1)
        # self.image_orientation = 1 if self.image_exif_orientation in [3, 6, 8] else 0
        pass

    def bc_get_image(self):
        url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
               f'products/{self.product_id}/images/{self.bc_image_id}')
        response = requests.get(url=url, headers=creds.bc_api_headers)
        return response.content

    def bc_post_image(self, product_id, source_url, description="", is_thumbnail=False):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images'
        payload = {
            "product_id": self.product_id,
            "is_thumbnail": self.is_thumbnail,
            "sort_order": self.sort_order,
            "description": self.description,
            "image_url": self.url
        }
        response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
        return response.json()

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

    def bc_delete_product_image(self):
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
        # query = f"""
        # DELETE FROM EC_PHOTO
        # WHERE PHOTO_NAME = '{self.name}'
        # """
        # self.db.query_db(query, commit=True)
        # print(f"Photo {self.name} deleted from database.")
        pass


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


def update_product_timestamps(sku_list):
    """Takes in a list of SKUs and updates the last maintenance date for each product in the list"""
    tuple_list = tuple(sku_list)
    db = query_engine.QueryEngine()
    query = f"""
    UPDATE IM_ITEM
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


# ------------------------
# UTILITIES
# -----------------------
def create_image_table(table_name):
    db = query_engine.QueryEngine()
    query = f"""
        CREATE TABLE {table_name} (
        ID int NOT NULL PRIMARY KEY,
        ITEM_NO varchar(50) NOT NULL,
        IMAGE_NAME nvarchar(255) NOT NULL,
        FILE_PATH nvarchar(255) NOT NULL,
        IMAGE_URL nvarchar(255),
        WEB_ID int NOT NULL,
        PRODUCT_ID int NOT NULL,
        VARIANT_ID int,
        THUMBNAIL BIT NOT NULL,
        SORT_ORDER int NOT NULL,
        DESCR nvarchar(100),
        CREATION_DT datetime NOT NULL DEFAULT(current_timestamp),
        LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
        );
        """
    db.query_db(query, commit=True)


# ------------------------
# DRIVER
# -----------------------
def product_integration(last_run_date):
    # Step 1
    updated_photos = get_updated_photos(last_run_date)
    update_product_timestamps(updated_photos)

    # Step 2 - Update E-Commerce Category Tree

    # Step 3 - Get list of all modified e-commerce items
    e_commerce_items = get_updated_products(last_run_date)

    # Step 4 - Get list of valid binding keys associated with these items
    binding_ids = get_binding_ids_to_process(e_commerce_items)

    # Things are getting serious now...
    # Step 5 - Create BoundProduct objects for each binding key. This includes all child products and will validate
    # the inputs from Counterpoint. If the product passes input validation (through initial validation or through
    # repair and reattempt), update product details in database and BigCommerce or post product if it is new.

    for x in binding_ids:
        bound_product = BoundProduct(x)
        if bound_product.validate_product():
            bound_product.update_bound_product()
