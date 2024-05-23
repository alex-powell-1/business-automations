import os
from datetime import datetime

import requests
from PIL import Image

from setup import creds
from setup import query_engine

"""
Get list of all e-commerce items. 
Get list of binding keys. 

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
        self.html_description: str = ""
        self.search_keywords: str = ""
        self.meta_title: str = ""
        self.meta_description: str = ""
        self.visible: bool = False
        self.featured: bool = False
        self.gift_wrap: bool = False
        # Product Details
        self.search_keywords: str = ""
        self.preorder_message: str = ""
        self.meta_title: str = ""
        self.meta_description: str = ""
        self.alt_text_1: str = ""
        self.alt_text_2: str = ""
        self.alt_text_3: str = ""
        self.alt_text_4: str = ""
        # Custom Fields
        self.botanical_name: str = ""
        self.climate_zone: str = ""
        self.plant_type: str = ""
        self.type: str = ""
        self.height: str = ""
        self.width: str = ""
        self.sun_exposure: str = ""
        self.bloom_time: str = ""
        self.bloom_color: str = ""
        self.attracts_pollinators: str = ""
        self.attracts_butterflies: str = ""
        self.growth_rate: str = ""
        self.deer_resistant: str = ""
        self.soil_type: str = ""
        self.color: str = ""
        self.size: str = ""

        # Get Bound Product Family
        self.get_bound_product_family()

        # Validate Product
        self.validation_retries = 10
        self.validate_product()

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
            print(self.children)

        self.parent = [x.sku for x in self.children if x.is_parent]
        print(self.parent)

    def validate_product(self):
        while self.validation_retries > 0:
            print(f"Validating {self.binding_id}")
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
            # Test for missing variant names
            for child in self.children:
                if child.variant_name == "":
                    print(f"Product {child.sku} is missing a variant name. Validation failed.")
                    return False

            print(f"Product {self.binding_id} is valid.")
            return True

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
        self.always_online: bool = False
        self.gift_wrap: bool = False
        self.brand: str = ""
        self.featured: bool = False
        self.in_store_only: bool = False
        self.sort_order: int = 0
        self.binding_id: str = ""
        self.is_parent: bool = False
        self.web_title: str = ""
        self.variant_name: str = ""
        self.status: str = ""

        # Product Pricing
        self.reg_price: float = 0
        self.price_1: float = 0
        self.price_2: float = 0

        # Inventory Levels
        self.quantity_available: int = 0
        self.buffer: int = 0
        self.buffered_quantity: int = 0

        # Product Details
        self.item_type = ""
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
        self.botanical_name: str = ""
        self.climate_zone: str = ""
        self.plant_type: str = ""
        self.type: str = ""
        self.height: str = ""
        self.width: str = ""
        self.sun_exposure: str = ""
        self.bloom_time: str = ""
        self.bloom_color: str = ""
        self.attracts_pollinators: str = ""
        self.growth_rate: str = ""
        self.deer_resistant: str = ""
        self.soil_type: str = ""
        self.color: str = ""
        self.size: str = ""
        # Product Images
        self.images = []
        # Initialize Product Details
        self.get_product_details()

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
        USR_PROF_ALPHA_14, USR_PROF_ALPHA_15
        
        FROM IM_ITEM ITEM
        
        INNER JOIN IM_PRC PRC ON ITEM.ITEM_NO=PRC.ITEM_NO
        
        LEFT OUTER JOIN IM_INV INV ON ITEM.ITEM_NO=INV.ITEM_NO
        LEFT OUTER JOIN EC_ITEM_DESCR ON ITEM.ITEM_NO=EC_ITEM_DESCR.ITEM_NO
        LEFT OUTER JOIN EC_CATEG_ITEM ON ITEM.ITEM_NO=EC_CATEG_ITEM.ITEM_NO
        LEFT OUTER JOIN EC_CATEG ON EC_CATEG.CATEG_ID=EC_CATEG_ITEM.CATEG_ID
        
        WHERE ITEM.ITEM_NO = '{self.sku}' and ITEM.IS_ECOMM_ITEM = 'Y'
        """
        response = db.query_db(query)
        if response is not None:
            self.binding_id: str = response[0][0] if response[0][0] else ""

            # self.product_id: int = 0 This could be in separate table or would need to add columns to IM_ITEM
            # self.variant_id: int = 0 This could be in separate table or would need to add columns to IM_ITEM

            # Status
            self.web_enabled: bool = True if response[0][1] == 'Y' else False
            self.is_parent: bool = True if response[0][2] == 'Y' else False
            self.web_visible: bool = True if response[0][3] == 'Y' else False
            self.always_online: bool = True if response[0][4] == 'Y' else False
            self.gift_wrap: bool = True if response[0][5] == 'Y' else False
            self.brand: str = response[0][6] if response[0][6] else ""
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
            self.botanical_name: str = response[0][32] if response[0][32] else ""
            self.climate_zone: str = response[0][33] if response[0][33] else ""
            self.plant_type: str = response[0][34] if response[0][34] else ""
            self.type: str = response[0][35] if response[0][35] else ""
            self.height: str = response[0][36] if response[0][36] else ""
            self.width: str = response[0][37] if response[0][37] else ""
            self.sun_exposure: str = response[0][38] if response[0][38] else ""
            self.bloom_time: str = response[0][39] if response[0][39] else ""
            self.bloom_color: str = response[0][40] if response[0][40] else ""
            self.attracts_pollinators: str = response[0][41] if response[0][41] else ""
            self.growth_rate: str = response[0][42] if response[0][42] else ""
            self.deer_resistant: str = response[0][43] if response[0][43] else ""
            self.soil_type: str = response[0][44] if response[0][44] else ""
            self.color: str = response[0][45] if response[0][45] else ""
            self.size: str = response[0][46] if response[0][46] else ""

            # Product Images
            self.get_local_product_images()

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
                # Could use this to backfill database with image id and sort order info
                pass

class ProductImage:
    def __init__(self, image_name: str):
        self.name = image_name
        self.path = f"{creds.photo_path_prefix}{image_name}"
        self.type = image_name.split(".")[-1].lower()
        self.size = 0
        self.image_resolution = (0, 0)
        self.image_orientation = 0
        self.image_quality = 0
        self.image_exif = {}
        self.image_exif_orientation = 0
        self.modified_date = 0
        self.created_date = 0
        self.product_id = 0
        self.variant_id = 0
        self.bc_image_id = 0
        self.sort_order = 0
        # self.image_exif_transpose = False
        # self.image_exif_transpose_code = 0
        # self.image_exif_transpose_orientation = 0
        # self.image_exif_transpose_flip = False
        # self.image_exif_transpose_flip_code = 0
        self.initialize_image_details()

    def initialize_image_details(self):
        try:
            self.size = os.path.getsize(self.path)
        except FileNotFoundError:
            return
        else:
            im = Image.open(self.path)
            self.image_resolution = im.size
            self.image_quality = im.info.get("quality", 0)
            self.image_exif = im.getexif()
            self.image_exif_orientation = self.image_exif.get(0x0112, 1)

            # Dates
            self.modified_date = datetime.fromtimestamp(os.path.getmtime(self.path))
            self.created_date = datetime.fromtimestamp(os.path.getctime(self.path))

            # Sort Order
            if "^" not in self.name.split(".")[0]:
                self.sort_order = 1
            else:
                self.sort_order = int(self.name.split(".")[0].split("^")[1]) + 1

    def bc_get_image(self):
        url = (f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/'
               f'products/{self.product_id}/images/{self.bc_image_id}')
        response = requests.get(url=url, headers=creds.bc_api_headers)
        return response.content

    def bc_post_image(self, product_id, source_url, description="", is_thumbnail=False):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images'
        payload = {
            "product_id": product_id,
            "is_thumbnail": is_thumbnail,
            "sort_order": self.sort_order,
            "description": description,
            "image_url": source_url
        }
        response = requests.post(url=url, headers=creds.bc_api_headers, json=payload)
        return response.content

    def bc_update_product_image(self, product_id, image_id, source_url, description="", is_thumbnail=False):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/products/{product_id}/images/{image_id}'
        payload = {
            "product_id": product_id,
            "is_thumbnail": is_thumbnail,
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


bio_tone = Product("10037")
for image in bio_tone.images:
    for k, v in image.__dict__.items():
        print(f"{k}: {v}")
    print("\n")
