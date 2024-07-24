from setup import creds
from setup import query_engine
from setup.error_handler import ProcessOutErrorHandler


class Database:
    db = query_engine.QueryEngine()
    error_handler = ProcessOutErrorHandler.error_handler
    logger = ProcessOutErrorHandler.logger

    class Shopify:
        product_table = creds.shopify_product_table
        category_table = creds.shopify_category_table
        image_table = creds.shopify_image_table
        brands_table = creds.shopify_brands_table
        metafield_table = creds.shopify_metafield_table
        customer_table = creds.shopify_customer_table
        order_table = creds.shopify_order_table
        gift_cert_table = creds.shopify_gift_cert_table
        promo_table = creds.shopify_promo_table

        def rebuild_tables(self):
            def create_tables():
                tables = {
                    'categories': f"""
                                            CREATE TABLE {Database.Shopify.category_table} (
                                            CATEG_ID int IDENTITY(1,1) PRIMARY KEY,
                                            COLLECTION_ID bigint,
                                            MENU_ID bigint,
                                            CP_CATEG_ID bigint NOT NULL,
                                            CP_PARENT_ID bigint,
                                            CATEG_NAME nvarchar(255) NOT NULL,
                                            SORT_ORDER int,
                                            DESCRIPTION text,
                                            IS_VISIBLE BIT NOT NULL DEFAULT(1),
                                            IMG_SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'brands': f"""
                                            CREATE TABLE {Database.Shopify.brands_table} (
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
                                            IMAGE_SIZE int,
                                            IS_CUSTOMIZED BIT,
                                            CUSTOM_URL nvarchar(255),
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'products': f"""
                                            CREATE TABLE {Database.Shopify.product_table} (                                        
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO varchar(50) NOT NULL,
                                            BINDING_ID varchar(10),
                                            IS_PARENT BIT,
                                            PRODUCT_ID bigint NOT NULL,
                                            VARIANT_ID bigint,
                                            INVENTORY_ID bigint,
                                            VARIANT_NAME nvarchar(255),
                                            OPTION_ID bigint,
                                            OPTION_VALUE_ID bigint,
                                            CATEG_ID varchar(255),
                                            CF_BOTAN_NAM bigint, 
                                            CF_CLIM_ZON bigint, 
                                            CF_PLANT_TYP bigint, 
                                            CF_TYP bigint, 
                                            CF_HEIGHT bigint, 
                                            CF_WIDTH bigint, 
                                            CF_SUN_EXP bigint, 
                                            CF_BLOOM_TIM bigint,
                                            CF_FLOW_COL bigint,
                                            CF_POLLIN bigint, 
                                            CF_GROWTH_RT bigint, 
                                            CF_DEER_RES bigint, 
                                            CF_SOIL_TYP bigint,
                                            CF_COLOR bigint,
                                            CF_SIZE bigint,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'images': f"""
                                            CREATE TABLE {Database.Shopify.image_table} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            IMAGE_NAME nvarchar(255) NOT NULL,
                                            ITEM_NO varchar(50),
                                            FILE_PATH nvarchar(255) NOT NULL,
                                            IMAGE_URL nvarchar(255),
                                            PRODUCT_ID bigint,
                                            IMAGE_ID bigint,
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
                    'customers': f"""
                                            CREATE TABLE {Database.Shopify.customer_table} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            CUST_NO varchar(50) NOT NULL,
                                            BC_CUST_ID int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'orders': f"""
                                            CREATE TABLE {Database.Shopify.order_table} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            ORDER_NO int NOT NULL,
                                            DOC_ID bigint,
                                            STATUS bit DEFAULT(0)
                                            )""",
                    'gift': f"""
                                            CREATE TABLE {Database.Shopify.gift_cert_table} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            GFC_NO varchar(30) NOT NULL,
                                            BC_GFC_ID int NOT NULL,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            )""",
                    'promo': f""" 
                                            CREATE TABLE {Database.Shopify.promo_table}(
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            GRP_COD nvarchar(50) NOT NULL,
                                            RUL_SEQ_NO int,
                                            BC_ID int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );""",
                    'metafields': f""" 
                                            CREATE TABLE {Database.Shopify.metafield_table}(
                                            META_ID bigint NOT NULL, 
                                            NAME varchar(50) NOT NULL, 
                                            NAME_SPACE varchar(50), 
                                            META_KEY varchar(50), 
                                            TYPE varchar(50), 
                                            PIN bit, 
                                            OWNER_TYPE varchar(50),
                                            LST_MAINT_DT DATETIME DEFAULT(current_timestamp))""",
                    'qr': f"""
                                            CREATE TABLE {creds.qr_table} (
                                            QR_CODE varchar(100) NOT NULL PRIMARY KEY,
                                            URL varchar(100),
                                            PUBLICATION varchar(50),
                                            MEDIUM varchar(50),
                                            OFFER varchar(100),
                                            DESCR varchar(255),
                                            COUPON_CODE varchar(20),
                                            COUPON_USE int DEFAULT(0),
                                            VISIT_COUNT int DEFAULT(0),
                                            CREATE_DT datetime NOT NULL DEFAULT(current_timestamp),
                                            LST_SCAN datetime
                                            )""",
                    'qr_activ': f"""
                                            CREATE TABLE {creds.qr_activity_table} (
                                            SCAN_DT datetime NOT NULL DEFAULT(current_timestamp) PRIMARY KEY,
                                            CODE varchar(100) NOT NULL FOREIGN KEY REFERENCES SN_QR(QR_CODE),
                                            );""",
                }

                for table in tables:
                    Database.db.query_db(tables[table], commit=True)

            # Drop Tables
            def drop_tables():
                tables = [
                    creds.bc_customer_table,
                    creds.bc_image_table,
                    creds.bc_product_table,
                    creds.bc_brands_table,
                    creds.bc_category_table,
                    creds.bc_gift_cert_table,
                    creds.bc_order_table,
                ]

                def drop_table(table_name):
                    Database.db.query_db(f'DROP TABLE {table_name}', commit=True)

                for table in tables:
                    drop_table(table)

            # Recreate Tables
            drop_tables()
            create_tables()

        class Collection:
            def get_cp_categ_id(collection_id):
                query = f"""
                        SELECT CP_CATEG_ID FROM {creds.shopify_category_table}
                        WHERE COLLECTION_ID = {collection_id}
                        """
                response = Database.db.query_db(query)
                try:
                    return response[0][0]
                except:
                    return None

            def insert(category):
                query = f"""
                INSERT INTO {creds.shopify_category_table}(COLLECTION_ID, MENU_ID, CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, 
                SORT_ORDER, DESCRIPTION, IS_VISIBLE, IMG_SIZE, LST_MAINT_DT)
                VALUES({category.collection_id if category.collection_id else 'NULL'}, 
                {category.menu_id if category.menu_id else 'NULL'}, {category.cp_categ_id}, 
                {category.cp_parent_id}, '{category.name}', {category.sort_order}, 
                '{category.description.replace("'", "''")}', {1 if category.is_visible else 0}, 
                {category.image_size if category.image_size else 'NULL'},
                '{category.lst_maint_dt:%Y-%m-%d %H:%M:%S}')
                """
                response = Database.db.query_db(query, commit=True)
                if response['code'] == 200:
                    Database.logger.success(f'Category {category.name} added to Middleware.')
                else:
                    error = f'Error adding category {category.name} to Middleware. \nQuery: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(category):
                query = f"""
                UPDATE {creds.shopify_category_table}
                SET COLLECTION_ID = {category.collection_id if category.collection_id else 'NULL'}, 
                MENU_ID = {category.menu_id if category.menu_id else 'NULL'},
                CP_PARENT_ID = {category.cp_parent_id}, CATEG_NAME = '{category.name}',
                SORT_ORDER = {category.sort_order}, DESCRIPTION = '{category.description}', 
                IS_VISIBLE = {1 if category.is_visible else 0},
                IMG_SIZE = {category.image_size if category.image_size else 'NULL'},
                LST_MAINT_DT = '{category.lst_maint_dt:%Y-%m-%d %H:%M:%S}'
                WHERE CP_CATEG_ID = {category.cp_categ_id}
                """
                response = Database.db.query_db(query, commit=True)
                if response['code'] == 200:
                    Database.logger.success(f'Category {category.name} updated in Middleware.')
                else:
                    error = f'Error updating category {category.name} in Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def delete(collection_id=None, cp_categ_id=None):
                if collection_id is not None:
                    cp_categ_id = Database.Shopify.Collection.get_cp_categ_id(collection_id)

                if cp_categ_id is None:
                    raise Exception('No CP_CATEG_ID provided for deletion.')

                query = f"""
                        DELETE FROM {creds.shopify_category_table}
                        WHERE CP_CATEG_ID = {cp_categ_id}
                        """
                response = Database.db.query_db(query, commit=True)
                if response['code'] == 200:
                    Database.logger.success(f'Category {cp_categ_id} deleted from Middleware.')
                else:
                    error = f'Error deleting category {cp_categ_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def backfill_html_description(collection_id, description):
                cp_categ_id = Database.Shopify.Collection.get_cp_categ_id(collection_id)
                query = f"""
                        UPDATE EC_CATEG
                        SET HTML_DESCR = '{description}'
                        WHERE CATEG_ID = '{cp_categ_id}'
                        """
                response = Database.db.query_db(query, commit=True)
                if response['code'] != 200:
                    print(query)
                    raise Exception(response['message'])

        class Product:
            def get_id(item_no=None, binding_id=None, image_id=None):
                """Get product ID from SQL using image ID. If not found, return None."""
                if item_no:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Database.Shopify.product_table} WHERE ITEM_NO = '{item_no}'"
                    )
                if image_id:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {creds.shopify_image_table} WHERE IMAGE_ID = '{image_id}'"
                    )
                if binding_id:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Database.Shopify.product_table} WHERE BINDING_ID = '{binding_id}'"
                    )

                if item_no or image_id or binding_id:
                    prod_id_res = Database.db.query_db(product_query)
                    if prod_id_res is not None:
                        return prod_id_res[0][0]

            def sync(product):
                for variant in product.variants:
                    if variant.db_id is None:
                        Database.Shopify.Product.Variant.insert(product=product, variant=variant)
                    else:
                        Database.Shopify.Product.Variant.update(product=product, variant=variant)

                for image in product.images:
                    if image.db_id is None:
                        Database.Shopify.Product.Image.insert(image)
                    else:
                        Database.Shopify.Product.Image.update(image)

            def insert(product):
                for variant in product.variants:
                    Database.Shopify.Product.Variant.insert(product, variant)

            def delete(product_id):
                query = f'DELETE FROM {Database.Shopify.product_table} WHERE PRODUCT_ID = {product_id}'
                response = Database.db.query_db(query, commit=True)

                if response['code'] == 200:
                    Database.logger.success(f'Product {product_id} deleted from Middleware.')
                else:
                    error = f'Error deleting product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            class Variant:
                def get_variant_id(sku):
                    query = f"""
                        SELECT VARIANT_ID FROM {creds.shopify_product_table}
                        WHERE ITEM_NO = {sku}
                        """
                    response = Database.db.query_db(query)
                    if response is not None:
                        return response[0][0]

                def get_option_id(sku):
                    query = f"""
                        SELECT OPTION_ID FROM {creds.shopify_product_table}
                        WHERE ITEM_NO = {sku}
                        """
                    response = Database.db.query_db(query)
                    if response is not None:
                        return response[0][0]

                def get_option_value_id(sku):
                    query = f"""
                        SELECT OPTION_VALUE_ID FROM {creds.shopify_product_table}
                        WHERE ITEM_NO = {sku}
                        """
                    response = Database.db.query_db(query)
                    if response is not None:
                        return response[0][0]

                def insert(product, variant):
                    if product.shopify_collections:
                        categories_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        categories_string = None

                    insert_query = f"""
                        INSERT INTO {Database.Shopify.product_table} (ITEM_NO, BINDING_ID, IS_PARENT, 
                        PRODUCT_ID, VARIANT_ID, INVENTORY_ID, VARIANT_NAME, OPTION_ID, OPTION_VALUE_ID, CATEG_ID,
                        CF_BOTAN_NAM, CF_CLIM_ZON, CF_PLANT_TYP, CF_TYP, CF_HEIGHT, CF_WIDTH, CF_SUN_EXP, CF_BLOOM_TIM,
                        CF_FLOW_COL, CF_POLLIN, CF_GROWTH_RT, CF_DEER_RES, CF_SOIL_TYP, CF_COLOR, CF_SIZE)
                         
                        VALUES ('{variant.sku}', {f"'{product.binding_id}'" if product.binding_id else 'NULL'}, 
                        {1 if variant.is_parent else 0}, {product.product_id if product.product_id else "NULL"}, 
                        {variant.variant_id if variant.variant_id else "NULL"}, 
                        {variant.inventory_id if variant.inventory_id else "NULL"}, 
                        {f"'{variant.variant_name}'" if variant.variant_name else "NULL"}, 
                        {variant.option_id if variant.option_id else "NULL"}, 
                        {variant.option_value_id if variant.option_value_id else "NULL"}, 
                        {f"'{categories_string}'" if categories_string else "NULL"}, 
                        {variant.botanical_name_id if variant.botanical_name_id else "NULL"},
                        {variant.climate_zone_id if variant.climate_zone_id else "NULL"},
                        {variant.plant_type_id if variant.plant_type_id else "NULL"},
                        {variant.type_id if variant.type_id else "NULL"},
                        {variant.height_id if variant.height_id else "NULL"},
                        {variant.width_id if variant.width_id else "NULL"},
                        {variant.sun_exposure_id if variant.sun_exposure_id else "NULL"},
                        {variant.bloom_time_id if variant.bloom_time_id else "NULL"},
                        {variant.flower_color_id if variant.flower_color_id else "NULL"},
                        {variant.pollinator_id if variant.pollinator_id else "NULL"},
                        {variant.growth_rate_id if variant.growth_rate_id else "NULL"},
                        {variant.deer_resistant_id if variant.deer_resistant_id else "NULL"},
                        {variant.soil_type_id if variant.soil_type_id else "NULL"},
                        {variant.color_id if variant.color_id else "NULL"},
                        {variant.size_id if variant.size_id else "NULL"}
                        )
                        """

                    response = product.db.query_db(insert_query, commit=True)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - INSERT Variant {product.sku}'
                        )
                    else:
                        error = f'Query: {insert_query}\n\nResponse: {response}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.insert(SKU: {variant.sku})'
                        )
                        raise Exception(error)

                def update(product, variant):
                    if product.shopify_collections:
                        categories_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        categories_string = None

                    update_query = f"""
                        UPDATE {Database.Shopify.product_table} 
                        SET ITEM_NO = '{variant.sku}', 
                        BINDING_ID = {f"'{product.binding_id}'" if product.binding_id else 'NULL'}, 
                        IS_PARENT = {1 if variant.is_parent else 0}, 
                        PRODUCT_ID = {product.product_id if product.product_id else 'NULL'}, 
                        VARIANT_ID = {variant.variant_id if variant.variant_id else 'NULL'}, 
                        INVENTORY_ID = {variant.inventory_id if variant.inventory_id else 'NULL'}, 
                        VARIANT_NAME = {f"'{variant.variant_name}'" if variant.variant_id else "NULL"}, 
                        OPTION_ID = {variant.option_id if variant.option_id else "NULL"}, 
                        OPTION_VALUE_ID = {variant.option_value_id if variant.option_value_id else "NULL"}, 
                        CATEG_ID = {f"'{categories_string}'" if categories_string else "NULL"}, 
                        CF_BOTAN_NAM = {variant.botanical_name_id if variant.botanical_name_id else "NULL"},
                        CF_CLIM_ZON = {variant.climate_zone_id if variant.climate_zone_id else "NULL"},
                        CF_PLANT_TYP = {variant.plant_type_id if variant.plant_type_id else "NULL"},
                        CF_TYP = {variant.type_id if variant.type_id else "NULL"},
                        CF_HEIGHT = {variant.height_id if variant.height_id else "NULL"},
                        CF_WIDTH = {variant.width_id if variant.width_id else "NULL"},
                        CF_SUN_EXP = {variant.sun_exposure_id if variant.sun_exposure_id else "NULL"},
                        CF_BLOOM_TIM = {variant.bloom_time_id if variant.bloom_time_id else "NULL"},
                        CF_FLOW_COL = {variant.flower_color_id if variant.flower_color_id else "NULL"},
                        CF_POLLIN = {variant.pollinator_id if variant.pollinator_id else "NULL"},
                        CF_GROWTH_RT = {variant.growth_rate_id if variant.growth_rate_id else "NULL"},
                        CF_DEER_RES = {variant.deer_resistant_id if variant.deer_resistant_id else "NULL"},
                        CF_SOIL_TYP = {variant.soil_type_id if variant.soil_type_id else "NULL"},
                        CF_COLOR = {variant.color_id if variant.color_id else "NULL"},
                        CF_SIZE = {variant.size_id if variant.size_id else "NULL"},
                        LST_MAINT_DT = GETDATE() 
                        WHERE ID = {variant.db_id}
                        """

                    response = Database.db.query_db(update_query, commit=True)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - UPDATE Variant'
                        )
                    else:
                        error = f'Query: {update_query}\n\nResponse: {response}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Variant.update(SKU: {variant.sku})'
                        )
                        raise Exception(error)

                def delete(variant_id):
                    query = f'DELETE FROM {Database.Shopify.product_table} WHERE VARIANT_ID = {variant_id}'
                    response = Database.db.query_db(query, commit=True)

                    if response['code'] == 200:
                        Database.logger.success(f'Variant {variant_id} deleted from Middleware.')
                    else:
                        error = f'Error deleting variant {variant_id} from Middleware. \n Query: {query}\nResponse: {response}'
                        Database.error_handler.add_error_v(error=error)
                        raise Exception(error)

            class Image:
                def get_image_id(file_name):
                    img_id_res = Database.db.query_db(
                        f"SELECT IMAGE_ID FROM {creds.shopify_image_table} WHERE IMAGE_NAME = '{file_name}'"
                    )
                    try:
                        return img_id_res[0][0]
                    except:
                        return None

                def insert(image):
                    img_insert = f"""
                    INSERT INTO {creds.shopify_image_table} (IMAGE_NAME, ITEM_NO, FILE_PATH,
                    PRODUCT_ID, IMAGE_ID, THUMBNAIL, IMAGE_NUMBER, SORT_ORDER,
                    IS_BINDING_IMAGE, BINDING_ID, IS_VARIANT_IMAGE, DESCR, SIZE)
                    VALUES (
                    '{image.image_name}', {f"'{image.sku}'" if image.sku != '' else 'NULL'},
                    '{image.file_path}', {image.product_id}, {image.image_id}, '{1 if image.is_thumbnail else 0}', 
                    '{image.image_number}', '{image.sort_order}', '{image.is_binding_image}',
                    {f"'{image.binding_id}'" if image.binding_id else 'NULL'}, '{image.is_variant_image}',
                    {f"'{image.description.replace("'", "''")}'" if image.description != '' else 'NULL'},
                    {image.size})"""
                    insert_img_response = Database.db.query_db(img_insert, commit=True)
                    if insert_img_response['code'] == 200:
                        Database.logger.success(f'SQL INSERT Image {image.image_name}: Success')
                    else:
                        error = f'Error inserting image {image.image_name} into Middleware. \nQuery: {img_insert}\nResponse: {insert_img_response}'
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Image.insert({image.image_name})'
                        )
                        raise Exception(error)

                def update(image):
                    q = f"""
                    UPDATE {creds.shopify_image_table}
                    SET IMAGE_NAME = '{image.image_name}', ITEM_NO = '{image.sku}', FILE_PATH = '{image.file_path}',
                    IMAGE_URL = '{image.image_url}', PRODUCT_ID = '{image.product_id}', IMAGE_ID = '{image.image_id}',
                    THUMBNAIL = '{1 if image.is_thumbnail else 0}', IMAGE_NUMBER = '{image.image_number}',
                    SORT_ORDER = '{image.sort_order}', IS_BINDING_IMAGE = '{image.is_binding_image}',
                    BINDING_ID = {f"'{image.binding_id}'" if image.binding_id else 'NULL'},
                    IS_VARIANT_IMAGE = '{image.is_variant_image}',
                    DESCR = {f"'{image.description.replace("'", "''")}'" if
                                image.description != '' else 'NULL'}, SIZE = '{image.size}'
                    WHERE ID = {image.db_id}"""

                    res = Database.db.query_db(q, commit=True)
                    if res['code'] == 200:
                        Database.logger.success(f'SQL UPDATE Image {image.image_name}: Success')
                    else:
                        error = (
                            f'Error updating image {image.image_name} in Middleware. \nQuery: {q}\nResponse: {res}'
                        )
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Image.update({image.image_name})'
                        )
                        raise Exception(error)

                def delete(image):
                    if image.image_id is None:
                        image.image_id = Database.Shopify.Product.Image.get_image_id(filename=image.image_name)
                    q = f'DELETE FROM {creds.shopify_image_table} WHERE IMAGE_ID = {image.image_id}'
                    res = Database.db.query_db(q, commit=True)
                    if res['code'] == 200:
                        Database.logger.success(f'SQL DELETE Image {image.image_name}: Success')
                    else:
                        error = (
                            f'Error deleting image {image.image_name} in Middleware. \nQuery: {q}\nResponse: {res}'
                        )
                        Database.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Image.delete({image.image_name})'
                        )
                        raise Exception(error)

        class Metafield_Definition:
            def get(definition_id):
                query = f"""
                        SELECT * FROM {creds.shopify_metafield_table}
                        WHERE META_ID = {definition_id}
                        """
                response = Database.db.query_db(query)
                if response is not None:
                    return {
                        'META_ID': f'gid://shopify/MetafieldDefinition/{response[0]}',
                        'NAME': response[1],
                        'NAME_SPACE': response[2],
                        'META_KEY': response[3],
                        'TYPE': response[4],
                        'PIN': response[5],
                        'OWNER_TYPE': response[6],
                        'LST_MAINT_DT': response[7],
                    }

            def get_all():
                query = f"""
                        SELECT * FROM {creds.shopify_metafield_table}
                        """
                response = Database.db.query_db(query)
                if response is not None:
                    result = {}
                    for row in response:
                        result[row[1]] = {
                            'META_ID': f'gid://shopify/MetafieldDefinition/{row[0]}',
                            'NAME': row[1],
                            'NAME_SPACE': row[2],
                            'META_KEY': row[3],
                            'TYPE': row[4],
                            'PIN': row[5],
                            'OWNER_TYPE': row[6],
                            'LST_MAINT_DT': row[7],
                        }

                    return result

            def insert(values):
                query = f"""
                        INSERT INTO {creds.shopify_metafield_table} (META_ID, NAME, NAME_SPACE, META_KEY, TYPE, PIN, OWNER_TYPE)
                        VALUES {values['META_ID'], values['NAME'], values['NAME_SPACE'], values['META_KEY'], values['TYPE'], values['PIN'], values['OWNER_TYPE']}
                        """
                response = Database.db.query_db(query, commit=True)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def update(values):
                query = f"""
                        UPDATE {creds.shopify_metafield_table}
                        SET NAME = {values['NAME']}, NAME_SPACE = {values['NAME_SPACE']}, META_KEY = {values['META_KEY']}, TYPE = {values['TYPE']}, PIN = {values['PIN']}, OWNER_TYPE = {values['OWNER_TYPE']}, LST_MAINT_DT = GETDATE()
                        WHERE META_ID = {values['META_ID']}
                        """
                response = Database.db.query_db(query, commit=True)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def delete(definition_id):
                query = f"""
                        DELETE FROM {creds.shopify_metafield_table}
                        WHERE META_ID = {definition_id}
                        """
                response = Database.db.query_db(query, commit=True)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def delete_all():
                query = f"""
                        DELETE FROM {creds.shopify_metafield_table}
                        """
                response = Database.db.query_db(query, commit=True)
                if response['code'] != 200:
                    raise Exception(response['message'])


if __name__ == '__main__':
    print(Database.Shopify.Collection.get_cp_categ_id(482197405990))
