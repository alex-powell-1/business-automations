import datetime

from setup import creds
from setup import query_engine
import requests


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


def get_category_trees():
    url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/trees"
    response = requests.get(url, headers=creds.test_bc_api_headers)
    return response.json()


def get_categories():
    url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/categories"
    response = requests.get(url, headers=creds.bc_api_headers)
    return response.json()


class BigCommerceCategoryTree:
    def __init__(self):
        self.categories = []
        self.get_categories()
        self.tree_id = 1

    def __str__(self):
        result = ""
        for category in self.categories:
            result += f"{category.id}: {category.name}\n"
            # for child in category.children:
            #     result += f"    {child.id}: {child.name}\n"
            #     for grandchild in child.children:
            #         result += f"        {grandchild.id}: {grandchild.name}\n"
            #         for great_grandchild in grandchild.children:
            #             result += f"            {great_grandchild.id}: {great_grandchild.name}\n"
        return result

    # def create_tree(self):
    #     payload = [{
    #         "id": self.tree_id,
    #         "name": "Default catalog tree",
    #         "channels": [
    #             1
    #         ]}
    #     ]
    #     url = f' https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees'
    #     response = requests.put(url=url, headers=creds.bc_api_headers, json=creds.bc_api_headers)
    #     if response.status_code == 200:
    #         print("Category Tree Created Successfully.")
    #     else:
    #         print("Error Creating Category Tree.")
    #         print(response.json())

    def get_categories(self):
        url = f"https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees/1/categories"
        response = requests.get(url=url, headers=creds.bc_api_headers)
        for x in response.json()['data']:
            self.categories.append(BigCommerceCategory(category_id=x['id'],
                                                       parent_id=x['parent_id'],
                                                       name=x['name'],
                                                       is_visible=x['is_visible'], depth=x['depth'], path=x['path'],
                                                       children=x['children'], url=x['url']))


# tree = BigCommerceCategoryTree()
# for x in tree.categories:
#     for k,v in x.__dict__.items():
#         print(k,v)


def back_fill_middleware():
    db = query_engine.QueryEngine()
    query = f"""
    SELECT cp.CATEG_ID, ISNULL(cp.PARENT_ID, 0), cp.DESCR, cp.DISP_SEQ_NO, cp.HTML_DESCR, 
    cp.LST_MAINT_DT, sn.CP_CATEG_ID
    FROM EC_CATEG cp
    LEFT OUTER JOIN SN_CATEG sn on cp.CATEG_ID=sn.CP_CATEG_ID
    WHERE cp.CATEG_ID != '0'
    """
    response = db.query_db(query)
    if response:
        for x in response:
            lst_maint_dt = x[5]
            sn_cp_categ_id = x[6]
            if sn_cp_categ_id is None:
                # Insert new records
                cp_categ_id = x[0]
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
                db.query_db(query, commit=True)
            else:
                if lst_maint_dt > last_run_time:
                    # Update existing records
                    cp_categ_id = x[0]
                    cp_parent_id = x[1]
                    category_name = x[2]
                    sort_order = x[3]
                    description = x[4]
                    lst_maint_dt = x[5]
                    query = f"""
                    UPDATE SN_CATEG
                    SET CP_PARENT_ID = {cp_parent_id}, CATEG_NAME = '{category_name}',
                    SORT_ORDER = {sort_order}, DESCRIPTION = '{description}', LST_MAINT_DT = '{lst_maint_dt:%Y-%m-%d %H:%M:%S}'
                    WHERE CP_CATEG_ID = {sn_cp_categ_id}
                    """
                    print("Will update record")
                    db.query_db(query, commit=True)

