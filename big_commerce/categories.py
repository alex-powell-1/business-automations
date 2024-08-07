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
            self.children.append(
                BigCommerceCategory(
                    category_id=x['id'],
                    parent_id=x['parent_id'],
                    name=x['name'],
                    is_visible=x['is_visible'],
                    depth=x['depth'],
                    path=x['path'],
                    children=x['children'],
                    url=x['url'],
                )
            )


def get_category_trees():
    url = f'https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/catalog/trees'
    response = requests.get(url, headers=creds.test_bc_api_headers)
    return response.json()


def get_categories():
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/categories'
    response = requests.get(url, headers=creds.bc_api_headers)
    return response.json()


class BigCommerceCategoryTree:
    def __init__(self):
        self.categories = []
        self.get_categories()
        self.tree_id = 1

    def __str__(self):
        result = ''
        for category in self.categories:
            result += f'{category.id}: {category.name}\n'
            # for child in category.children:
            #     result += f"    {child.id}: {child.name}\n"
            #     for grandchild in child.children:
            #         result += f"        {grandchild.id}: {grandchild.name}\n"
            #         for great_grandchild in grandchild.children:
            #             result += f"            {great_grandchild.id}: {great_grandchild.name}\n"
        return result

    def get_categories(self):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/catalog/trees/1/categories'
        response = requests.get(url=url, headers=creds.bc_api_headers)
        for x in response.json()['data']:
            self.categories.append(
                BigCommerceCategory(
                    category_id=x['id'],
                    parent_id=x['parent_id'],
                    name=x['name'],
                    is_visible=x['is_visible'],
                    depth=x['depth'],
                    path=x['path'],
                    children=x['children'],
                    url=x['url'],
                )
            )
