from integration.catalog import Catalog
from integration.customers import Customers
from integration.database import Database

from setup import date_presets

import time


class Integrator:
    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.db = Database()
        self.log_file = open("test.txt", "a")

        self.category_tree = None
        self.brands = None
        self.catalog = Catalog(last_sync=last_sync)
        self.customers = None
        self.gift_certificates = None
        self.orders = None

    def __str__(self):
        return (
            f"Integration Object\n"
            f"Last Sync: {self.last_sync}\n"
            f"{self.catalog}\n"
            f"{self.category_tree}\n"
        )

    def initialize(self):
        business_start = date_presets.business_start_date
        start_time = time.time()
        self.db.rebuild_tables()
        self.category_tree = Catalog.CategoryTree(last_sync=business_start)
        self.brands = Catalog.Brands(last_sync=business_start)
        self.catalog = Catalog(last_sync=business_start)
        self.customers = Customers(last_sync=business_start)
        print(f"Initialization Complete. " f"Total time: {time.time() - start_time}")

    def sync(self):
        self.catalog.sync()
        # self.category_tree.build_bc_category_tree()
        # self.customers.sync()


integrator = Integrator(last_sync=date_presets.five_minutes_ago)
integrator.sync()
