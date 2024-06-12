from integration.catalog import Catalog
from integration.customers import Customers
from integration.database import Database

from setup import date_presets, creds
from datetime import datetime

import time


class Integrator:
    def __init__(self):
        last_sync = self.get_last_sync()
        self.last_sync = last_sync
        self.db = Database()
        self.log_file = open("test.txt", "a")

        self.category_tree = Catalog.CategoryTree(last_sync=last_sync)
        self.brands = Catalog.Brands(last_sync=last_sync)
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

    def get_last_sync(self):
        with open("last_sync.txt", "r+") as file:
            last_sync = datetime.strptime(file.read(), "%Y-%m-%d %H:%M:%S")
            print(f"Last Sync: {last_sync}")
            return last_sync

    def set_last_sync(self):
        with open("last_sync.txt", "w") as file:
            file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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
        self.category_tree.sync()
        self.brands.sync()
        self.catalog.sync()
        # self.customers.sync()
        # self.gift_certificates.sync()
        # self.orders.sync()
        self.set_last_sync()


start_sync_time = time.time()
integrator = Integrator()
integrator.sync()
print(f"Sync Complete. Total time: {time.time() - start_sync_time}")
