from integration.catalog import Catalog
from integration.customers import Customers
from integration.database import Database

from setup import date_presets, creds
from datetime import datetime

from integration.error_handler import GlobalErrorHandler


import time


class Integrator:
    error_handler = GlobalErrorHandler.error_handler
    logger = error_handler.logger

    def __init__(self):
        last_sync = self.get_last_sync()
        self.last_sync = last_sync
        self.db = Database()
        self.category_tree = Catalog.CategoryTree(last_sync=last_sync)
        self.brands = Catalog.Brands(last_sync=last_sync)
        self.catalog = Catalog(
            last_sync=last_sync,
        )

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
            Integrator.logger.info(message=f"Last Sync: {last_sync}")
            return last_sync

    def set_last_sync(self, start_time):
        with open("last_sync.txt", "w") as file:
            file.write(start_time.strftime("%Y-%m-%d %H:%M:%S"))

    def initialize(self):
        business_start = date_presets.business_start_date
        start_time = time.time()
        self.db.rebuild_tables()
        self.category_tree = Catalog.CategoryTree(last_sync=business_start)
        self.brands = Catalog.Brands(last_sync=business_start)
        self.catalog = Catalog(last_sync=business_start)
        self.customers = Customers(last_sync=business_start)
        Integrator.logger.info(
            message=f"Initialization Complete. "
            f"Total time: {time.time() - start_time}"
        )

    def sync(self):
        start_sync_time = datetime.now()
        self.logger.header("Sync Starting")
        self.catalog.sync()
        self.set_last_sync(start_sync_time)
        completion_time = (datetime.now() - start_sync_time).seconds
        Integrator.logger.info(f"Sync completion time: {completion_time} seconds")
        Integrator.error_handler.print_errors()


if __name__ == "__main__":
    integrator = Integrator()
    integrator.sync()
