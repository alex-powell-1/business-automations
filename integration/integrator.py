from integration.catalog import Catalog
from integration.customers import Customers
from integration.database import Database
from integration import interface

from setup import date_presets, creds
from datetime import datetime

from integration.error_handler import GlobalErrorHandler

import sys
import time


class Integrator:
    error_handler = GlobalErrorHandler.error_handler
    logger = error_handler.logger

    def __init__(self):
        self.last_sync = self.get_last_sync()
        self.db = Database()

        self.catalog = Catalog(
            last_sync=self.last_sync,
        )

        self.customers = None
        self.gift_certificates = None
        self.orders = None

    def __str__(self):
        return f"Integrator\n" f"Last Sync: {self.last_sync}\n"

    def get_last_sync(self):
        """Read the last sync time from a file for use in sync operations."""
        with open("last_sync.txt", "r+") as file:
            last_sync = datetime.strptime(file.read(), "%Y-%m-%d %H:%M:%S")
            Integrator.logger.info(message=f"Last Sync: {last_sync}")
            return last_sync

    def set_last_sync(self, start_time):
        """Write the last sync time to a file for future use."""
        with open("last_sync.txt", "w") as file:
            file.write(start_time.strftime("%Y-%m-%d %H:%M:%S"))

    def initialize(self):
        """Initialize the integrator by deleting the catalog, rebuilding the tables, and syncing the catalog."""
        start_time = time.time()
        self.catalog.delete_catalog()
        self.customers.delete_customers()
        self.db.rebuild_tables()
        self.catalog = Catalog(
            last_sync=date_presets.business_start_date,
        )
        self.sync(initial=True)
        # self.customers = Customers(last_sync=business_start)
        Integrator.logger.info(
            message=f"Initialization Complete. "
            f"Total time: {time.time() - start_time}"
        )

    def sync(self, initial=False):
        start_sync_time = datetime.now()
        self.logger.header("Sync Starting")
        self.catalog.sync(initial=initial)
        self.set_last_sync(start_sync_time)
        completion_time = (datetime.now() - start_sync_time).seconds
        Integrator.logger.info(f"Sync completion time: {completion_time} seconds")
        Integrator.error_handler.print_errors()


def main_menu():
    print(
        "Please enter a command to execute: \n"
        "- initialize -- will delete all products, brands, categories, and customers from bigcommerce and start over.\n"
        "- get -- will get info about a product, brand, category, or customer)\n"
        "- delete -- will delete a product, brand, category, or customer from BC)\n"
        "- sync -- will run a normal sync\n"
    )

    try:
        input_command = input("Enter command: \n")
    except KeyboardInterrupt:
        sys.exit(0)
    else:
        # Initialize the integrator by deleting the catalog, rebuilding the tables, and syncing the catalog.
        if input_command == "initialize":
            integrator.initialize()
        # Get information about a product, brand, category, or customer
        elif input_command == "get":
            command = input(
                "\nEnter command: \n"
                "- product\n"
                "- brands\n"
                "- categories\n"
                "- customer\n\n"
            )
            if command == "product":
                sku = input("Enter product sku: ")
                payload = Catalog.get_product(item_no=sku)
                product = integrator.catalog.Product(
                    product_data=payload, last_sync=integrator.last_sync
                )
                product.get_product_details(last_sync=integrator.last_sync)
                print(product)

            elif command == "brands":
                brands = integrator.catalog.Brands(last_sync=integrator.last_sync)
                print(brands)

            elif command == "categories":
                tree = Catalog.CategoryTree(last_sync=integrator.last_sync)
                print(tree)

            elif command == "customer":
                customer_id = input("Enter customer id: ")
                integrator.customers.get_customer(customer_id=customer_id)

            user_menu_choices()

        elif input_command == "sync":
            integrator.sync()

        elif input_command.startswith("delete"):
            command = input_command.split(" ")
            if command[1] == "product":
                integrator.catalog.delete_product(sku=command[2])
            elif command[1] == "catalog":
                integrator.catalog.delete_catalog()
            elif command[1] == "brands":
                integrator.catalog.delete_brands()
            elif command[1] == "categories":
                integrator.catalog.delete_categories()
            elif command[1] == "products":
                integrator.catalog.delete_products()
        else:
            print("Invalid command.")


def user_menu_choices():
    choice = input("\n\nPress 1 to continue or any other key to exit.")
    if choice == "1":
        print("\n\n")
        main_menu()
    else:
        sys.exit(0)


if __name__ == "__main__":
    integrator = Integrator()
    # Argument parsing
    if len(sys.argv) > 1:
        if sys.argv[1] == "initialize":
            integrator.initialize()
        elif sys.argv[1] == "delete":
            if len(sys.argv) > 2:
                if sys.argv[2] == "product":
                    if len(sys.argv) > 3:
                        integrator.catalog.delete_product(sku=sys.argv[3])
                    else:
                        print("Please provide a sku to delete.")

                elif sys.argv[2] == "catalog":
                    integrator.catalog.delete_catalog()

                elif sys.argv[2] == "brands":
                    integrator.catalog.delete_brands()

                elif sys.argv[2] == "categories":
                    integrator.catalog.delete_categories()

                elif sys.argv[2] == "products":
                    integrator.catalog.delete_products()
        # CLI interface
        elif sys.argv[1] == "input":
            print(interface.art)
            print(f"Version: {interface.Version}\n")
            main_menu()

    else:
        integrator.sync()
