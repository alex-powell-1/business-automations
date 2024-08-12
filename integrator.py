from integration.shopify_catalog import Catalog
from integration.shopify_customers import Customers
from integration.database import Database
from integration import interface

from setup import date_presets
from datetime import datetime

from setup.error_handler import ProcessOutErrorHandler
from setup.utilities import get_last_sync, set_last_sync

import sys
import time


class Integrator:
    error_handler = ProcessOutErrorHandler.error_handler
    logger = ProcessOutErrorHandler.logger

    def __init__(self):
        self.last_sync = get_last_sync(file_name='last_sync_integrator.txt')
        self.catalog = Catalog(last_sync=self.last_sync)
        # self.customers = Customers(last_sync=self.last_sync)

    def __str__(self):
        return f'Integrator\n' f'Last Sync: {self.last_sync}\n'

    def initialize(self, rebuild=False):
        """Initialize the integrator by deleting the catalog, rebuilding the tables, and syncing the catalog."""
        start_time = time.time()
        self.catalog.delete_catalog()
        # self.customers.delete_customers()
        if rebuild:
            # Drop and rebuild the tables
            Database.Shopify.rebuild_tables()

        self.catalog = Catalog(last_sync=date_presets.business_start_date)
        self.sync(initial=True)
        # self.customers = Customers(last_sync=business_start)
        Integrator.logger.info(message=f'Initialization Complete. ' f'Total time: {time.time() - start_time}')

    def sync(self, initial=False):
        start_sync_time = datetime.now()
        self.logger.header('Sync Starting')
        # self.customers.sync()  # Throwing error for email already taken. Need to fix.
        self.catalog.category_tree.sync()
        self.catalog.sync(initial=initial)
        set_last_sync(file_name='last_sync_integrator.txt', start_time=start_sync_time)
        completion_time = (datetime.now() - start_sync_time).seconds
        Integrator.logger.info(f'Sync completion time: {completion_time} seconds')
        if Integrator.error_handler.errors:
            Integrator.error_handler.print_errors()


def main_menu():
    print(interface.art)
    print(f'Version: {interface.Version}\n')
    print(
        'Please enter a command to execute: \n'
        '- initialize -- will delete all products, brands, categories, and customers from bigcommerce and start over.\n'
        '- get -- will get info about a product, brand, category, or customer)\n'
        '- tc -- will update timestamp for a product\n'
        '- delete -- will delete a product, brand, category, or customer from BC)\n'
        '- sync -- will run a normal sync\n'
    )

    try:
        input_command = input('Enter command: \n')
    except KeyboardInterrupt:
        sys.exit(0)
    else:
        # Initialize the integrator by deleting the catalog, rebuilding the tables, and syncing the catalog.
        if input_command == 'initialize':
            integrator.initialize()
        # Get information about a product, brand, category, or customer
        elif input_command == 'get':
            command = input('\nEnter command: \n' '- product\n' '- brands\n' '- categories\n' '- customer\n\n')
            if command == 'product':
                sku = input('Enter product sku: ')
                payload = Catalog.get_product(item_no=sku)
                product = integrator.catalog.Product(product_data=payload, last_sync=integrator.last_sync)
                product.get()
                print(product)

            elif command == 'brands':
                brands = integrator.catalog.Brands(last_sync=integrator.last_sync)
                print(brands)

            elif command == 'categories':
                tree = Catalog.CategoryTree(last_sync=integrator.last_sync)
                print(tree)

            # elif command == 'customer':
            #     customer_id = input('Enter customer id: ')
            #     integrator.customers.get_customer(customer_id=customer_id)

            user_menu_choices()

        # Update timestamp for a product
        elif input_command == 'tc':
            sku = input('Enter product sku: ')
            integrator.catalog.Product.update_timestamp(sku=sku)
            main_menu()

        elif input_command == 'sync':
            integrator.sync()

        elif input_command.startswith('delete'):
            command = (
                input(
                    '\nEnter command: \n' '- product\n' '- catalog\n' '- brands\n' '- categories\n' '- products\n\n'
                )
                .lower()
                .strip()
            )
            if command == 'product':
                sku = input('Enter product sku: ')
                print(f'Are you sure you want to delete product {sku}? (y/n)')
                choice = input('Enter choice: ')
                if choice.lower() == 'y':
                    integrator.catalog.Product.delete(sku=sku, update_timestamp=True)
                    main_menu()
                else:
                    print('Aborted.')
                    time.sleep(2)
                    main_menu()

            elif command == 'catalog':
                print(
                    'Are you sure you want to delete the product catalog?\nThis includes all products, brands, and categories(y/n)'
                )
                choice = input('Enter choice: ')
                if choice.lower() == 'y':
                    integrator.catalog.delete()
                    main_menu()
                else:
                    print('Aborted.')
                    time.sleep(2)
                    main_menu()

            elif command == 'categories':
                print('Are you sure you want to delete all categories? (y/n)')
                choice = input('Enter choice: ')
                if choice.lower() == 'y':
                    integrator.catalog.delete(categories=True)
                    main_menu()
                else:
                    print('Aborted.')
                    time.sleep(2)
                    main_menu()

            elif command == 'products':
                print('Are you sure you want to delete all products? (y/n)')
                choice = input('Enter choice: ')
                if choice.lower() == 'y':
                    integrator.catalog.delete(products=True)
                    main_menu()
                else:
                    print('Aborted.')
                    time.sleep(2)
                    main_menu()
        else:
            print('Invalid command.')


def user_menu_choices():
    choice = input('\n\nPress 1 to continue or any other key to exit.')
    if choice == '1':
        print('\n\n')
        main_menu()
    else:
        sys.exit(0)


if __name__ == '__main__':
    integrator = Integrator()
    # Argument parsing
    if len(sys.argv) > 1:
        if sys.argv[1] == 'initialize':
            integrator.initialize()
        elif sys.argv[1] == 'delete':
            if len(sys.argv) > 2:
                if sys.argv[2] == 'product':
                    if len(sys.argv) > 3:
                        integrator.catalog.delete_product(sku=sys.argv[3])
                    else:
                        print('Please provide a sku to delete.')

                elif sys.argv[2] == 'catalog':
                    integrator.catalog.delete_catalog()

                elif sys.argv[2] == 'brands':
                    integrator.catalog.delete_brands()

                elif sys.argv[2] == 'categories':
                    integrator.catalog.delete_categories()

                elif sys.argv[2] == 'products':
                    integrator.catalog.delete_products()
        # CLI interface
        elif sys.argv[1] == 'input':
            main_menu()

    else:
        integrator.sync()
