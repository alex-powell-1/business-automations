from integration.catalog_api import Catalog
from integration.customers_api import Customers
from integration.promotions_api import Promotions
from datetime import datetime, timedelta

from database import Database
from integration import interface
from setup import creds

from setup import date_presets
from product_tools.sort_order import SortOrderEngine
from setup.error_handler import ProcessOutErrorHandler
from setup.utilities import get_last_sync, set_last_sync, timer

import sys
import time
from traceback import format_exc as tb


class Integrator:
    def __init__(self):
        self.dates = date_presets.Dates()
        self.last_sync: datetime = get_last_sync(file_name='./integration/last_sync_integrator.txt')
        self.start_sync_time: datetime = None
        self.completion_time: datetime = None
        self.module = str(sys.modules[__name__]).split('\\')[-1].split('.')[0].title()
        self.eh = ProcessOutErrorHandler
        self.error_handler = self.eh.error_handler
        self.logger = self.eh.logger
        self.customer_sync: bool = creds.Integrator.customer_sync
        self.promotions_sync: bool = creds.Integrator.promotion_sync
        self.catalog_sync: bool = creds.Integrator.catalog_sync
        self.sort_collections: bool = creds.Integrator.collection_sorting
        self.verbose: bool = creds.Integrator.verbose_logging
        self.customers = Customers(last_sync=self.last_sync, verbose=self.verbose, enabled=self.customer_sync)
        self.promotions = Promotions(last_sync=self.last_sync, verbose=self.verbose, enabled=self.promotions_sync)
        self.catalog = Catalog(
            dates=self.dates, last_sync=self.last_sync, verbose=self.verbose, enabled=self.catalog_sync
        )

    def __str__(self):
        result = creds.Integrator.title + '\n'
        result += f'Authors: {creds.Integrator.authors}\nVersion: {creds.Integrator.version}\n'
        result += f'Last Sync: {self.last_sync}\n----------------\n'
        sync_tasks = ''
        sync_tasks += self.customers.__str__()
        # sync_tasks += self.promotions.__str__()
        sync_tasks += self.catalog.__str__()
        if sync_tasks:
            result += sync_tasks
        else:
            result += 'No customers, promotions, collections, or products to sync.\n'
            if self.sort_collections:
                result += 'Collection Sorting: Enabled\n'
        return result

    def initialize(self, rebuild=False):
        """Initialize the integrator by deleting the catalog, rebuilding the tables, and syncing the catalog."""
        start_time = datetime.now()
        self.catalog.delete()  # Will delete all products, and collections  NEED TO TEST ON DEV STORE
        # self.customers.()
        if rebuild:
            Database.Shopify.rebuild_tables()  # Will drop and rebuild all Shopify tables

        self.catalog = Catalog(dates=self.dates, initial_sync=True)
        self.sync()
        # self.customers = Customers(last_sync=business_start)
        Integrator.logger.info(message=f'Initialization Complete. ' f'Total time: {time.time() - start_time}')

    @timer
    def sync(self):
        """Sync the catalog, customers, and promotions."""
        self.start_sync_time = datetime.now()
        self.logger.header(
            f'Starting sync at {self.start_sync_time:%Y-%m-%d %H:%M:%S}. Last sync: {integrator.last_sync}'
        )
        # Sync the catalog, customers, and promotions
        if self.customer_sync:
            self.customers.sync()
        if self.promotions_sync:
            self.promotions.sync()
        if self.catalog_sync:
            self.catalog.sync()
        if self.sort_collections:
            SortOrderEngine.sort(verbose=self.verbose)
        # Finished
        self.completion_time = datetime.now()
        integrator.logger.info(f'Sync complete at {self.completion_time:%Y-%m-%d %H:%M:%S}')

        set_last_sync(file_name='./integration/last_sync_integrator.txt', start_time=self.start_sync_time)

        if self.error_handler.errors:
            self.error_handler.print_errors()


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
            command = input('\nEnter command: \n' '- product\n' '- collections\n' '- customer\n\n')
            if command == 'product':
                sku = input('Enter product sku: ')
                payload = Catalog.get_product(item_no=sku)
                product = integrator.catalog.Product(product_data=payload, last_sync=integrator.last_sync)
                product.get()
                print(product)

            elif command == 'collections':
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

    if len(sys.argv) > 1:
        if sys.argv[1] == 'input':
            main_menu()
        else:
            if '-y' in sys.argv:  # Run the integrator without user input
                print(integrator)
                integrator.sync(eh=integrator.eh, operation=integrator.module)

            if '-v' in sys.argv:  # Set verbose logging
                integrator.verbose = True

            if '-l' in sys.argv:  # Run the integrator in a loop
                try:
                    while True:
                        now = datetime.now()
                        hour = now.hour
                        if 19 > hour > 4:  # Daytime - between 7am and 7pm
                            minutes_between_sync = creds.Integrator.int_day_run_interval
                        else:
                            minutes_between_sync = creds.Integrator.int_night_run_interval

                        delay = minutes_between_sync * 60

                        try:
                            integrator = Integrator()  # Reinitialize the integrator each time
                            integrator.sync(eh=integrator.eh, operation=integrator.module)

                        except Exception as e:
                            integrator.error_handler.add_error_v(
                                error=f'Error: {e}', origin=integrator.module, traceback=tb()
                            )
                            time.sleep(60)
                        else:
                            next_sync = integrator.completion_time + timedelta(minutes=minutes_between_sync)
                            time.sleep(delay)

                except KeyboardInterrupt:
                    sys.exit(0)

                except Exception as e:
                    integrator.error_handler.add_error_v(error=e, origin=integrator.module, traceback=tb())

    else:
        print(integrator)
        input = input('Continue? (y/n): ')
        if input.lower() == 'y':
            integrator.sync(eh=integrator.eh, operation=integrator.module)
        else:
            sys.exit(0)
