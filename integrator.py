from integration.catalog_api import Catalog, Product, Collections
from integration.customers_api import Customers, Subscribers
from integration.promotions_api import Promotions
from datetime import datetime

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
    """Driver for integration between NCR Counterpoint and Shopify e-commerce platform"""

    eh = ProcessOutErrorHandler  # error handler
    module = str(sys.modules[__name__]).split('\\')[-1].split('.')[0].title()

    def __init__(self):
        self.dates = date_presets.Dates()
        self.verbose: bool = creds.Integrator.verbose_logging
        # Dates
        self.last_sync: datetime = get_last_sync(file_name='./integration/last_sync_integrator.txt')
        self.start_sync_time: datetime = None
        self.completion_time: datetime = None

        self.error_handler = Integrator.eh.error_handler
        self.logger = Integrator.eh.logger

        # Sync component booleans for enabling/disabling
        self.customer_sync: bool = creds.Integrator.customer_sync
        self.subscriber_sync: bool = creds.Integrator.subscriber_sync
        self.promotions_sync: bool = creds.Integrator.promotion_sync
        self.catalog_sync: bool = creds.Integrator.catalog_sync
        self.sort_collections: bool = creds.Integrator.collection_sorting

        # Sync components
        if self.customer_sync:
            self.customers: Customers = Customers(last_sync=self.last_sync, verbose=self.verbose)
        if self.subscriber_sync:
            self.subscribers: Subscribers = Subscribers(last_sync=self.last_sync, verbose=self.verbose)
        if self.promotions_sync:
            self.promotions: Promotions = Promotions(last_sync=self.last_sync, verbose=self.verbose)
        if self.catalog_sync:
            self.catalog: Catalog = Catalog(dates=self.dates, last_sync=self.last_sync, verbose=self.verbose)

    def __str__(self):
        result = creds.Integrator.title + '\n'
        result += f'Authors: {creds.Integrator.authors}\nVersion: {creds.Integrator.version}\n'
        result += f'Last Sync: {self.last_sync}\n----------------\n'

        sync_tasks = ''

        if self.customer_sync:
            sync_tasks += self.customers.__str__()
        if self.subscriber_sync:
            sync_tasks += self.subscribers.__str__()
        if self.promotions_sync:
            sync_tasks += self.promotions.__str__()
        if self.catalog_sync:
            sync_tasks += self.catalog.__str__()

        if sync_tasks:
            result += sync_tasks
        else:
            result += 'No customers, subscribers, promotions, collections, or products to sync.\n'
            if self.sort_collections:
                result += 'Collection Sorting: Enabled\n'
        return result

    @timer
    def sync(self):
        """Sync the catalog, customers, and promotions."""
        self.start_sync_time = datetime.now()
        self.logger.header(
            f'Starting sync at {self.start_sync_time:%Y-%m-%d %H:%M:%S}. Last sync: {integrator.last_sync}'
        )

        if self.customer_sync:
            self.customers.sync()
        if self.subscriber_sync:
            self.subscribers.sync()
        if self.promotions_sync:
            self.promotions.sync()
        if self.catalog_sync:
            self.catalog.sync()

        # Finished
        self.completion_time = datetime.now()
        integrator.logger.info(f'Sync complete at {self.completion_time:%Y-%m-%d %H:%M:%S}')

        set_last_sync(file_name='./integration/last_sync_integrator.txt', start_time=self.start_sync_time)

        if self.error_handler.errors:
            self.error_handler.print_errors()


def main_menu():
    """WIP: Menu System for CLI Interface"""
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
            # integrator.initialize()
            print('Initialization disabled.')

        # Get information about a product, brand, category, or customer
        elif input_command == 'get':
            command = input('\nEnter command: \n' '- product\n' '- collections\n' '- customer\n\n')
            if command == 'product':
                sku = input('Enter product sku: ')
                payload = Catalog.get_product(item_no=sku)
                product = Product(product_data=payload, last_sync=integrator.last_sync)
                product.get()
                print(product)

            elif command == 'collections':
                collections = Collections(last_sync=integrator.last_sync)
                print(collections)

            elif command == 'customer':
                customer_id = input('Enter customer id: ')
                integrator.customers.get_customer(customer_id=customer_id)

            user_menu_choices()

        # Update timestamp for a product
        elif input_command == 'tc':
            sku = input('Enter product sku: ')
            Database.CP.Product.update_timestamp(sku=sku)
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
                    Product.delete(sku=sku, update_timestamp=True)
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
                    pw = input('Enter password: ')
                    if pw == creds.Integrator.sms_sync_keyword:
                        integrator.catalog.delete()
                    else:
                        print('Incorrect password!')
                        time.sleep(1)
                    main_menu()
                else:
                    print('Aborted.')
                    time.sleep(2)
                    main_menu()

            elif command == 'categories':
                print('Are you sure you want to delete all categories? (y/n)')
                choice = input('Enter choice: ')
                if choice.lower() == 'y':
                    pw = input('Enter password: ')
                    if pw == creds.Integrator.sms_sync_keyword:
                        integrator.catalog.delete(categories=True)
                    else:
                        print('Incorrect password!')
                        time.sleep(1)
                    main_menu()
                else:
                    print('Aborted.')
                    time.sleep(2)
                    main_menu()

            elif command == 'products':
                print('Are you sure you want to delete all products? (y/n)')
                choice = input('Enter choice: ')
                if choice.lower() == 'y':
                    pw = input('Enter password: ')
                    if pw == creds.Integrator.sms_sync_keyword:
                        integrator.catalog.delete(products=True)
                    else:
                        print('Incorrect password!')
                        time.sleep(1)
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
            # main_menu()
            print('Input Mode: Disabled')

        else:
            if '-y' in sys.argv:  # Run the integrator without user input
                print(integrator)
                integrator.sync(eh=Integrator.eh, operation=Integrator.module)

            if '-v' in sys.argv:  # Set verbose logging
                integrator.verbose = True

            if '-l' in sys.argv:  # Run the integrator in a loop
                try:
                    while True:
                        now = datetime.now()
                        hour = now.hour
                        
                        if creds.Integrator.day_start <= hour <= creds.Integrator.day_end:
                            minutes_between_sync = creds.Integrator.int_day_run_interval
                        else:
                            minutes_between_sync = creds.Integrator.int_night_run_interval

                        delay = minutes_between_sync * 60
                        step = 12  # interations between collection sorts

                        for i in range(step):
                            try:
                                integrator = Integrator()  # Reinitialize the integrator each time
                                integrator.sync(eh=Integrator.eh, operation=Integrator.module)

                            except Exception as e:
                                integrator.error_handler.add_error_v(
                                    error=f'Sync Error: {e}', origin=integrator.module, traceback=tb()
                                )

                            else:
                                time.sleep(delay)

                        if integrator.sort_collections:
                            try:
                                SortOrderEngine.sort(verbose=integrator.verbose)
                            except Exception as e:
                                integrator.error_handler.add_error_v(
                                    error=f'Collection Sort Error: {e}', origin=integrator.module, traceback=tb()
                                )

                except KeyboardInterrupt:
                    sys.exit(0)

                except Exception as e:
                    integrator.error_handler.add_error_v(error=e, origin=integrator.module, traceback=tb())

    else:
        print(integrator)
        input = input('Continue? (y/n): ')
        if input.lower() == 'y':
            integrator.sync(eh=Integrator.eh, operation=Integrator.module)
        else:
            sys.exit(0)


