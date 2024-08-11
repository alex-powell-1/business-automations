from integration.shopify_catalog import Catalog
from product_tools import inventory_upload
from setup import creds


from datetime import datetime

from setup.error_handler import Logger, ErrorHandler
from setup.utilities import get_last_sync, set_last_sync
from time import sleep


class Inventory:
    logger = Logger(f"{creds.log_main}/integration/process_out/log_{datetime.now().strftime("%m_%d_%y")}.log")
    error_handler = ErrorHandler(logger)

    def __init__(self):
        self.last_sync = get_last_sync(file_name='last_sync_inventory.txt')
        self.verbose = False
        self.catalog = Catalog(last_sync=self.last_sync, inventory_only=True, verbose=self.verbose)

    def __str__(self):
        return f'Integrator\n' f'Last Sync: {self.last_sync}\n'

    def sync(self, initial=False):
        start_sync_time = datetime.now()
        self.catalog.sync()
        set_last_sync(file_name='last_sync_inventory.txt', start_time=start_sync_time)
        # completion_time = (datetime.now() - start_sync_time).seconds
        # Inventory.logger.info(f'Inventory Sync completion time: {completion_time} seconds')
        if Inventory.error_handler.errors:
            Inventory.error_handler.print_errors()


if __name__ == '__main__':
    while True:
        try:
            now = datetime.now()
            hour = now.hour

            if 18 > hour > 7:
                delay = 10
                step = 6
            else:
                delay = 300
                step = 10

            # Upload Inventory to file share.
            inventory_upload.upload_inventory()

            for i in range(step):
                inventory = Inventory()
                inventory.sync()
                sleep(delay)

        except Exception as e:
            Inventory.error_handler.add_error_v(f'Error: {e}')
