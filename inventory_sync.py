from integration.shopify_catalog import Catalog
from product_tools import inventory_upload


from datetime import datetime

from setup.error_handler import ProcessOutErrorHandler
from traceback import format_exc as tb
from setup.utilities import get_last_sync, set_last_sync
from time import sleep


class Inventory:
    eh = ProcessOutErrorHandler
    logger = eh.logger
    error_handler = eh.error_handler

    def __init__(self, verbose=False):
        self.last_sync = get_last_sync(file_name='./integration/last_sync_inventory.txt')
        self.verbose = verbose
        self.catalog = Catalog(last_sync=self.last_sync, inventory_only=True, verbose=self.verbose)

    def __str__(self):
        return f'Integrator\n' f'Last Sync: {self.last_sync}\n'

    def sync(self, initial=False):
        start_sync_time = datetime.now()
        self.catalog.sync()
        set_last_sync(file_name='./integration/last_sync_inventory.txt', start_time=start_sync_time)
        completion_time = (datetime.now() - start_sync_time).seconds
        if self.verbose:
            Inventory.logger.info(f'Inventory Sync completion time: {completion_time} seconds')
        if Inventory.error_handler.errors:
            Inventory.error_handler.print_errors()


if __name__ == '__main__':
    verbose_logging = False

    while True:
        now = datetime.now()
        hour = now.hour
        if 18 > hour > 7:
            delay = 10
            step = 6
        else:
            delay = 300
            step = 10
        try:
            # Upload Inventory to file share.
            inventory_upload.upload_inventory(verbose=verbose_logging, eh=Inventory.eh)

            for i in range(step):
                inventory = Inventory(verbose=verbose_logging)
                inventory.sync()
                sleep(delay)

        except Exception as e:
            Inventory.error_handler.add_error_v(error=f'Error: {e}', origin='inventory_sync.py', traceback=tb())
            sleep(60)
