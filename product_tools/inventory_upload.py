from datetime import datetime

import pandas as pd

from setup import creds
from setup.query_engine import QueryEngine
from setup.webDAV_engine import WebDAVClient
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


def create_inventory_csv(retail=True):
    db = QueryEngine()
    if retail:
        # RETAIL AVAILABILITY
        query = """
        SELECT item.item_no, item.long_descr, item.PRC_1, ISNULL(inv.qty_avail, 0), item.categ_cod
        FROM im_item item 
        INNER JOIN im_inv inv on item.ITEM_NO=inv.item_no 
        WHERE inv.QTY_AVAIL >0 and item.stat='A' 
        ORDER BY long_descr
        """
    else:
        # WHOLESALE AVAILABILITY
        query = """
        SELECT item.item_no, item.long_descr, item.REG_PRC, ISNULL(inv.qty_avail, 0), item.categ_cod 
        FROM im_item item inner join im_inv inv on item.ITEM_NO=inv.item_no 
        WHERE inv.QTY_AVAIL >0 and item.stat='A' 
        AND CATEG_COD IN ('trees','annual','deciduous','edibles','evergreen','flowering', 
        'grasses','ground','perennial','supplies') 
        ORDER BY item.long_descr
        """
    response = db.query_db(query)
    if response is not None:
        item_list = []
        for x in response:
            item_number = x[0]
            item_descr = x[1]
            item_price = round(float(x[2]), 2)
            item_qty_avail = int(x[3])
            category = x[4]
            item_list.append([item_number, item_descr, item_price, item_qty_avail, category])

        if retail:
            df = pd.DataFrame(item_list)
            df.to_csv(creds.retail_inventory_csv, mode='w', header=False, index=False)
        else:
            # WHOLESALE
            df = pd.DataFrame(item_list)
            df.to_csv(creds.wholesale_inventory_csv, mode='w', header=False, index=False)
        error_handler.logger.info('CSV file created.')


def upload_inventory():
    """Uploads csv of inventory for retail and wholesale availability data tables"""
    # # Retail Segment
    error_handler.logger.info(f'Inventory upload starting at {datetime.now():%H:%M:%S}')
    error_handler.logger.info('Creating inventory csv for upload to Retail Availability')
    create_inventory_csv(retail=True)
    error_handler.logger.info('Retail Inventory Uploaded to WebDav Server')

    # Wholesale Segment
    error_handler.logger.info('Creating inventory csv for upload to Wholesale Availability')
    create_inventory_csv(retail=False)
    error_handler.logger.info(f'Inventory Upload: Finished at {datetime.now():%H:%M:%S}')


upload_inventory()
