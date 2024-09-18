from datetime import datetime

import pandas as pd
import csv
from setup import creds
from database import Database as db
from setup.error_handler import ScheduledTasksErrorHandler


def create_inventory_csv(retail=True, eh=ScheduledTasksErrorHandler):
    if retail:
        filename = 'CurrentAvailability.csv'
        # RETAIL AVAILABILITY
        query = """
        SELECT item.item_no, item.long_descr, item.PRC_1, ISNULL(inv.qty_avail, 0), item.categ_cod
        FROM im_item item 
        INNER JOIN im_inv inv on item.ITEM_NO=inv.item_no 
        WHERE inv.QTY_AVAIL >0 and item.stat='A' 
        ORDER BY long_descr
        """
    else:
        filename = 'CommercialAvailability.csv'
        # WHOLESALE AVAILABILITY
        query = """
        SELECT item.item_no, item.long_descr, item.REG_PRC, ISNULL(inv.qty_avail, 0), item.categ_cod 
        FROM im_item item inner join im_inv inv on item.ITEM_NO=inv.item_no 
        WHERE inv.QTY_AVAIL >0 and item.stat='A' 
        AND CATEG_COD IN ('trees','annual','deciduous','edibles','evergreen','flowering', 
        'grasses','ground','perennial','supplies') 
        ORDER BY item.long_descr
        """
    response = db.query(query)

    if response is not None:
        item_list = []
        for x in response:
            item_number = x[0]
            item_descr = x[1]
            item_price = round(float(x[2]), 2)
            item_qty_avail = int(x[3])
            category = x[4]
            item_list.append([item_number, item_descr, item_price, item_qty_avail, category])

        df = pd.DataFrame(item_list)

        directory = creds.Company.public_files_local_path + '/availability'
        file_location = directory + '/' + filename

        df.to_csv(
            file_location, mode='w', header=False, index=False, quoting=csv.QUOTE_NONE
        )  # quoting fixes double quotes issue


def upload_inventory(verbose=True, eh=ScheduledTasksErrorHandler):
    """Uploads csv of inventory for retail and wholesale availability data tables"""
    if verbose:
        eh.logger.info(f'Inventory upload starting at {datetime.now():%H:%M:%S}')
    create_inventory_csv(retail=True)
    create_inventory_csv(retail=False)
    if verbose:
        eh.logger.info(f'Inventory Upload: Finished at {datetime.now():%H:%M:%S}')


if __name__ == '__main__':
    upload_inventory()
