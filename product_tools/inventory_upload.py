from setup import creds
from setup.query_engine import QueryEngine
import pandas as pd
from setup.webDAV_engine import upload_file
from datetime import datetime


def create_inventory_csv(retail=True):
    db = QueryEngine()
    if retail:
        # RETAIL AVAILABILITY
        query = """
        SELECT item.item_no, item.long_descr, item.PRC_1, ISNULL(inv.qty_avail, 0) 
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
            if not retail:
                category = x[4]
                item_list.append([item_number, item_descr, item_price, item_qty_avail, category])
            else:
                item_list.append([item_number, item_descr, item_price, item_qty_avail])

        if retail:
            df = pd.DataFrame(item_list)
            df.to_csv(creds.retail_inventory_csv, mode='w', header=False, index=False)
        else:
            # WHOLESALE
            df = pd.DataFrame(item_list)
            df.to_csv(creds.wholesale_inventory_csv, mode='w', header=False, index=False)


def upload_inventory():
    """Uploads csv of inventory for retail and wholesale availability data tables"""
    print("-------------")
    print("Inventory Upload")
    print("-------------")
    print(f"Inventory upload starting at {datetime.now()}")
    print("Creating inventory csv for upload to Retail Availability")
    create_inventory_csv(retail=True)
    print("Uploading to WebDav Server")
    upload_file(file=creds.retail_inventory_csv, server_url=creds.web_dav_server)
    print(f"Retail Inventory Uploaded to WebDav Server")

    print("Creating inventory csv for upload to Wholesale Availability")
    create_inventory_csv(retail=False)
    print("Uploading to WebDav Server")
    upload_file(file=creds.wholesale_inventory_csv, server_url=creds.web_dav_server)
    print(f"Wholesale Inventory Uploaded to WebDav Server")

    print(f"Inventory upload complete at {datetime.now()}\n")
