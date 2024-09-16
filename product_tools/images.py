import os
import time
from datetime import datetime

import csv_diff
import pandas
from setup import create_log
from setup import webDAV_engine
from setup import creds


def get_modified_datetime(file):
    """Get Modified timestamp as a datetime object"""
    modified_time = os.path.getmtime(file)
    convert_time = time.localtime(modified_time)
    format_time = time.strftime('%d%m%Y %H:%M:%S', convert_time)
    datetime_object = datetime.strptime(format_time, '%d%m%Y %H:%M:%S')
    return datetime_object


def get_modified_photos(reference_date):
    """Returns a list of photos modified since a specific reference date"""
    list_of_files = os.listdir(creds.product_images)
    list_of_sku = []
    for item in list_of_files[1:]:
        modified_date = get_modified_datetime(f'{creds.product_images}/{item}')
        if modified_date > reference_date:
            list_of_sku.append(item)
    return list_of_sku


# date_as_string = '2024-04-11 09:03:14'
# date = datetime.strptime(date_as_string, '%Y-%m-%d %H:%M:%S')
# get_modified_photos(date)

# print(webDAV_engine.upload_product_photo(file='../test.jpg', server_url=f'{creds.web_dav_product_photos}/'))


def render_photos_to_csv():
    list_of_files = os.listdir(creds.product_images)

    # PANDAS IS MUCH SLOWER FOR SOME REASON
    # for item in list_of_files[1:]:
    #     modified_date = get_modified_datetime(f"{creds.photo_path}/{item}")
    #     log_data = [[modified_date, item]]
    #     df = pandas.DataFrame(log_data, columns=["modified", "photo"])
    #     # Looks for file. If it has been deleted, it will recreate.
    #     create_log.write_log(df, creds.product_photo_log)

    import csv

    # id = 1
    fields = ['modified', 'photo']

    # name of csv file
    filename = creds.product_photo_log

    # writing to csv file
    with open(filename, 'w') as csvfile:
        # creating a csv dict writer object
        writer = csv.DictWriter(csvfile, fieldnames=fields)

        # writing headers (field names)
        writer.writeheader()
        for item in list_of_files[1:]:
            modified_date = get_modified_datetime(f'{creds.product_images}/{item}')
            mydict = {'modified': modified_date, 'photo': item}
            # writing data rows
            writer.writerow(mydict)
            # id += 1


# render_photos_to_csv()

from csv_diff import load_csv, compare

diff = compare(load_csv(open('../one.csv')), load_csv(open('../two.csv')))
# print(diff)
print('Added Photos')

for x in diff['added']:
    # This control structure should commence POST HTTP requests
    print(x)

print('Removed Photos')
for x in diff['removed']:
    # This control structure should commence DELETE HTTP requests
    print(x)
