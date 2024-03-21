import pyodbc

import customers.stop_sms
from setup import creds
from setup import date_presets
from datetime import datetime
# from customers import tiered_pricing, stock_notification
# from product_tools import set_inactive_status
# from product_tools import ecomm_flags
# from product_tools import always_online
# from product_tools import resize_photos
# from product_tools import sort_order
# from product_tools import stock_buffer
# from product_tools import brands
# from product_tools import featured
# from product_tools import related_items
from reporting.product_reports import administrative_report, get_missing_image_list, revenue_report, get_all_ecommerce_items, get_missing_item_descriptions
# from sms import sms_automations
# from sms import sms_queries
# from sms.sms_messages import birthdays, first_time_customers, returning_customers, wholesale_sms_messages
# from product_tools import inventory_upload

# # Business Automations
# # Author: Alex Powell
# # Date: February 6, 2024
# # Description: A series of programmatic automations to serve business needs of retail and e-comm store.

now = datetime.now()
day = now.day
hour = now.hour
minute = now.minute

sms_test_mode = False  # if true, will only write generated messages write to logs
sms_test_customer = False  # if true, will only send to single employee for testing

# administrative_report(recipients=creds.alex_only)

# customers.stock_notification.send_stock_notification_emails()

# set_inactive_status.set_products_to_inactive()

# from customers import stock_notification
# stock_notification.send_stock_notification_emails()
# print(get_missing_image_list())
from analysis.web_scraping import scrape_competitor_prices
scrape_competitor_prices()