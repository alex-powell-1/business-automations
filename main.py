from setup import creds
from setup import date_presets
from datetime import datetime
from customers import tiered_pricing
from product_tools import set_inactive_status
from product_tools import ecomm_flags
from product_tools import always_online
from product_tools import resize_photos
from product_tools import sort_order
from product_tools import stock_buffer
from product_tools import brands
from product_tools import featured
from product_tools import related_items
from reporting import product_reports
from sms import sms_automations
from sms import sms_queries
from sms.sms_messages import birthdays, first_time_customers, returning_customers, wholesale_sms_messages
from product_tools import inventory_upload

# Business Automations
# Author: Alex Powell
# Date: February 6, 2024
# Description: A series of programmatic automations to serve business needs of retail and e-comm store.

now = datetime.now()
day = now.day
hour = now.hour
minute = now.minute

sms_test_mode = False  # if true, will only write generated messages write to logs
sms_test_customer = False  # if true, will only send to single employee for testing

# -----------------
# EVERY HOUR TASKS
# -----------------

if minute == 0:
    # UPLOAD CURRENT INVENTORY STOCK LEVELS TO WEBDAV SERVER
    inventory_upload.upload_inventory()

    # TIERED PRICING
    # Move wholesale customers into pricing tiers based on
    # total sales over the last 6 months
    tiered_pricing.update_tiered_pricing(date_presets.six_months_ago, date_presets.today)

    # PRODUCT STATUS CODES
    # Move active product_tools with zero stock into inactive status
    # unless they are on order, hold, quote
    set_inactive_status.set_products_to_inactive()

    # BRANDS
    # Set all items with no brand to the company brand
    # Set all products with specific keywords to correct e-commerce brand
    brands.update_brands()

    # ECOMMERCE FLAGS
    # Adds e-comm web enabled status and web visible to active product_tools with stock
    # Remove web-enabled status for single product_tools that haven't sold in two years
    # and are not 'Always Online'
    ecomm_flags.set_ecommerce_flags()

    # STOCK BUFFER
    # Set stock buffers based on rules by vendor, category
    stock_buffer.stock_buffer_updates()

    # PHOTO RESIZE/FORMATTING
    # Resizes large photos in the item images folder to a max resolution of 1280 by 1280 pixels
    # Re-formats .png to .jpg and .jpeg to .jpg while preserving aspect ratio and rotation data
    resize_photos.resize_photos(creds.photo_path, mode="big")

# -----------------
# ONE PER DAY TASKS
# -----------------

# 2 AM TASKS
if hour == 2:
    # BEST SELLERS
    # Update Big Commerce with "total_sold" for all ecommerce items. This lets customers
    # Sort search results by "Best Sellers" with accurate information
    # Runs at 2AM and takes approx. 15 minutes
    related_items.update_total_sold()

    # RELATED ITEMS
    # Update Big Commerce with related items for each product.
    # Gives products popular amendments and products per category during
    # Same time last year
    related_items.set_related_items_by_category()

# 4 AM TASKS
if hour == 4:
    # ALWAYS ONLINE
    # Set Always Online status for top performing items
    always_online.set_always_online(always_online.get_top_items(date_presets.last_year_start,
                                                                date_presets.today, number_of_items=200))

    # SORT ORDER BY PREDICTED REVENUE
    # Update Sort Order for all product_tools at 4AM.
    # Uses revenue data from same period last year as a predictive method of rank importance.
    sort_order.sort_order_engine()

    # FEATURED PRODUCTS
    # Update Featured Products at 4 AM
    featured.update_featured_items()

# 5 AM TASKS
if hour == 16:
    # ADMINISTRATIVE REPORT
    # Generate report in styled html/css and email to administrative team list
    product_reports.administrative_report(recipients=creds.alex_only)
    # REVENUE REPORT
    # sent to accounting department
    if datetime.today().isoweekday() == 7:  # only on Sunday
        product_reports.revenue_report(recipients=creds.flash_sales_recipients)

# 9 AM TASKS
if hour == 9:
    # BIRTHDAY MMS CUSTOMER COUPON ON FIRST DAY OF MONTH (MMS)
    if day == 1 and minute == 0:
        sms_automations.create_customer_text(query=sms_queries.birthday,
                                             msg_descr=f"Birthday Text - {now.month} {now.year}",
                                             msg=birthdays.birthday_coupon_1,
                                             image_url=birthdays.BIRTHDAY_COUPON,
                                             send_rwd_bal=False,
                                             log_location=creds.birthday_coupon_log,
                                             test_mode=sms_test_mode,
                                             test_customer=sms_test_customer)
# 10:30 AM TASKS
if hour == 10 and minute == 30:
    # WHOLESALE CUSTOMER TEXT MESSAGE 1 - RANDOM MESSAGE CHOICE (SMS)
    sms_automations.create_customer_text(query=sms_queries.wholesale_1,
                                         msg_descr=wholesale_sms_messages.message_1_descr,
                                         msg=wholesale_sms_messages.message_1,
                                         msg_prefix=True,
                                         send_rwd_bal=False,
                                         log_location=creds.wholesale_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)

# 11:30 AM TASKS
if hour == 11 and minute == 30:
    # FIRST-TIME CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)
    sms_automations.create_customer_text(query=sms_queries.ftc_text_3,
                                         msg_descr=first_time_customers.ftc_3_descr,
                                         msg=first_time_customers.ftc_3_body,
                                         send_rwd_bal=True,
                                         log_location=creds.first_time_customer_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)

    # RETURNING CUSTOMER TEXT MESSAGE 1 - THANK YOU (SMS)
    sms_automations.create_customer_text(query=sms_queries.rc_1,
                                         msg_descr=returning_customers.rc_1_descr,
                                         msg=returning_customers.rc_1_body,
                                         send_rwd_bal=True,
                                         log_location=creds.returning_customer_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)

# 3:30 PM TASKS
if hour == 15 and minute == 30:
    # RETURNING CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)
    sms_automations.create_customer_text(query=sms_queries.rc_3,
                                         msg_descr=returning_customers.rc_3_descr,
                                         msg=returning_customers.rc_3_body,
                                         send_rwd_bal=True,
                                         log_location=creds.returning_customer_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)

# 6:30 PM TASKS
if hour == 18 and minute == 30:
    # FIRST-TIME CUSTOMER TEXT 1 - WELCOME (SMS)
    sms_automations.create_customer_text(query=sms_queries.ftc_text_1,
                                         msg_descr=first_time_customers.ftc_1_descr,
                                         msg=first_time_customers.ftc_1_body,
                                         send_rwd_bal=True,
                                         log_location=creds.first_time_customer_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)

# 7:00 PM TASKS
if hour == 19:
    # FIRST_TIME CUSTOMER TEXT 2 - 5 OFF COUPON (MMS)
    sms_automations.create_customer_text(query=sms_queries.ftc_text_2,
                                         msg_descr=first_time_customers.ftc_2_descr,
                                         msg=first_time_customers.ftc_2_body,
                                         image_url=creds.five_off_coupon,
                                         send_rwd_bal=True,
                                         log_location=creds.first_time_customer_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)

    # RETURNING CUSTOMER TEXT 2 - 5 OFF COUPON (MMS)
    sms_automations.create_customer_text(query=sms_queries.rc_2,
                                         msg_descr=returning_customers.rc_2_descr,
                                         msg=returning_customers.rc_2_body,
                                         image_url=creds.five_off_coupon,
                                         send_rwd_bal=True,
                                         log_location=creds.returning_customer_log,
                                         test_mode=sms_test_mode,
                                         test_customer=sms_test_customer)
