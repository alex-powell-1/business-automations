from datetime import datetime

import customer_tools.stop_sms
from customer_tools import stock_notification
from customer_tools import tiered_pricing
from customer_tools import customers
from product_tools import always_online
from product_tools import brands
from product_tools import featured
from product_tools import inventory_upload
from product_tools import related_items
from product_tools import resize_photos
from product_tools import set_inactive_status
from product_tools import sort_order
from product_tools import stock_buffer
from reporting import lead_generator_notification
from reporting import product_reports
from reporting import report_builder
from setup import creds
from setup import date_presets
from setup import network
from sms import sms_automations
from sms import sms_queries
from sms.sms_messages import (
    birthdays,
    first_time_customers,
    returning_customers,
    wholesale_sms_messages,
)
from utilities import backups

# -----------------
# Scheduled Tasks
# -----------------

now = datetime.now()
day = now.day
hour = now.hour
minute = now.minute

sms_test_mode = True  # if true, will only write generated messages write to logs
sms_test_customer = False  # if true, will only send to single employee for testing

errors = 0

log_file = open(creds.business_automation_log, "a")

print("-----------------------", file=log_file)
print(f"Business Automations Starting at {now:%H:%M:%S}", file=log_file)
print("-----------------------", file=log_file)

try:
    # ADMINISTRATIVE REPORT
    # Generate report in styled html/css and email to administrative team list
    if (
        creds.administrative_report["enabled"]
        and creds.administrative_report["hour"] == hour
    ):
        try:
            product_reports.administrative_report(
                recipients=creds.administrative_report["recipients"],
                log_file=log_file,
            )
        except Exception as err:
            errors += 1
            print("Error: Administrative Report", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

    # ITEMS REPORT EMAIL
    # For product management team
    if creds.item_report["enabled"] and creds.item_report["hour"] == hour:
        try:
            report_builder.item_report(
                recipients=creds.item_report["recipients"], log_file=log_file
            )
        except Exception as err:
            errors += 1
            print("Error: Administrative Report", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

    # LANDSCAPE DESIGN LEAD NOTIFICATION EMAIL
    # Customer Followup Email to Sales Team
    if creds.lead_email["enabled"] and creds.lead_email["hour"] == hour:
        try:
            lead_generator_notification.lead_notification_email(
                recipients=creds.lead_email["recipients"], log_file=log_file
            )
        except Exception as err:
            errors += 1
            print("Error: Lead Notification Email", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

    if minute == 0 or minute == 30:
        # # -----------------
        # # TWICE PER HOUR TASKS
        # # -----------------

        # # Checks health of Web App API. Notifies system administrator if not running via SMS text.
        try:
            network.health_check(log_file)
        except Exception as err:
            errors += 1
            print("Error: Network Health Check", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

        # Create new Counterpoint customer_tools from today's marketing leads
        try:
            lead_generator_notification.create_new_customers(log_file)
        except Exception as err:
            errors += 1
            print("Error: New Customer Creation", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

        # SET CONTACT 1
        # Concatenate First and Last name of non-business customer_tools and
        # fill contact 1 field in counterpoint (if null)
        try:
            customer_tools.customers.set_contact_1(log_file)
        except Exception as err:
            errors += 1
            print("Error: Contact 1", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

        # TIERED WHOLESALE PRICING LEVELS
        # Reassessing tiered pricing for all customers based on current year
        try:
            tiered_pricing.reassess_tiered_pricing(
                start_date=date_presets.year_start,
                end_date=date_presets.today,
                log_file=log_file,
                demote=False,
            )
        except Exception as err:
            errors += 1
            print("Error: Wholesale Tiered Pricing", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

    if minute == 0:
        # -----------------
        # EVERY HOUR TASKS
        # -----------------

        # NETWORK CONNECTIVITY
        # Check server for internet connection. Restart is there is no connection to internet.
        network.restart_server_if_disconnected(log_file)

        # UPLOAD CURRENT INVENTORY STOCK LEVELS TO WEBDAV SERVER
        try:
            inventory_upload.upload_inventory(log_file)
        except Exception as err:
            errors += 1
            print("Error: Inventory Upload", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

        # TIERED PRICING
        # Move wholesale customer_tools into pricing tiers based on
        # total sales over the last 6 months
        # tiered_pricing.update_tiered_pricing(date_presets.six_months_ago, date_presets.today)

        # PHOTO RESIZE/FORMATTING
        # Resizes large photos in the item images folder to a max resolution of 1280 by 1280 pixels
        # Re-formats .png to .jpg and .jpeg to .jpg while preserving aspect ratio and rotation data
        try:
            resize_photos.resize_photos(creds.photo_path, log_file, mode="big")
        except Exception as err:
            errors += 1
            print("Error: Photo Resizing/Reformatting", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

        # ----------------------
        # EVERY OTHER HOUR TASKS
        # ----------------------
        # Between 6 AM and 8 PM - Performed on even hours
        if 20 >= hour >= 6 and hour % 2 == 0:
            # ITEM STATUS CODES
            # Move active product_tools with zero stock into inactive status
            # unless they are on order, hold, quote
            try:
                set_inactive_status.set_products_to_inactive(log_file)
            except Exception as err:
                errors += 1
                print("Error: Item Status Codes", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

            # BRANDS
            # Set all items with no brand to the company brand
            # Set all products with specific keywords to correct e-commerce brand
            try:
                brands.update_brands(log_file)
            except Exception as err:
                errors += 1
                print("Error: Item Brands", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

            # ECOMMERCE FLAGS
            # Adds e-comm web enabled status and web visible to active product_tools with stock
            # Remove web-enabled status for single product_tools that haven't sold in two years
            # and are not 'Always Online'
            # Disabled on April 18th, 2024
            # ecomm_flags.set_ecommerce_flags()

            # STOCK BUFFER
            # Set stock buffers based on rules by vendor, category
            try:
                stock_buffer.stock_buffer_updates(log_file)
            except Exception as err:
                errors += 1
                print("Error: Stock Buffer Updates", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

            # Customer Export for Use in Constant Contact Campaigns
            try:
                customers.export_customers_to_csv(log_file)
            except Exception as err:
                errors += 1
                print("Error: Customer Export To CSV", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

        # -----------------
        # ONE PER DAY TASKS
        # -----------------

        # 2 AM TASKS
        if hour == 2:
            if datetime.today().weekday() == 6:
                # Only on Sundays
                # TOTAL SOLD
                # Update Big Commerce with "total_sold" for all ecommerce items. This lets customer_tools
                # Sort search results by "Best Sellers" with accurate information
                # Runs at 2AM and takes approx. 15 minutes
                try:
                    related_items.update_total_sold(log_file)
                except Exception as err:
                    errors += 1
                    print("Error: Update Total Sold", file=log_file)
                    print(err, file=log_file)
                    print("-----------------------\n", file=log_file)

                # RELATED ITEMS
                # Update Big Commerce with related items for each product.
                # Gives products popular amendments and products per category during
                # Same time last year
                try:
                    related_items.set_related_items_by_category(log_file)
                except Exception as err:
                    errors += 1
                    print("Error:Related Items", file=log_file)
                    print(err, file=log_file)
                    print("-----------------------\n", file=log_file)

        # 4 AM TASKS
        if hour == 4:
            # ALWAYS ONLINE
            # Set Always Online status for top performing items
            try:
                always_online.set_always_online(
                    log_file=log_file,
                    item_list=always_online.get_top_items(
                        start_date=date_presets.last_year_start,
                        end_date=date_presets.today,
                        number_of_items=200,
                    ),
                )
            except Exception as err:
                errors += 1
                print("Error: Always Online Status", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

            if datetime.today().weekday() == 6:
                # Only on Sundays
                # SORT ORDER BY PREDICTED REVENUE, RELEASE DATE, AND STOCK LEVEL
                # Update Sort Order for all product_tools at 4AM.
                # Uses revenue data from same period last year as a predictive method of rank importance.
                try:
                    sort_order.sort_order_engine(log_file)
                except Exception as err:
                    errors += 1
                    print("Error: Sort Order", file=log_file)
                    print(err, file=log_file)
                    print("-----------------------\n", file=log_file)

                # FEATURED PRODUCTSs
                # Update Featured Products at 4 AMs
                try:
                    featured.update_featured_items(log_file)
                except Exception as err:
                    errors += 1
                    print("Error: Featured Products", file=log_file)
                    print(err, file=log_file)
                    print("-----------------------\n", file=log_file)

        # 11:30 AM TASKS
    if hour == 11 and minute == 30:
        # STOCK NOTIFICATION EMAIL WITH COUPON GENERATION
        # Read CSV file, check all items for stock, send auto generated emails to customer_tools
        # with product photo, product description (if exists), coupon (if applicable), and
        # direct purchase links. Generate coupon and send to big for e-comm use.

        title = "Stock Notification Email"
        try:
            stock_notification.send_stock_notification_emails(log_file)
        except Exception as err:
            errors += 1
            print(f"Error: {title}", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

    if hour == 22 and minute == 30:
        # Nightly Off-Site Backups
        # Will copy critical files to off-site location
        try:
            backups.offsite_backups(log_file)
        except Exception as err:
            errors += 1
            print("Error: Off-Site Backup", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)

    #    __          __     _         _____  ___          _   __________  ___    __  __
    #   / _\  /\/\  / _\   /_\  /\ /\/__   \/___\/\/\    /_\ /__   \_   \/___\/\ \ \/ _\
    #   \ \  /    \ \ \   //_\\/ / \ \ / /\//  //    \  //_\\  / /\// /\//  //  \/ /\ \
    #   _\ \/ /\/\ \_\ \ /  _  \ \_/ // / / \_// /\/\ \/  _  \/ //\/ /_/ \_// /\  / _\ \
    #   \__/\/    \/\__/ \_/ \_/\___/ \/  \___/\/    \/\_/ \_/\/ \____/\___/\_\ \/  \__/

    if creds.birthday_text["enabled"]:
        if day == creds.birthday_text["day"]:
            if (
                hour == creds.birthday_text["hour"]
                and minute == creds.birthday_text["minute"]
            ):
                title = "Birthday Text"
                print(
                    f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                    file=log_file,
                )
                try:
                    sms_automations.create_customer_text(
                        query=sms_queries.birthday,
                        msg_descr=f"Birthday Text - {now.month} {now.year}",
                        msg=birthdays.birthday_coupon_1,
                        image_url=birthdays.BIRTHDAY_COUPON,
                        send_rwd_bal=False,
                        detail_log=creds.birthday_coupon_log,
                        general_log=log_file,
                        test_mode=creds.sms_automations["test_mode"],
                        test_customer=creds.sms_automations["test_customer"]["enabled"],
                    )
                except Exception as err:
                    errors += 1
                    print(f"Error: {title}", file=log_file)
                    print(err, file=log_file)
                    print("-----------------------\n", file=log_file)

    # WHOLESALE CUSTOMER TEXT MESSAGE 1 - RANDOM MESSAGE CHOICE (SMS)
    if creds.wholesale_1_text["enabled"]:
        if (
            hour == creds.wholesale_1_text["hour"]
            and minute == creds.wholesale_1_text["minute"]
        ):
            title = "Wholesale Text 1"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )
            try:
                sms_automations.create_customer_text(
                    query=sms_queries.wholesale_1,
                    msg_descr=wholesale_sms_messages.message_1_descr,
                    msg=wholesale_sms_messages.message_1,
                    msg_prefix=True,
                    send_rwd_bal=False,
                    detail_log=creds.wholesale_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

    # FIRST-TIME CUSTOMER TEXT MESSAGE 1 - WELCOME (SMS)
    if creds.ftc_1_text["enabled"]:
        if hour == creds.ftc_1_text["hour"] and minute == creds.ftc_1_text["minute"]:
            title = "First Time Cust Text 1"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )
            try:
                sms_automations.create_customer_text(
                    query=sms_queries.ftc_text_1,
                    msg_descr=first_time_customers.ftc_1_descr,
                    msg=first_time_customers.ftc_1_body,
                    send_rwd_bal=True,
                    detail_log=creds.first_time_customer_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

    if creds.ftc_2_text["enabled"]:
        if hour == creds.ftc_2_text["hour"] and minute == creds.ftc_2_text["minute"]:
            # FIRST-TIME CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
            title = "First Time Cust Text 2"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )
            try:
                sms_automations.create_customer_text(
                    query=sms_queries.ftc_text_2,
                    msg_descr=first_time_customers.ftc_2_descr,
                    msg=first_time_customers.ftc_2_body,
                    image_url=creds.five_off_coupon,
                    send_rwd_bal=True,
                    detail_log=creds.first_time_customer_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

    # FIRST-TIME CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)

    if creds.ftc_3_text["enabled"]:
        if hour == creds.ftc_3_text["hour"] and minute == creds.ftc_3_text["minute"]:
            title = "First Time Cust Text 3"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )

            try:
                sms_automations.create_customer_text(
                    query=sms_queries.ftc_text_3,
                    msg_descr=first_time_customers.ftc_3_descr,
                    msg=first_time_customers.ftc_3_body,
                    send_rwd_bal=True,
                    detail_log=creds.first_time_customer_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

    # RETURNING CUSTOMER TEXT MESSAGE 1 - THANK YOU (SMS)
    if creds.rc_1_text["enabled"]:
        if hour == creds.rc_1_text["hour"] and minute == creds.rc_1_text["minute"]:
            title = "Returning Cust Text 1"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )
            try:
                sms_automations.create_customer_text(
                    query=sms_queries.rc_1,
                    msg_descr=returning_customers.rc_1_descr,
                    msg=returning_customers.rc_1_body,
                    send_rwd_bal=True,
                    detail_log=creds.returning_customer_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

    # RETURNING CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
    if creds.rc_2_text["enabled"]:
        if hour == creds.rc_2_text["hour"] and minute == creds.rc_2_text["minute"]:
            title = "Returning Cust Text 2"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )
            try:
                sms_automations.create_customer_text(
                    query=sms_queries.rc_2,
                    msg_descr=returning_customers.rc_2_descr,
                    msg=returning_customers.rc_2_body,
                    image_url=creds.five_off_coupon,
                    send_rwd_bal=True,
                    detail_log=creds.returning_customer_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

    # RETURNING CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)
    if creds.rc_3_text["enabled"]:
        if hour == creds.rc_3_text["hour"] and minute == creds.rc_3_text["minute"]:
            title = "Returning Cust Text 3"
            print(
                f"SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}",
                file=log_file,
            )
            try:
                sms_automations.create_customer_text(
                    query=sms_queries.rc_3,
                    msg_descr=returning_customers.rc_3_descr,
                    msg=returning_customers.rc_3_body,
                    send_rwd_bal=True,
                    detail_log=creds.returning_customer_log,
                    general_log=log_file,
                    test_mode=creds.sms_automations["test_mode"],
                    test_customer=creds.sms_automations["test_customer"]["enabled"],
                )
            except Exception as err:
                errors += 1
                print(f"Error: {title}", file=log_file)
                print(err, file=log_file)
                print("-----------------------\n", file=log_file)

except KeyboardInterrupt:
    print("-----------------------", file=log_file)
    print(f"Process Terminated by User at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
else:
    print("-----------------------", file=log_file)
    print(f"Business Automations Complete at {datetime.now():%H:%M:%S}", file=log_file)
    total_seconds = (datetime.now() - now).total_seconds()
    minutes = total_seconds // 60
    seconds = round(total_seconds % 60, 2)
    print(
        f"Total time of operation: {minutes} minutes {seconds} seconds", file=log_file
    )
finally:
    print(f"Total Errors: {errors}", file=log_file)
    print("-----------------------\n\n\n", file=log_file)
    log_file.close()