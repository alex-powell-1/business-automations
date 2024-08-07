from datetime import datetime
from big_commerce import coupons
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
from product_tools import stock_buffer
from reporting import lead_generator_notification
from reporting import product_reports
from setup.email_engine import Email
from setup import creds
from setup import date_presets
from setup import network
from sms import sms_automations
from sms import sms_queries
from sms.sms_messages import birthdays, first_time_customers, returning_customers, wholesale_sms_messages
from setup import backups
from setup.error_handler import ScheduledTasksErrorHandler

# -----------------
# Scheduled Tasks
# -----------------

now = datetime.now()
day = now.day
hour = now.hour
minute = now.minute

sms_test_mode = False  # if true, will only write generated messages write to logs
sms_test_customer = False  # if true, will only send to single employee for testing

error_handler = ScheduledTasksErrorHandler.error_handler
logger = ScheduledTasksErrorHandler.logger

try:
    logger.info(f'Business Automations Starting at {now:%H:%M:%S}')

    # ADMINISTRATIVE REPORT - Generate report/email to administrative team list
    if creds.administrative_report['enabled'] and creds.administrative_report['hour'] == hour:
        try:
            product_reports.administrative_report(recipients=creds.administrative_report['recipients'])
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Administrative Report')

    # ITEMS REPORT EMAIL - for product management team
    if creds.item_report['enabled'] and creds.item_report['hour'] == hour:
        try:
            Email.ItemReport.send(recipients=creds.item_report['recipients'])
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Item Report')

    # LANDSCAPE DESIGN LEAD NOTIFICATION EMAIL - Customer Followup Email to Sales Team
    if creds.lead_email['enabled'] and creds.lead_email['hour'] == hour:
        try:
            lead_generator_notification.lead_notification_email(recipients=creds.lead_email['recipients'])
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Lead Notification Email')

    if minute == 0 or minute == 30:
        # # Checks health of Web App API. Notifies system administrator if not running via SMS text.
        try:
            network.health_check()
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Health Check')

        # SET CONTACT 1
        # Concatenate First and Last name of non-business customer_tools and
        # fill contact 1 field in counterpoint (if null)
        try:
            customers.set_contact_1()
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Set Contact 1')

        # TIERED WHOLESALE PRICING LEVELS
        # Reassessing tiered pricing for all customers based on current year
        try:
            tiered_pricing.reassess_tiered_pricing(
                start_date=date_presets.one_year_ago, end_date=date_presets.today, demote=False
            )
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Tiered Pricing')

    if minute == 16:
        # -----------------
        # EVERY HOUR TASKS
        # -----------------

        # NETWORK CONNECTIVITY
        # Check server for internet connection. Restart is there is no connection to internet.
        network.restart_server_if_disconnected()

        # ----------------------
        # EVERY OTHER HOUR TASKS
        # ----------------------
        # Between 6 AM and 8 PM - Performed on even hours
        if 20 >= hour >= 6 and hour % 2 == 0:
            # ITEM STATUS CODES
            # Move active product_tools with zero stock into inactive status
            # unless they are on order, hold, quote
            try:
                set_inactive_status.set_products_to_inactive()
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Inactive Status')

            # BRANDS
            # Set all items with no brand to the company brand
            # Set all products with specific keywords to correct e-commerce brand
            try:
                brands.update_brands()
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Brands')

            # STOCK BUFFER
            # Set stock buffers based on rules by vendor, category
            try:
                stock_buffer.stock_buffer_updates()
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Stock Buffer')

            # Customer Export for Use in Constant Contact Campaigns
            try:
                customers.export_customers_to_csv()
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Export Customers')

        # -----------------
        # ONE PER DAY TASKS
        # -----------------

    #     # 11:30 AM TASKS
    # if hour == 11 and minute == 30:
    #     # STOCK NOTIFICATION EMAIL WITH COUPON GENERATION
    #     # Read CSV file, check all items for stock, send auto generated emails to customer_tools
    #     # with product photo, product description (if exists), coupon (if applicable), and
    #     # direct purchase links. Generate coupon and send to big for e-comm use.
    #     try:
    #         stock_notification.send_stock_notification_emails()
    #     except Exception as err:
    #         error_handler.add_error_v(error=err, origin='Stock Notification Email')

    if hour == 22 and minute == 30:
        # Nightly Off-Site Backups
        # Will copy critical files to off-site location
        try:
            backups.offsite_backups()
        except Exception as err:
            error_handler.add_error_v(error=err, origin='Offsite Backups')

    #    __          __     _         _____  ___          _   __________  ___    __  __
    #   / _\  /\/\  / _\   /_\  /\ /\/__   \/___\/\/\    /_\ /__   \_   \/___\/\ \ \/ _\
    #   \ \  /    \ \ \   //_\\/ / \ \ / /\//  //    \  //_\\  / /\// /\//  //  \/ /\ \
    #   _\ \/ /\/\ \_\ \ /  _  \ \_/ // / / \_// /\/\ \/  _  \/ //\/ /_/ \_// /\  / _\ \
    #   \__/\/    \/\__/ \_/ \_/\___/ \/  \___/\/    \/\_/ \_/\/ \____/\___/\_\ \/  \__/

    if creds.birthday_text['enabled']:
        if day == creds.birthday_text['day']:
            if hour == creds.birthday_text['hour'] and minute == creds.birthday_text['minute']:
                title = 'Birthday Text'
                logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
                try:
                    sms_automations.create_customer_text(
                        origin='Automations',
                        campaign=title,
                        query=sms_queries.birthday,
                        msg=birthdays.birthday_coupon_1,
                        image_url=birthdays.BIRTHDAY_COUPON,
                        send_rwd_bal=False,
                        test_mode=creds.sms_automations['test_mode'],
                        test_customer=creds.sms_automations['test_customer']['enabled'],
                    )

                except Exception as err:
                    error_handler.add_error_v(error=err, origin='Birthday Text')

    # WHOLESALE CUSTOMER TEXT MESSAGE 1 - RANDOM MESSAGE CHOICE (SMS)
    if creds.wholesale_1_text['enabled']:
        if hour == creds.wholesale_1_text['hour'] and minute == creds.wholesale_1_text['minute']:
            title = 'Wholesale Text 1'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.wholesale_1,
                    msg=wholesale_sms_messages.message_1,
                    msg_prefix=True,
                    send_rwd_bal=False,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Wholesale Text 1')

    # FIRST-TIME CUSTOMER TEXT MESSAGE 1 - WELCOME (SMS)
    if creds.ftc_1_text['enabled']:
        if hour == creds.ftc_1_text['hour'] and minute == creds.ftc_1_text['minute']:
            title = 'First Time Cust Text 1'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.ftc_text_1,
                    msg=first_time_customers.ftc_1_body,
                    send_rwd_bal=True,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='First Time Cust Text 1')

    if creds.ftc_2_text['enabled']:
        if hour == creds.ftc_2_text['hour'] and minute == creds.ftc_2_text['minute']:
            # FIRST-TIME CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
            title = 'First Time Cust Text 2'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.ftc_text_2,
                    msg=first_time_customers.ftc_2_body,
                    image_url=creds.five_off_coupon,
                    send_rwd_bal=True,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='First Time Cust Text 2')

    # FIRST-TIME CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)

    if creds.ftc_3_text['enabled']:
        if hour == creds.ftc_3_text['hour'] and minute == creds.ftc_3_text['minute']:
            title = 'First Time Cust Text 3'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')

            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.ftc_text_3,
                    msg=first_time_customers.ftc_3_body,
                    send_rwd_bal=True,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='First Time Cust Text 3')

    # RETURNING CUSTOMER TEXT MESSAGE 1 - THANK YOU (SMS)
    if creds.rc_1_text['enabled']:
        if hour == creds.rc_1_text['hour'] and minute == creds.rc_1_text['minute']:
            title = 'Returning Cust Text 1'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.rc_1,
                    msg=returning_customers.rc_1_body,
                    send_rwd_bal=True,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Returning Cust Text 1')

    # RETURNING CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
    if creds.rc_2_text['enabled']:
        if hour == creds.rc_2_text['hour'] and minute == creds.rc_2_text['minute']:
            title = 'Returning Cust Text 2'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.rc_2,
                    msg=returning_customers.rc_2_body,
                    image_url=creds.five_off_coupon,
                    send_rwd_bal=True,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Returning Cust Text 2')

    # RETURNING CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)
    if creds.rc_3_text['enabled']:
        if hour == creds.rc_3_text['hour'] and minute == creds.rc_3_text['minute']:
            title = 'Returning Cust Text 3'
            logger.info(f'SMS/MMS Automation: {title} - {datetime.now():%H:%M:%S}')
            try:
                sms_automations.create_customer_text(
                    origin='Automations',
                    campaign=title,
                    query=sms_queries.rc_3,
                    msg=returning_customers.rc_3_body,
                    send_rwd_bal=True,
                    test_mode=creds.sms_automations['test_mode'],
                    test_customer=creds.sms_automations['test_customer']['enabled'],
                )
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Returning Cust Text 3')

except KeyboardInterrupt:
    logger.info(f'Process Terminated by User at {datetime.now():%H:%M:%S}')
else:
    logger.success(f'Business Automations Complete at {datetime.now():%H:%M:%S}')
    total_seconds = (datetime.now() - now).total_seconds()
    minutes = total_seconds // 60
    seconds = round(total_seconds % 60, 2)
    logger.info(f'Total time of operation: {minutes} minutes {seconds} seconds')
