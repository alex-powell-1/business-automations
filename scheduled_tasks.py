from datetime import datetime
from customer_tools import tiered_pricing
from customer_tools import customers
from product_tools import brands
from product_tools import set_inactive_status
from product_tools import stock_buffer
from customer_tools import stock_notification
from customer_tools.merge import Merge
from setup.email_engine import Email
from setup import creds, date_presets, network, utilities
from sms import sms_automations, sms_messages, sms_queries
from setup import backups
from setup.error_handler import ScheduledTasksErrorHandler
from setup.sms_engine import SMSEngine
import time


# -----------------
# Scheduled Tasks
# -----------------
while True:
    dates = date_presets.Dates()  # obj passed into functions to create queries and sms messages

    now = datetime.now()
    day = now.day
    hour = now.hour
    minute = now.minute

    eh = ScheduledTasksErrorHandler
    error_handler = eh.error_handler
    logger = eh.logger

    try:
        #######################################################################################################
        ############################################### REPORTS ###############################################
        #######################################################################################################
        if minute == 0:
            # ADMINISTRATIVE REPORT - Generate report/email to administrative team list
            admin_report = creds.Reports.Administrative
            if admin_report.enabled and admin_report.hour == hour:
                try:
                    Email.Staff.AdminReport.send(recipients=admin_report.recipients, dates=dates)
                except Exception as err:
                    error_handler.add_error_v(error=err, origin='Administrative Report')

            # ITEMS REPORT EMAIL - for product management team
            item_report = creds.Reports.Item
            if item_report.enabled and item_report.hour == hour:
                try:
                    Email.Staff.ItemReport.send(recipients=item_report.recipients)
                except Exception as err:
                    error_handler.add_error_v(error=err, origin='Item Report')

            # LOW STOCK REPORT EMAIL - for product management team
            low_stock_report = creds.Reports.LowStock
            if low_stock_report.enabled and low_stock_report.hour == hour:
                try:
                    Email.Staff.LowStockReport.send(recipients=low_stock_report.recipients, dates=dates)
                except Exception as err:
                    error_handler.add_error_v(error=err, origin='Low Stock Report')

            ######################################################################################################
            ################################## MARKETING LEAD NOTIFCATION EMAIL ##################################
            ######################################################################################################

            # LANDSCAPE DESIGN LEAD NOTIFICATION EMAIL - Customer Followup Email to Sales Team
            design_leads = creds.Reports.MarketingLeads
            if design_leads.enabled and design_leads.hour == hour:
                try:
                    Email.Staff.DesignLeadNotification.send(recipients=design_leads.recipients)
                except Exception as err:
                    error_handler.add_error_v(error=err, origin='Lead Notification Email')

        #######################################################################################################
        ######################################### TWICE PER HOUR TASKS ########################################
        #######################################################################################################

        if minute == 0 or minute == 30:
            # NETWORK CONNECTIVITY
            # Check server for internet connection. Restart is there is no connection to internet.
            network.restart_server_if_disconnected()

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

        ###################################################################################################
        ###################################### EVERY-OTHER HOURLY TASKS #####################################
        ###################################################################################################

        # Between 6 AM and 8 PM - Performed on even hours
        if minute == 0 and 20 >= hour >= 6 and hour % 2 == 0:
            # ITEM STATUS CODES
            # Move active product_tools with zero stock into inactive status
            # unless they are on order, hold, quote
            try:
                set_inactive_status.set_products_to_inactive(eh=ScheduledTasksErrorHandler)
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

        ###################################################################################################
        ########################################### DAILY TASKS ###########################################
        ###################################################################################################

        if hour == 5 and minute == 0:  # 5 AM
            try:
                customers.fix_first_and_last_sale_dates(dt=dates)
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Fix First and Last Sale Dates')

        if hour == 10 and minute == 0:  # 10 AM
            test_text = f"""From Server: This is a test message. Today is {dates.today}.
            Yesterday was {dates.yesterday}. Tomorrow is {dates.tomorrow}."""
            SMSEngine.send_text(
                origin='SERVER',
                campaign='Alex Test',
                to_phone=creds.Company.network_notification_phone,
                message=test_text,
            )

        if hour == 11 and minute == 30:  # 11:30 AM
            # STOCK NOTIFICATION EMAIL WITH COUPON GENERATION
            # Read CSV file, check all items for stock, send auto generated emails to customer_tools
            # with product photo, product description (if exists), coupon (if applicable), and
            # direct purchase links. Generate coupon and send to big for e-comm use.
            try:
                stock_notification.send_stock_notifications()
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Stock Notification Email')

        if hour == 22 and minute == 30:  # 10:30 PM
            # Nightly Off-Site Backups
            # Will copy selected files to off-site location
            try:
                backups.offsite_backups()
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Offsite Backups')

            # MERGE CUSTOMERS
            # Merge duplicate customers by email or phone. Skips customers with open orders.
            try:
                merge = Merge(eh=eh)
            except Exception as err:
                error_handler.add_error_v(error=err, origin='Merge Customers')

            # Delete Old Log Files
            utilities.delete_old_files()

        ###################################################################################################
        ########################################### SMS AUTOMATIONS #######################################
        ###################################################################################################

        sms = creds.SMSAutomations
        origin = 'Automations'

        if not sms.enabled:
            logger.warn('SMS Automations are disabled.')
        else:
            messages = sms_messages.SMSMessages(dates)
            birthday_queries = sms_queries.BirthdayQueries(dates)
            wholesale_queries = sms_queries.WholesaleQueries(dates)
            ftc_queries = sms_queries.FTCQueries(dates)
            rc_queries = sms_queries.RCQueries(dates)

            #############################################################################################
            #################################### BIRTHDAY CUSTOMER AUTOMATIONS ##########################
            #############################################################################################

            birthday_1 = sms.Campaigns.Birthday
            if birthday_1.enabled:
                if day == birthday_1.day and hour == birthday_1.hour and minute == birthday_1.minute:
                    logger.info(f'SMS/MMS Automation: {birthday_1.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=birthday_1.title,
                            query=birthday_queries.text_1,
                            msg=messages.birthday.coupon_1,
                            image_url=creds.Coupon.birthday,
                            send_rwd_bal=False,
                        )

                    except Exception as err:
                        error_handler.add_error_v(error=err, origin=birthday_1.title)

            #############################################################################################
            ############################### Wholesale Customer Automations ##############################
            #############################################################################################

            wholesale_1 = sms.Campaigns.Wholesale1
            if wholesale_1.enabled:
                if hour == wholesale_1.hour and minute == wholesale_1.minute:
                    logger.info(f'SMS/MMS Automation: {wholesale_1.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=wholesale_1.title,
                            query=wholesale_queries.text_1,
                            msg=messages.wholesale.message_1,
                            msg_prefix=True,
                            send_rwd_bal=False,
                        )
                    except Exception as err:
                        error_handler.add_error_v(error=err, origin=wholesale_1.title)

            #############################################################################################
            ############################## First-Time Customer Automations ##############################
            #############################################################################################

            # FIRST-TIME CUSTOMER TEXT MESSAGE 1 - WELCOME (SMS)
            ftc1 = creds.SMSAutomations.Campaigns.FTC1
            if ftc1.enabled:
                if hour == ftc1.hour and minute == ftc1.minute:
                    logger.info(f'SMS/MMS Automation: {ftc1.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=ftc1.title,
                            query=ftc_queries.text_1,
                            msg=messages.ftc.ftc_1_body,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        error_handler.add_error_v(error=err, origin=ftc1.title)

            ftc2 = creds.SMSAutomations.Campaigns.FTC2
            if ftc2.enabled:
                if hour == ftc2.hour and minute == ftc2.minute:
                    # FIRST-TIME CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
                    logger.info(f'SMS/MMS Automation: {ftc2.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=ftc2.title,
                            query=ftc_queries.text_2,
                            msg=messages.ftc.ftc_2_body,
                            image_url=creds.Coupon.five_off,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        error_handler.add_error_v(error=err, origin=ftc2.title)

            # FIRST-TIME CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)

            ftc3 = creds.SMSAutomations.Campaigns.FTC3
            if ftc3.enabled:
                if hour == ftc3.hour and minute == ftc3.minute:
                    logger.info(f'SMS/MMS Automation: {ftc3.title} - {datetime.now():%H:%M:%S}')

                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=ftc3.title,
                            query=ftc_queries.text_3,
                            msg=messages.ftc.ftc_3_body,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        error_handler.add_error_v(error=err, origin=ftc3.title)

            #############################################################################################
            ############################## Returning Customer Automations ###############################
            #############################################################################################

            # RETURNING CUSTOMER TEXT MESSAGE 1 - THANK YOU (SMS)
            rc1 = sms.Campaigns.RC1
            if rc1.enabled:
                if hour == rc1.hour and minute == rc1.minute:
                    logger.info(f'SMS/MMS Automation: {rc1.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=rc1.title,
                            query=rc_queries.text_1,
                            msg=messages.rc.rc_1_body,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        error_handler.add_error_v(error=err, origin=rc1.title)

            # RETURNING CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
            rc2 = sms.Campaigns.RC2
            if rc2.enabled and hour == rc2.hour and minute == rc2.minute:
                logger.info(f'SMS/MMS Automation: {rc2.title} - {datetime.now():%H:%M:%S}')
                try:
                    sms_automations.create_customer_text(
                        origin=origin,
                        campaign=rc2.title,
                        query=rc_queries.text_2,
                        msg=messages.rc.rc_2_body,
                        image_url=creds.Coupon.five_off,
                        send_rwd_bal=True,
                    )
                except Exception as err:
                    error_handler.add_error_v(error=err, origin=rc2.title)

            # RETURNING CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)
            rc3 = sms.Campaigns.RC3
            if rc3.enabled and hour == rc3.hour and minute == rc3.minute:
                logger.info(f'SMS/MMS Automation: {rc3.title} - {datetime.now():%H:%M:%S}')
                try:
                    sms_automations.create_customer_text(
                        origin=origin,
                        campaign=rc3.title,
                        query=rc_queries.text_3,
                        msg=messages.rc.rc_3_body,
                        send_rwd_bal=True,
                    )
                except Exception as err:
                    error_handler.add_error_v(error=err, origin=rc3.title)

    except KeyboardInterrupt:
        logger.info(f'Process Terminated by User at {datetime.now():%H:%M:%S}')
    else:
        # logger.success(f'Business Automations Complete at {datetime.now():%H:%M:%S}')
        total_seconds = (datetime.now() - now).total_seconds()
        minutes = total_seconds // 60
        seconds = round(total_seconds % 60, 2)
        # logger.info(f'Total time of operation: {minutes} minutes {seconds} seconds')
    finally:
        time.sleep(60)
