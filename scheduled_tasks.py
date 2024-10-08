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
from traceback import format_exc as tb
from setup.utilities import timer
import sys


class ScheduledTasks:
    def __init__(self):
        self.module = str(sys.modules[__name__]).split('\\')[-1].split('.')[0].title()
        self.dates = date_presets.Dates()
        self.eh = ScheduledTasksErrorHandler
        self.error_handler = self.eh.error_handler
        self.logger = self.eh.logger
        self.verbose = False

    def generate_reports(self):
        """Generate and send reports to staff"""
        # ADMINISTRATIVE REPORT - for administrative team and management
        admin_report = creds.Reports.Administrative
        if admin_report.enabled:
            if admin_report.hour == self.dates.hour and admin_report.minute == self.dates.minute:
                try:
                    Email.Staff.AdminReport.send(recipients=admin_report.recipients)
                except Exception as err:
                    self.error_handler.add_error_v(error=err, origin='Administrative Report')

        # ITEMS REPORT - for product management team
        item_report = creds.Reports.Item
        if item_report.enabled:
            if item_report.hour == self.dates.hour and item_report.minute == self.dates.minute:
                try:
                    Email.Staff.ItemReport.send(recipients=item_report.recipients)
                except Exception as err:
                    self.error_handler.add_error_v(error=err, origin='Item Report')

        # LOW STOCK REPORT - for product management team
        low_stock_report = creds.Reports.LowStock
        if low_stock_report.enabled:
            if low_stock_report.hour == self.dates.hour and low_stock_report.minute == self.dates.minute:
                try:
                    Email.Staff.LowStockReport.send(recipients=low_stock_report.recipients)
                except Exception as err:
                    self.error_handler.add_error_v(error=err, origin='Low Stock Report')

            # MARKETING LEAD NOTIFICATION EMAIL - for Sales team
            design_leads = creds.Reports.MarketingLeads
            if design_leads.enabled:
                if design_leads.hour == self.dates.hour and design_leads.minute == self.dates.minute:
                    try:
                        Email.Staff.DesignLeadNotification.send(recipients=design_leads.recipients)
                    except Exception as err:
                        self.error_handler.add_error_v(error=err, origin='Lead Notification Email')

    def twice_per_hour_tasks(self):
        if self.dates.minute == 0 or self.dates.minute == 30:
            # NETWORK CONNECTIVITY - Check server for internet connection. Restart if not connected.
            network.restart_server_if_disconnected()

            # Checks health of Web App API. Notifies system administrator if not running via SMS text.
            try:
                network.health_check()
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Health Check')

            # SET CONTACT 1
            # Concatenate First and Last name of non-business customer_tools and
            # fill contact 1 field in counterpoint (if null)
            try:
                customers.set_contact_1()
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Set Contact 1')

            # TIERED WHOLESALE PRICING LEVELS
            # Reassessing tiered pricing for all customers based on current year
            try:
                tiered_pricing.reassess_tiered_pricing(
                    start_date=date_presets.one_year_ago, end_date=date_presets.today, demote=False
                )
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Tiered Pricing')

    def every_other_hourly_tasks(self):
        # Between 6 AM and 8 PM - Performed on even hours
        if self.dates.minute == 0 and 20 >= self.dates.hour >= 6 and self.dates.hour % 2 == 0:
            # ITEM STATUS CODES
            # Move active product_tools with zero stock into inactive status
            # unless they are on order, hold, quote
            try:
                set_inactive_status.set_products_to_inactive(eh=self.eh)
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Inactive Status')

            # BRANDS
            # Set all items with no brand to the company brand
            # Set all products with specific keywords to correct e-commerce brand
            try:
                brands.update_brands()
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Brands')

            # STOCK BUFFER
            # Set stock buffers based on rules by vendor, category
            try:
                stock_buffer.stock_buffer_updates()
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Stock Buffer')

    def once_per_day_tasks(self):
        if self.dates.hour == 5 and self.dates.minute == 0:  # 5 AM
            try:
                customers.fix_first_and_last_sale_dates(dt=self.dates)
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Fix First and Last Sale Dates')

        if self.dates.hour == 10 and self.dates.minute == 0:  # 10 AM
            test_text = f"""From Server: This is a test message. Today is {self.dates.today}.
            Yesterday was {self.dates.yesterday}. One week ago was {self.dates.one_week_ago}."""
            SMSEngine.send_text(
                origin='SERVER',
                campaign='Alex Test',
                to_phone=creds.Company.network_notification_phone,
                message=test_text,
            )

        if self.dates.hour == 11 and self.dates.minute == 30:  # 11:30 AM
            # STOCK NOTIFICATION EMAIL WITH COUPON GENERATION
            # Read CSV file, check all items for stock, send auto generated emails to customer_tools
            # with product photo, product description (if exists), coupon (if applicable), and
            # direct purchase links. Generate coupon and send to big for e-comm use.
            try:
                stock_notification.send_stock_notifications()
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Stock Notification Email')

        if self.dates.hour == 22 and self.dates.minute == 30:  # 10:30 PM
            # Nightly Off-Site Backups
            # Will copy selected files to off-site location
            try:
                backups.offsite_backups()
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Offsite Backups')

            # MERGE CUSTOMERS
            # Merge duplicate customers by email or phone. Skips customers with open orders.
            try:
                Merge(eh=self.eh)
            except Exception as err:
                self.error_handler.add_error_v(error=err, origin='Merge Customers')

            # Delete Old Log Files
            utilities.delete_old_files()

    def sms_automations(self):
        sms = creds.SMSAutomations
        origin = 'Automations'

        if not sms.enabled:
            self.logger.warn('SMS Automations are disabled.')
        else:
            messages = sms_messages.SMSMessages(self.dates)
            birthday_queries = sms_queries.BirthdayQueries(self.dates)
            wholesale_queries = sms_queries.WholesaleQueries(self.dates)
            ftc_queries = sms_queries.FTCQueries(self.dates)
            rc_queries = sms_queries.RCQueries(self.dates)

            #############################################################################################
            #################################### BIRTHDAY CUSTOMER AUTOMATIONS ##########################
            #############################################################################################

            birthday_1 = sms.Campaigns.Birthday
            if birthday_1.enabled:
                if self.dates.day == birthday_1.day:
                    if self.dates.hour == birthday_1.hour and self.dates.minute == birthday_1.minute:
                        self.logger.info(f'SMS/MMS Automation: {birthday_1.title} - {datetime.now():%H:%M:%S}')
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
                            self.error_handler.add_error_v(error=err, origin=birthday_1.title)

            #############################################################################################
            ############################### Wholesale Customer Automations ##############################
            #############################################################################################

            wholesale_1 = sms.Campaigns.Wholesale1
            if wholesale_1.enabled:
                if self.dates.hour == wholesale_1.hour and self.dates.minute == wholesale_1.minute:
                    self.logger.info(f'SMS/MMS Automation: {wholesale_1.title} - {datetime.now():%H:%M:%S}')
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
                        self.error_handler.add_error_v(error=err, origin=wholesale_1.title)

            #############################################################################################
            ############################## First-Time Customer Automations ##############################
            #############################################################################################

            # FIRST-TIME CUSTOMER TEXT MESSAGE 1 - WELCOME (SMS)
            ftc1 = creds.SMSAutomations.Campaigns.FTC1
            if ftc1.enabled:
                if self.dates.hour == ftc1.hour and self.dates.minute == ftc1.minute:
                    self.logger.info(f'SMS/MMS Automation: {ftc1.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=ftc1.title,
                            query=ftc_queries.text_1,
                            msg=messages.ftc.ftc_1_body,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        self.error_handler.add_error_v(error=err, origin=ftc1.title)

            ftc2 = creds.SMSAutomations.Campaigns.FTC2
            if ftc2.enabled:
                if self.dates.hour == ftc2.hour and self.dates.minute == ftc2.minute:
                    # FIRST-TIME CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
                    self.logger.info(f'SMS/MMS Automation: {ftc2.title} - {datetime.now():%H:%M:%S}')
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
                        self.error_handler.add_error_v(error=err, origin=ftc2.title)

            # FIRST-TIME CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)

            ftc3 = creds.SMSAutomations.Campaigns.FTC3
            if ftc3.enabled:
                if self.dates.hour == ftc3.hour and self.dates.minute == ftc3.minute:
                    self.logger.info(f'SMS/MMS Automation: {ftc3.title} - {datetime.now():%H:%M:%S}')

                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=ftc3.title,
                            query=ftc_queries.text_3,
                            msg=messages.ftc.ftc_3_body,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        self.error_handler.add_error_v(error=err, origin=ftc3.title)

            #############################################################################################
            ############################## Returning Customer Automations ###############################
            #############################################################################################

            # RETURNING CUSTOMER TEXT MESSAGE 1 - THANK YOU (SMS)
            rc1 = sms.Campaigns.RC1
            if rc1.enabled:
                if self.dates.hour == rc1.hour and self.dates.minute == rc1.minute:
                    self.logger.info(f'SMS/MMS Automation: {rc1.title} - {datetime.now():%H:%M:%S}')
                    try:
                        sms_automations.create_customer_text(
                            origin=origin,
                            campaign=rc1.title,
                            query=rc_queries.text_1,
                            msg=messages.rc.rc_1_body,
                            send_rwd_bal=True,
                        )
                    except Exception as err:
                        self.error_handler.add_error_v(error=err, origin=rc1.title)

            # RETURNING CUSTOMER TEXT MESSAGE 2 - 5 OFF COUPON (MMS)
            rc2 = sms.Campaigns.RC2
            if rc2.enabled and self.dates.hour == rc2.hour and self.dates.minute == rc2.minute:
                self.logger.info(f'SMS/MMS Automation: {rc2.title} - {datetime.now():%H:%M:%S}')
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
                    self.error_handler.add_error_v(error=err, origin=rc2.title)

            # RETURNING CUSTOMER TEXT MESSAGE 3 - ASK FOR GOOGLE REVIEW (SMS)
            rc3 = sms.Campaigns.RC3
            if rc3.enabled and self.dates.hour == rc3.hour and self.dates.minute == rc3.minute:
                self.logger.info(f'SMS/MMS Automation: {rc3.title} - {datetime.now():%H:%M:%S}')
                try:
                    sms_automations.create_customer_text(
                        origin=origin,
                        campaign=rc3.title,
                        query=rc_queries.text_3,
                        msg=messages.rc.rc_3_body,
                        send_rwd_bal=True,
                    )
                except Exception as err:
                    self.error_handler.add_error_v(error=err, origin=rc3.title)

    @timer
    def run(self):
        self.generate_reports()
        self.twice_per_hour_tasks()
        self.every_other_hourly_tasks()
        self.once_per_day_tasks()
        self.sms_automations()


if __name__ == '__main__':
    tasks = ScheduledTasks()
    if len(sys.argv) > 1:
        if '-v' in sys.argv:
            tasks.verbose = True
        if '-l' in sys.argv:  # Run the integrator in a loop
            while True:
                try:
                    tasks = ScheduledTasks()  # Reinitialize to reset dates
                    tasks.run(eh=tasks.eh, operation='Scheduled Tasks')
                except KeyboardInterrupt:
                    tasks.logger.info(f'Process Terminated by User at {datetime.now():%H:%M:%S}')
                except Exception as err:
                    tasks.error_handler.add_error_v(error=err, origin='Scheduled Tasks', traceback=tb())
                finally:
                    time.sleep(60)

    else:
        tasks.run(eh=tasks.eh, operation=tasks.module)
