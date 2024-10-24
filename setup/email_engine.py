from setup import creds
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from setup.barcode_engine import generate_barcode
from product_tools.products import Product
from setup import utilities
from datetime import datetime
from reporting import product_reports
from database import Database
from setup.date_presets import Dates

from setup.admin_report_html import boiler_plate, css, body_start, body_end

from product_tools import products
from setup.error_handler import ScheduledTasksErrorHandler as error_handler
from email.utils import formataddr
from jinja2 import Template
import os


class Email:
    name = creds.Company.name
    address = creds.Gmail.Sales.username
    pw = creds.Gmail.Sales.password

    def render(
        to_name,
        to_address,
        subject,
        content,
        mode='mixed',
        logo=False,
        image=None,
        image_name=None,
        barcode=None,
        attachment=False,
    ):
        msg = MIMEMultipart(mode)
        msg['From'] = formataddr((Email.name, Email.address))
        msg['To'] = formataddr((to_name, to_address))
        msg['Subject'] = subject

        msg_html = MIMEText(content, 'html')
        msg.attach(msg_html)

        if logo:
            with open(creds.Company.logo, 'rb') as logo_file:
                logo = logo_file.read()
                msg_logo = MIMEImage(logo, 'jpg')
                msg_logo.add_header('Content-ID', '<image1>')
                msg_logo.add_header('Content-Disposition', 'inline', filename='Logo.jpg')
                msg.attach(msg_logo)

        if image is not None:
            if image_name is not None:
                image_name = image_name
            else:
                image_name = 'product.jpg'

            with open(image, 'rb') as item_photo:
                product = item_photo.read()
                msg_product_photo = MIMEImage(product, 'jpg')
                msg_product_photo.add_header('Content-ID', '<image2>')
                msg_product_photo.add_header('Content-Disposition', 'inline', filename=image_name)
                msg.attach(msg_product_photo)

        if barcode is not None:
            with open(barcode, 'rb') as item_photo:
                product = item_photo.read()
                msg_barcode = MIMEImage(product, 'png')
                msg_barcode.add_header('Content-ID', '<image3>')
                msg_barcode.add_header('Content-Disposition', 'inline', filename='barcode.png')
                msg.attach(msg_barcode)

        if attachment:
            with open(creds.Marketing.DesignLeadForm.pdf_attachment, 'rb') as file:
                pdf = file.read()
                attached_file = MIMEApplication(_data=pdf, _subtype='pdf')
                attached_file.add_header(
                    _name='content-disposition',
                    _value='attachment',
                    filename=f'{creds.Marketing.DesignLeadForm.pdf_name}',
                )
                msg.attach(attached_file)

        return msg

    def send(
        recipients_list,
        subject,
        content,
        mode='mixed',
        logo=False,
        image=None,
        image_name=None,
        barcode=None,
        attachment=False,
        staff=False,
    ):
        def send_mail(to_address, message):
            with smtplib.SMTP('smtp.gmail.com', port=587) as connection:
                connection.ehlo()
                connection.starttls()
                connection.ehlo()
                connection.login(user=Email.address, password=Email.pw)
                connection.sendmail(Email.address, to_address, message.as_string().encode('utf-8'))
                connection.quit()

        if staff:
            # Dictionary of recipients in creds config
            for person in recipients_list:
                to_name = creds.Company.staff[person]['full_name']
                to_address = creds.Company.staff[person]['email']
                msg = Email.render(
                    to_name=to_name,
                    to_address=to_address,
                    subject=subject,
                    content=content,
                    mode=mode,
                    logo=logo,
                    image=image,
                    image_name=image_name,
                    barcode=barcode,
                    attachment=attachment,
                )
                send_mail(to_address=to_address, message=msg)
        else:
            # General Use
            for k, v in recipients_list.items():
                to_name = k
                to_address = v
                msg = Email.render(
                    to_name=to_name,
                    to_address=to_address,
                    subject=subject,
                    content=content,
                    mode=mode,
                    logo=logo,
                    image=image,
                    image_name=image_name,
                    barcode=barcode,
                    attachment=attachment,
                )
                send_mail(to_address=to_address, message=msg)

    class Customer:
        class GiftCard:
            def send(name, email, gc_code, amount):
                """Sends gift card to customer"""
                email = 'alex@settlemyrenursery.com'

                recipient = {name: email}

                try:
                    amount = int(amount)
                except ValueError:
                    error_handler.logger.error(f'Error converting {amount} to int.')

                print(f'Sending Gift Card to {name} at {email}')

                with open('./templates/gift_card.html', 'r') as file:
                    template_str = file.read()

                jinja_template = Template(template_str)

                generate_barcode(data=gc_code, filename=gc_code)

                subject = "You've received a gift card!"

                email_data = {
                    'title': subject,
                    'name': name,
                    'gc_code': gc_code,
                    'amount': amount,
                    'company': creds.Company.name,
                    'company_url': creds.Company.url,
                    'company_phone': creds.Company.phone,
                    'company_address_line_1': creds.Company.address_html_1,
                    'company_address_line_2': creds.Company.address_html_2,
                }

                email_content = jinja_template.render(email_data)

                barcode = f'{creds.Company.barcodes}/{gc_code}.png'

                Email.send(
                    recipients_list=recipient,
                    subject=subject,
                    content=email_content,
                    image='./setup/images/gift_card.jpg',
                    image_name='gift_card.jpg',
                    mode='related',
                    logo=True,
                    barcode=barcode,
                )

                os.remove(barcode)

        class DesignLead:
            def send(first_name, email):
                """Send email and PDF to customer in response to request for design information."""
                recipient = {first_name: email}

                with open('./templates/design_lead/customer_email.html', 'r') as file:
                    template_str = file.read()

                jinja_template = Template(template_str)

                data = creds.Marketing.DesignLeadForm

                email_data = {
                    'title': data.email_subject,
                    'greeting': f'Hi {first_name},',
                    'service': data.service,
                    'company': creds.Company.name,
                    'list_items': data.list_items,
                    'signature_name': data.signature_name,
                    'signature_title': data.signature_title,
                    'company_phone': creds.Company.phone,
                    'company_url': creds.Company.url,
                    'company_reviews': creds.Company.reviews,
                    'unsubscribe_endpoint': f'{creds.API.endpoint}{creds.API.Route.unsubscribe}?email={email}',
                }

                email_content = jinja_template.render(email_data)

                Email.send(
                    recipients_list=recipient,
                    subject=data.email_subject,
                    content=email_content,
                    mode='mixed',
                    logo=False,
                    attachment=data.pdf_attachment,
                )

        class StockNotification:
            def send(greeting, email, item_number, coupon_code, photo=None):
                """Send PDF attachment to customer"""
                recipient = {'': email}
                # Generate HTML
                # ---------------

                # Get Item Details by creating new Product Object
                item = Product(item_number)

                # Create Subject
                email_subject = f'{item.web_title.title()} is back in stock!'

                with open('./templates/stock_notification.html', 'r') as file:
                    template_str = file.read()

                jinja_template = Template(template_str)

                email_data = {
                    'title': email_subject,
                    'greeting': greeting,
                    'item': item.web_title,
                    'qty': item.buffered_quantity_available,
                    'company': creds.Company.name,
                    'item_description': item.web_description,
                    'item_url': item.item_url,
                    'coupon_code': coupon_code,
                    'coupon_offer': creds.Marketing.StockNotification.offer,
                    'signature_name': 'Beth',
                    'signature_title': 'Sales Manager',
                    'company_phone': creds.Company.phone,
                    'company_url': creds.Company.url,
                    'company_reviews': creds.Company.reviews,
                    'company_address_line_1': creds.Company.address_html_1,
                    'company_address_line_2': creds.Company.address_html_2,
                    'unsubscribe_endpoint': f'{creds.API.endpoint}{creds.API.Route.unsubscribe}?email={email}',
                }

                email_content = jinja_template.render(email_data)

                Email.send(
                    recipients_list=recipient,
                    subject=email_subject,
                    content=email_content,
                    image=photo,
                    image_name='coupon.png',
                    barcode=f'./{coupon_code}.png',
                )

    class Staff:
        class AdminReport:
            def send(recipients: list[str]):
                error_handler.logger.info(f'Generating Admin Report Data - Starting at {datetime.now():%H:%M:%S}')
                dates = Dates()
                subject = f'Administrative Report - {dates.today:%x}'
                report_data = product_reports.report_generator(
                    title='Administrative Report',
                    dates=dates,
                    revenue=True,
                    cogs_report=True,
                    last_week_report=True,
                    mtd_month_report=True,
                    last_year_mtd_report=True,
                    forecasting_report=True,
                    top_items_by_category=True,
                    missing_images_report=True,
                    negatives_report=True,
                    ecomm_category_report=True,
                    non_web_enabled_report=True,
                    low_stock_items_report=True,
                    sales_rep_report=True,
                    wholesale_report=True,
                    inactive_items_report=True,
                    missing_descriptions_report=True,
                )
                html_contents = boiler_plate + css + body_start + report_data + body_end

                Email.send(
                    recipients_list=recipients,
                    subject=subject,
                    content=html_contents,
                    logo=True,
                    mode='related',
                    image=None,
                    staff=True,
                )

                error_handler.logger.info(f'Administrative Report: Completed at {datetime.now():%H:%M:%S}')

        class LowStockReport:
            def send(recipients: list[str]):
                error_handler.logger.info(
                    f'Generating Low Stock Report Data - Starting at {datetime.now():%H:%M:%S}'
                )
                dates = Dates()

                subject = f'Low Stock Report - {dates.today:%x}'

                report_data = product_reports.report_generator(
                    title='Low Stock Report', dates=dates, low_stock_items_report=True
                )
                html_contents = boiler_plate + css + body_start + report_data + body_end

                Email.send(
                    recipients_list=recipients,
                    subject=subject,
                    content=html_contents,
                    logo=True,
                    mode='related',
                    image=None,
                    staff=True,
                )

                error_handler.logger.info(f'Low Stock Report: Completed at {datetime.now():%H:%M:%S}')

        class ItemReport:
            def send(recipients: list[str]):
                error_handler.logger.info(f'Items Report: Starting at {datetime.now():%H:%M:%S}')

                with open('./templates/reporting/item_report.html', 'r') as file:
                    template_str = file.read()

                jinja_template = Template(template_str)

                data = {
                    'invisible_items': product_reports.get_invisible_items(),
                    'missing_photo_data': product_reports.get_missing_image_list(),
                    'items_with_negative_qty': product_reports.get_negative_items(),
                    'non_web_enabled_items': product_reports.get_non_web_enabled_items(),
                    'missing_ecomm_category_data': product_reports.get_items_with_no_ecomm_category(),
                    'binding_key_issues': products.get_binding_id_issues(),
                    'inactive_items_with_stock': product_reports.get_inactive_items_with_stock(),
                    'missing_item_descriptions': product_reports.get_missing_item_descriptions(min_length=10),
                }

                email_content = jinja_template.render(data)

                Email.send(
                    recipients_list=recipients,
                    subject=f"Item Report for " f"{(datetime.now().strftime("%B %d, %Y"))}",
                    content=email_content,
                    image=None,
                    mode='related',
                    logo=True,
                    staff=True,
                )

                error_handler.logger.info(f'Items Report: Completed at {datetime.now():%H:%M:%S}')

        class DesignLeadNotification:
            def send(recipients: list[str]):
                """Renders Jinja2 template and sends HTML email to sales team with leads from yesterday
                for follow-up"""
                error_handler.logger.info(f'Lead Notification Email: Starting at {datetime.now():%H:%M:%S}')
                yesterday_entries = []
                db_data = Database.DesignLead.get()
                if db_data:
                    for x in db_data:
                        result = {
                            'date': f'{x[1]:%m/%d/%Y %I:%M:%S %p}',
                            'customer_number': x[2],
                            'first_name': x[3],
                            'last_name': x[4],
                            'email': x[5],
                            'phone': utilities.PhoneNumber(x[6]).to_cp(),
                            'interested_in': [],
                            'timeline': x[13],
                            'street': x[14],
                            'city': x[15],
                            'state': x[16],
                            'zip_code': x[17],
                            'comments': x[18],
                        }
                        if x[7] == 1:
                            result['interested_in'].append('Sketch and Go')
                        if x[8] == 1:
                            result['interested_in'].append('Scaled Drawing')
                        if x[9] == 1:
                            result['interested_in'].append('Digital Rendering')
                        if x[10] == 1:
                            result['interested_in'].append('On-Site Consultation')
                        if x[11] == 1:
                            result['interested_in'].append('Delivery and Placement')
                        if x[12] == 1:
                            result['interested_in'].append('Installation')

                        yesterday_entries.append(result)

                if len(yesterday_entries) < 1:
                    error_handler.logger.info('No entries to send.')
                else:
                    error_handler.logger.info(f'{len(yesterday_entries)} leads from yesterday. Constructing Email')
                    with open('./templates/design_lead/staff_follow_up.html', 'r') as template_file:
                        template_str = template_file.read()

                    jinja_template = Template(template_str)

                    email_data = {
                        'title': 'Design Lead Followup Email',
                        'company': creds.Company.name,
                        'leads': yesterday_entries,
                        'date_format': datetime,
                    }

                    email_content = jinja_template.render(email_data)

                    try:
                        Email.send(
                            recipients_list=recipients,
                            subject='Landscape Design Leads',
                            content=email_content,
                            mode='related',
                            logo=True,
                            staff=True,
                        )

                    except Exception as err:
                        error_handler.error_handler.add_error_v(error=err, origin='lead_notification_email')

                    else:
                        error_handler.logger.success('Email Sent.')

                error_handler.logger.info(f'Lead Notification Email: Finished at {datetime.now():%H:%M:%S}')


if __name__ == '__main__':

    Email.Staff.ItemReport.send(['alex'])
