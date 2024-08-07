from setup import creds
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from barcode_engine import generate_barcode

from email.utils import formataddr
from jinja2 import Template
import os


class Email:
    name = creds.company_name
    address = creds.sales_email
    pw = creds.sales_password

    def __init__(
        self,
        recipients_list,
        subject,
        content,
        mode,
        image=None,
        image_name=None,
        barcode=None,
        logo=True,
        attachment=None,
        staff=False,
    ):
        self.recipients_list = recipients_list
        self.subject = subject
        self.content = content
        self.mode = mode
        self.image = image
        self.image_name = image_name
        self.barcode = barcode
        self.logo = logo
        self.attachment = attachment
        self.staff = staff
        self.send()

    def render(self):
        self.msg = MIMEMultipart(self.mode)
        self.msg['From'] = formataddr((Email.name, Email.address))
        self.msg['To'] = formataddr((self.to_name, self.to_address))
        self.msg['Subject'] = self.subject

        self.msg_html = MIMEText(self.content, 'html')
        self.msg.attach(self.msg_html)

        if self.logo:
            with open(creds.logo, 'rb') as logo_file:
                logo = logo_file.read()
                msg_logo = MIMEImage(logo, 'jpg')
                msg_logo.add_header('Content-ID', '<image1>')
                msg_logo.add_header('Content-Disposition', 'inline', filename='Logo.jpg')
                self.msg.attach(msg_logo)

        if self.image is not None:
            if self.image_name is not None:
                image_name = self.image_name
            else:
                image_name = 'product.jpg'

            with open(self.image, 'rb') as item_photo:
                product = item_photo.read()
                msg_product_photo = MIMEImage(product, 'jpg')
                msg_product_photo.add_header('Content-ID', '<image2>')
                msg_product_photo.add_header('Content-Disposition', 'inline', filename=image_name)
                self.msg.attach(msg_product_photo)

        if self.barcode is not None:
            with open(self.barcode, 'rb') as item_photo:
                product = item_photo.read()
                msg_barcode = MIMEImage(product, 'png')
                msg_barcode.add_header('Content-ID', '<image3>')
                msg_barcode.add_header('Content-Disposition', 'inline', filename='barcode.png')
                self.msg.attach(msg_barcode)

        if self.attachment:
            with open(creds.design_pdf_attachment, 'rb') as file:
                pdf = file.read()
                attached_file = MIMEApplication(_data=pdf, _subtype='pdf')
                attached_file.add_header(
                    _name='content-disposition', _value='attachment', filename=f'{creds.design_pdf_name}'
                )
                self.msg.attach(attached_file)

    def send(self):
        def send_mail():
            with smtplib.SMTP('smtp.gmail.com', port=587) as connection:
                connection.ehlo()
                connection.starttls()
                connection.ehlo()
                connection.login(user=Email.address, password=Email.pw)
                connection.sendmail(Email.address, self.to_address, self.msg.as_string().encode('utf-8'))
                connection.quit()

        if self.staff:
            # Dictionary of recipients in creds config
            for person in self.recipients_list:
                self.to_name = creds.staff[person]['full_name']
                self.to_address = creds.staff[person]['email']
                self.render()
                send_mail()
        else:
            # General Use
            for k, v in self.recipients_list.items():
                self.to_name = k
                self.to_address = v
                self.render()
                send_mail()

    class GiftCard:
        def send(name, email, gc_code, amount):
            """Sends gift card to customer"""
            recipient = {name: email}

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
                'company': creds.company_name,
                'company_url': creds.company_url,
                'company_phone': creds.company_phone,
                'company_address_line_1': creds.company_address_html_1,
                'company_address_line_2': creds.company_address_html_2,
            }

            email_content = jinja_template.render(email_data)

            Email(
                recipients_list=recipient,
                subject=subject,
                content=email_content,
                image='./setup/images/gift_card.jpg',
                image_name='gift_card.jpg',
                mode='related',
                logo=True,
                barcode=f'./{gc_code}.png',
            )

            os.remove(f'./{gc_code}.png')

    class DesignLead:
        def send(first_name, email):
            """Send email and PDF to customer in response to request for design information."""
            recipient = {first_name: email}

            with open('./templates/design_lead/customer_email.html', 'r') as file:
                template_str = file.read()

            jinja_template = Template(template_str)

            email_data = {
                'title': creds.email_subject,
                'greeting': f'Hi {first_name},',
                'service': creds.service,
                'company': creds.company_name,
                'list_items': creds.list_items,
                'signature_name': creds.signature_name,
                'signature_title': creds.signature_title,
                'company_phone': creds.company_phone,
                'company_url': creds.company_url,
                'company_reviews': creds.company_reviews,
            }

            email_content = jinja_template.render(email_data)

            Email(
                recipients_list=recipient,
                subject=creds.email_subject,
                content=email_content,
                mode='mixed',
                logo=False,
                attachment=creds.design_pdf_attachment,
            )


if __name__ == '__main__':
    pass
