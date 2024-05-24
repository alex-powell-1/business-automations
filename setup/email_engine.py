from setup import creds
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import formataddr


def send_html_email(from_name, from_address, from_pw, recipients_list, subject, content, product_photo, mode, logo=True):
    # Dictionary of recipients in creds
    for k, v in recipients_list.items():
        to_name = k
        to_address = v

        msg = MIMEMultipart(mode)
        msg['From'] = formataddr((from_name, from_address))
        msg['To'] = formataddr((to_name, to_address))
        msg["Subject"] = subject

        msg_html = MIMEText(content, 'html')
        msg.attach(msg_html)

        if logo:
            with open(creds.logo, 'rb') as logo_file:
                logo = logo_file.read()
                msg_logo = MIMEImage(logo, 'jpg')
                msg_logo.add_header('Content-ID', '<image1>')
                msg_logo.add_header('Content-Disposition', 'inline', filename='Logo.jpg')
                msg.attach(msg_logo)

        if product_photo is not None:
            with open(product_photo, 'rb') as item_photo:
                product = item_photo.read()
                msg_product_photo = MIMEImage(product, 'jpg')
                msg_product_photo.add_header('Content-ID', '<image2>')
                msg_product_photo.add_header('Content-Disposition', 'inline', filename='product.jpg')
                msg.attach(msg_product_photo)

        with smtplib.SMTP("smtp.gmail.com", port=587) as connection:
            connection.ehlo()
            connection.starttls()
            connection.ehlo()
            connection.login(user=from_address, password=from_pw)
            connection.sendmail(from_address, to_address, msg.as_string().encode('utf-8'))
            connection.quit()


def send_staff_email(from_name, from_address, from_pw, recipients_list,
                     subject, content, product_photo, mode, logo=True):
    # Dictionary of recipients in creds
    for person in recipients_list:
        to_name = creds.staff[person]['full_name']
        to_address = creds.staff[person]['email']

        msg = MIMEMultipart(mode)
        msg['From'] = formataddr((from_name, from_address))
        msg['To'] = formataddr((to_name, to_address))
        msg["Subject"] = subject

        msg_html = MIMEText(content, 'html')
        msg.attach(msg_html)

        if logo:
            with open(creds.logo, 'rb') as logo_file:
                logo = logo_file.read()
                msg_logo = MIMEImage(logo, 'jpg')
                msg_logo.add_header('Content-ID', '<image1>')
                msg_logo.add_header('Content-Disposition', 'inline', filename='Logo.jpg')
                msg.attach(msg_logo)

        if product_photo is not None:
            with open(product_photo, 'rb') as item_photo:
                product = item_photo.read()
                msg_product_photo = MIMEImage(product, 'jpg')
                msg_product_photo.add_header('Content-ID', '<image2>')
                msg_product_photo.add_header('Content-Disposition', 'inline', filename='product.jpg')
                msg.attach(msg_product_photo)

        with smtplib.SMTP("smtp.gmail.com", port=587) as connection:
            connection.ehlo()
            connection.starttls()
            connection.ehlo()
            connection.login(user=from_address, password=from_pw)
            connection.sendmail(from_address, to_address, msg.as_string().encode('utf-8'))
            connection.quit()