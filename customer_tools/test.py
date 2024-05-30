import datetime
from email import utils

import pandas
from dateutil.relativedelta import relativedelta
from jinja2 import Template

import customer_tools
from big_commerce.coupons import generate_random_code, bc_create_coupon, cp_create_coupon
from product_tools.products import Product
from setup import creds
from setup.email_engine import send_html_email


def send_email(greeting, email, item_number, coupon_code, photo):
    """Send PDF attachment to customer"""
    recipient = {"": email}
    # Generate HTML
    # ---------------

    # Get Item Details by creating new Product Object
    item = Product(item_number)

    # Create Subject
    email_subject = f"{item.web_title.title()} is back in stock!"

    with open("./customer_tools/templates/stock_notification.html", "r") as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    email_data = {
        "title": email_subject,
        "greeting": greeting,
        "item": item.web_title,
        "qty": item.buffered_quantity_available,
        "company": creds.company_name,
        "item_description": item.web_description,
        "item_url": item.item_url,
        "coupon_code": coupon_code,
        "coupon_offer": "save $10 on an order of $100 or more",
        "signature_name": creds.signature_name,
        "signature_title": creds.signature_title,
        "company_phone": creds.company_phone,
        "company_url": creds.company_url,
        "company_reviews": creds.company_reviews,
        "company_address_line_1": creds.company_address_html_1,
        "company_address_line_2": creds.company_address_html_2
    }

    email_content = jinja_template.render(email_data)

    send_html_email(from_name=creds.company_name,
                    from_address=creds.sales_email,
                    from_pw=creds.sales_password,
                    recipients_list=recipient,
                    subject=email_subject,
                    content=email_content,
                    product_photo=photo,
                    mode="related",
                    logo=True)