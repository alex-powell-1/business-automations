from setup import creds
import pandas
from setup.email_engine import send_html_email
from product_tools.products import Product
from jinja2 import Template


def send_email(email, item_number):
    """Send PDF attachment to customer"""
    recipient = {"": email}
    # Generate HTML
    # ---------------

    # Get Item Details by creating new Product Object
    item = Product(item_number)

    # Create Subject
    email_subject = f"{item.web_title.title()} is back in stock!"

    with open("./customers/templates/stock_notification.html", "r") as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    email_data = {
        "title": email_subject,
        "item": item.web_title,
        "qty": item.buffered_quantity_available,
        "company": creds.company_name,
        "item_description": item.web_description,
        "item_url": item.item_url,
        "signature_name": creds.signature_name,
        "signature_title": creds.signature_title,
        "company_phone": creds.company_phone,
        "company_url": creds.company_url,
        "company_reviews": creds.company_reviews,
        "company_address": creds.company_address_html
    }

    email_content = jinja_template.render(email_data)

    send_html_email(from_name=creds.company_name,
                    from_address=creds.gmail_user,
                    recipients_list=recipient,
                    subject=email_subject,
                    content=email_content,
                    logo=True)


def send_stock_notification_emails():
    with open("./test.csv") as file:
        df = pandas.read_csv(file)
        entries = df.to_dict("records")
        print(entries)
        counter = 0
        for x in entries:
            print(x)
            print("Beginning Loop")
            item = Product(x['sku'])
            if item.buffered_quantity_available > 0:
                send_email(x['email'], x['sku'])
            df = df.drop(df.index[counter])
            counter += 1
        df.to_csv("./test.csv", header=True, columns=['sku', 'email'], index=False)
