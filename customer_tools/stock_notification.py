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


def send_stock_notification_emails(log_file):
    """Sends stock notification email updates for items that were out of stock
    but now have stock > 0. Cleans csv so contacts are only notified once"""
    print(f"Send Stock Notification Emails: Starting at {datetime.datetime.now():%H:%M:%S}", file=log_file)
    with open(creds.stock_notification_log, encoding='utf-8') as file:
        # Dataframe for Stock Notification Log
        df = pandas.read_csv(file)
        entries = df.to_dict("records")
        # counter used for dataframe index
        counter = 0
        # sent_messages for totaling messages in log
        sent_messages = 0
        for x in entries:
            email = x['email']
            sku = x['item_no']
            product_photo = creds.photo_path + f"/{sku}.jpg"
            # Create a Product object to get product details
            item = Product(sku)
            if item.buffered_quantity_available > 0:
                print(f"Item No: {sku} - now has stock! Creating message for {email}", file=log_file)
                # Get Customer Details
                customer_number = customer_tools.customers.get_customer_number_by_email(email)
                first_name = ""
                if customer_number is not None:
                    customer = customer_tools.customers.Customer(customer_number)
                    first_name = customer.first_name
                # Generate Greeting
                if first_name != "":
                    greeting = f"Hi {first_name.title()}"
                else:
                    greeting = "Hi there"
                # Create Coupon
                random_coupon_code = ""
                # List of exclusions. Will migrate this to SQL column eventually
                coupon_exclusions = ['45', '804', 'HB', 'BOSTON']
                if item.item_no not in coupon_exclusions:

                    # Create Coupon Code
                    random_coupon_code = generate_random_code(10)

                    # Create CounterPoint Coupons
                    try:
                        cp_create_coupon(description=f"{customer.first_name.title()} "
                                                     f"{customer.last_name.title()}-Stock:{sku}",
                                         code=random_coupon_code,
                                         amount=10,
                                         min_purchase=100,
                                         log_file=log_file)
                    except Exception as e:
                        print(f"CP Coupon Creation Error: {e}", file=log_file)
                    else:
                        print(f"CP Coupon Creation Success! Code: {random_coupon_code}", file=log_file)

                    # Create Coupon Expiration Date
                    expiration_date = utils.format_datetime(datetime.datetime.now() + relativedelta(days=+5))

                    # Send to BigCommerce. Create Coupon.
                    response = bc_create_coupon(name=f"Back in Stock({sku}, {email})",
                                                coupon_type="per_total_discount",
                                                amount=10,
                                                min_purchase=100,
                                                code=random_coupon_code,
                                                max_uses_per_customer=1,
                                                max_uses=1,
                                                expiration=expiration_date)

                    # Get Coupon ID from Big
                    try:
                        coupon_id = response['id']
                    # will throw a type error if same email already has a coupon for this SKU
                    except TypeError:
                        print("Coupon with this email and sku already exists", file=log_file)
                        # Delete from CSV and then continue to next iteration
                        df = df.drop(df.index[counter])
                        print("Failed: incrementing counter", file=log_file)
                        continue
                    else:
                        # Create New Log for new coupons
                        df2 = df
                        df2['id'] = coupon_id

                    # Write new coupon creation log to
                    try:
                        pandas.read_csv(creds.coupon_creation_log)
                    except FileNotFoundError:
                        df2.to_csv(creds.coupon_creation_log, header=True, columns=['date', 'email', 'item_no', "id"],
                                   index=False)
                    else:
                        df2.to_csv(creds.coupon_creation_log, header=False, columns=['date', 'email', 'item_no', "id"],
                                   index=False, mode='a')

                # Send Email to User about their desired SKU
                print(f"Sending email to {email} with code: {random_coupon_code}.", file=log_file)

                send_email(greeting=greeting, email=email, item_number=sku, coupon_code=random_coupon_code,
                           photo=product_photo)

                # Delete Row from
                df = df.drop(df.index[counter])

                sent_messages += 1

            else:
                # Item is out of stock. Will Skip
                counter += 1

        df.to_csv(creds.stock_notification_log, header=True, columns=['date', 'email', 'item_no'], index=False)

    print(f"Send Stock Notification Emails: Completed at {datetime.datetime.now():%H:%M:%S}", file=log_file)
    print(f"Total Messages Sent: {sent_messages}", file=log_file)
    print("-----------------------", file=log_file)