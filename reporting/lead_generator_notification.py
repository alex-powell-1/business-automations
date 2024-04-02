import numpy as np
import pandas
from jinja2 import Template

import customers.customers
from setup import create_log
from setup import creds
from setup.date_presets import *
from setup.email_engine import send_html_email


def lead_notification_email():
    """Renders Jinja2 template and sends HTML email to sales team with leads from yesterday
    for follow-up"""
    with open(creds.design_lead_log) as lead_file:
        # Dataframe for Log
        df = pandas.read_csv(lead_file)
        df = df.replace({np.nan: None})
        entries = df.to_dict("records")
        # Get yesterday submissions
        yesterday_entries = []
        for x in entries:
            if x['date'][:10] == yesterday or x['date'][:10] == today:
                yesterday_entries.append(x)

        if len(yesterday_entries) > 0:
            with open("./reporting/templates/follow_up.html", "r") as template_file:
                template_str = template_file.read()

            jinja_template = Template(template_str)

            email_data = {
                "title": "Design Lead Followup Email",
                "company": creds.company_name,
                "leads": yesterday_entries,
                "format": create_log,
                "date_format": datetime,
            }

            email_content = jinja_template.render(email_data)

            send_html_email(from_name=creds.company_name,
                            from_address=creds.gmail_sales_user,
                            from_pw=creds.gmail_sales_pw,
                            recipients_list=creds.sales_group,
                            subject="Landscape Design Leads",
                            content=email_content,
                            mode="related",
                            product_photo=None,
                            logo=True)


def create_new_customers():
    """Send yesterday's entry's to Counterpoint as new customers for further marketing.
    Will skip customer if email or phone is already in our system."""
    with open(creds.design_lead_log) as lead_file:
        # Dataframe for Log
        df = pandas.read_csv(lead_file)
        df = df.replace({np.nan: None})
        entries = df.to_dict("records")

        # Get yesterday submissions
        yesterday_entries = []

        for x in entries:
            if x['date'][:10] == yesterday or x['date'][:10] == today:
                yesterday_entries.append(x)

        if len(yesterday_entries) > 0:
            for x in yesterday_entries:
                if not customers.customers.is_customer(email_address=x['email'], phone_number=x['phone']):
                    first_name = x['first_name']
                    last_name = x['last_name']
                    phone_number = x['phone']
                    email = x['email']
                    street_address = x['street']
                    city = x['city']
                    state = x['state']
                    zip_code = x['zip_code']
                    # Add new customer via NCR Counterpoint API
                    customer_number = customers.customers.add_new_customer(first_name=first_name,
                                                                           last_name=last_name,
                                                                           phone_number=phone_number,
                                                                           email_address=email,
                                                                           street_address=street_address,
                                                                           city=city,
                                                                           state=state,
                                                                           zip_code=zip_code)
                    # Log on share
                    log_data = [[str(datetime.now())[:-7], customer_number, first_name, last_name, phone_number, email,
                                 street_address, city, state, zip_code]]
                    df = pandas.DataFrame(log_data, columns=["date", "customer_number", "first_name",
                                                             "last_name", "phone_number", "email", "street", "city",
                                                             "state", "zip_code"])
                    create_log.write_log(df, creds.new_customer_log)
                    print(f"Created customer: {customer_number}: {first_name} {last_name}")
                else:
                    print("Already a customer. Skipping.")
