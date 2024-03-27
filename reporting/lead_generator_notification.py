import numpy as np
import pandas
from jinja2 import Template

from setup import create_log
from setup import creds
from setup.date_presets import *
from setup.email_engine import send_html_email


def lead_notification_email():
    """Send PDF attachment to customer"""
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
                "title": "Customer Followup Email",
                "company": creds.company_name,
                "leads": yesterday_entries,
                "format": create_log,
                "date_format": datetime,
            }

            for x in yesterday_entries:
                print(datetime.strptime(x['date'], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y %I:%M:%S %p"))
                print(x['street'], x['city'], x['state'], x['zip_code'])
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
