from datetime import datetime
from dateutil.relativedelta import relativedelta

from jinja2 import Template

from reporting import product_reports, product_reports_new
from setup import creds, date_presets, email_engine
from product_tools import products


def item_report(recipient, log_file):
    print(f"Items Report: Starting at {datetime.now():%H:%M:%S}", file=log_file)

    with open("./reporting/templates/item_report.html", "r") as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    data = {
        "missing_photo_data": product_reports.get_missing_image_list(),
        "items_with_negative_qty": product_reports.get_negative_items(),
        "missing_ecomm_category_data": product_reports.get_items_with_no_ecomm_category(),
        "binding_key_issues": products.get_binding_id_issues(),
        "non_web_enabled_items": product_reports.get_non_ecomm_enabled_items(),
        "inactive_items_with_stock": product_reports.get_inactive_items_with_stock(),
        "missing_item_descriptions": product_reports.get_missing_item_descriptions(min_length=10)

    }

    email_content = jinja_template.render(data)

    email_engine.send_html_email(from_name=creds.company_name,
                                 from_address=creds.gmail_sales_user,
                                 from_pw=creds.gmail_sales_pw,
                                 recipients_list=recipient,

                                 subject=f"Item Report for "
                                         f"{(datetime.now().strftime("%B %d, %Y"))}",

                                 content=email_content,
                                 product_photo=None,
                                 mode="related",
                                 logo=True)
    print(f"Items Report: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)


def administrative_report():
    with open("./reporting/templates/admin_report.html", "r") as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    data = {
        "reporting_periods": date_presets.reporting_periods,
        "reports": product_reports_new,
        "date_presets": date_presets,
        "relative_delta": relativedelta,
        "day_of_week": datetime.today().isoweekday(),
        "datetime": datetime,
    }

    email_content = jinja_template.render(data)

    email_engine.send_html_email(from_name=creds.company_name,
                                 from_address=creds.gmail_sales_user,
                                 from_pw=creds.gmail_sales_pw,
                                 recipients_list=creds.alex_only,

                                 subject=f"Administrative Report - "
                                         f"{(datetime.now().strftime("%B %d, %Y"))}",

                                 content=email_content,
                                 product_photo=None,
                                 mode="related",
                                 logo=True)