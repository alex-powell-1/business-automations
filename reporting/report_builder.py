import datetime

from jinja2 import Template

from reporting import product_reports
from setup import creds
from setup import email_engine


def item_report():
    with open("./reporting/templates/item_report.html", "r") as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    data = {
        "missing_photo_data": product_reports.get_missing_image_list(),
        "items_with_negative_qty": product_reports.get_negative_items(),
        "missing_ecomm_category_data": product_reports.get_items_with_no_ecomm_category(),
        "non_web_enabled_items": product_reports.get_non_ecomm_enabled_items(),
        "inactive_items_with_stock": product_reports.get_inactive_items_with_stock(),
        "missing_item_descriptions": product_reports.get_missing_item_descriptions(min_length=60)

    }

    email_content = jinja_template.render(data)

    email_engine.send_html_email(from_name=creds.company_name,
                                 from_address=creds.gmail_sales_user,
                                 from_pw=creds.gmail_sales_pw,
                                 recipients_list=creds.sales_group,

                                 subject=f"Item Report for "
                                         f"{(datetime.datetime.now().strftime("%B %d, %Y"))}",

                                 content=email_content,
                                 product_photo=None,
                                 mode="related",
                                 logo=True)
