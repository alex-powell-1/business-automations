from datetime import datetime

from jinja2 import Template

from product_tools import products
from reporting import product_reports
from setup import creds, email_engine
from setup.error_handler import ScheduledTasksErrorHandler


def item_report(recipients):
	ScheduledTasksErrorHandler.logger.info(f'Items Report: Starting at {datetime.now():%H:%M:%S}')

	with open('./templates/reporting/item_report.html', 'r') as file:
		template_str = file.read()

	jinja_template = Template(template_str)

	data = {
		'missing_photo_data': product_reports.get_missing_image_list(),
		'items_with_negative_qty': product_reports.get_negative_items(),
		'missing_ecomm_category_data': product_reports.get_items_with_no_ecomm_category(),
		'binding_key_issues': products.get_binding_id_issues(),
		'non_web_enabled_items': product_reports.get_non_ecomm_enabled_items(),
		'inactive_items_with_stock': product_reports.get_inactive_items_with_stock(),
		'missing_item_descriptions': product_reports.get_missing_item_descriptions(min_length=10),
	}

	email_content = jinja_template.render(data)

	email_engine.Email(
		from_name=creds.company_name,
		from_address=creds.sales_email,
		from_pw=creds.sales_password,
		recipients_list=recipients,
		subject=f"Item Report for " f"{(datetime.now().strftime("%B %d, %Y"))}",
		content=email_content,
		product_photo=None,
		mode='related',
		logo=True,
		staff=True,
	)

	ScheduledTasksErrorHandler.logger.info(f'Items Report: Completed at {datetime.now():%H:%M:%S}')
