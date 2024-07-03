from product_tools.products import *
from reporting.product_reports import create_top_items_report
from setup import date_presets
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


def update_featured_items():
	"""Sets top 15 items (this time last year) to featured"""
	error_handler.logger.info(f'Featured Items: Starting at {datetime.now():%H:%M:%S}')
	count = 0
	top_items = create_top_items_report(
		beginning_date=date_presets.one_year_ago,
		ending_date=date_presets.last_year_forecast,
		mode='sales',
		return_format=3,
	)
	for x in top_items:
		try:
			item = Product(x)
		except Exception as err:
			error_handler.error_handler.add_error_v(f'Error: {err}')
			continue
		else:
			if item.buffered_quantity_available > 0:
				item.set_featured(status='Y')
				error_handler.logger.info(f'Set Item: {item.item_no} {item.descr} to Featured')
				count += 1
			else:
				item.set_featured(status='N')
				error_handler.logger.info(f'No Stock. Set Item: {item.item_no} {item.descr} to Not Featured')

	error_handler.logger.info(f'{count} products set to Featured')
	error_handler.logger.info(f'Featured Items: Finished at {datetime.now():%H:%M:%S}')
