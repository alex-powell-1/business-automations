from reporting.product_reports import sales_over_time_multi
from setup.date_presets import *
from reporting import product_reports


def top_item_visualization(start_date, end_date, category, mode="sales"):
    items = product_reports.create_top_items_report(start_date, end_date,
                                                    mode=mode,
                                                    number_of_items=10,
                                                    category=category,
                                                    return_format=3)
    sales_over_time_multi(items=items,
                          start_date=start_date,
                          end_date=end_date,
                          mode=mode)


top_item_visualization(one_year_ago, last_year_forecast, "TREES", mode="quantity")
