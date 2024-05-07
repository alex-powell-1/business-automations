from product_tools.products import *
from reporting.product_reports import create_top_items_report
from setup import date_presets


def update_featured_items(log_file):
    """Sets top 15 items (this time last year) to featured"""
    print(f"Featured Items: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    count = 0

    top_items = create_top_items_report(beginning_date=date_presets.one_year_ago,
                                        ending_date=date_presets.last_year_forecast,
                                        mode="sales", return_format=3)
    for x in top_items:
        try:
            item = Product(x)
        except Exception as err:
            print("Error: Class Construction", file=log_file)
            print(err, file=log_file)
            continue
        else:
            if item.buffered_quantity_available > 0:
                item.set_featured(status='Y', log_file=log_file)
                print(f"Set Item: {item.item_no} {item.descr} to Featured", file=log_file)
                count += 1
            else:
                item.set_featured(status='N', log_file=log_file)
                print(f"No Stock. Set Item: {item.item_no} {item.descr} to Not Featured", file=log_file)

    print(f"{count} products set to Featured", file=log_file)
    print(f"Featured Items: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
