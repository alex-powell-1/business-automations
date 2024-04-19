from product_tools.products import *
from reporting.product_reports import create_top_items_report


def update_featured_items(log_file):
    print(f"Featured Items: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    count = 0
    top_items = create_top_items_report(one_year_ago, last_year_forecast, "sales", return_format=3)
    for x in top_items:
        item = Product(x)
        if item.buffered_quantity_available > 0:
            item.set_featured(status='Y')
            print(f"Set Item: {item.item_no} {item.descr} to Featured", file=log_file)
            count += 1
        else:
            item.set_featured(status='N')
            print(f"No Stock. Set Item: {item.item_no} {item.descr} to Not Featured", file=log_file)

    print(f"{count} products set to Featured", file=log_file)
    print(f"Featured Items: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
