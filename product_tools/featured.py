from product_tools.products import *


def update_featured_items():
    top_items = create_top_items_report(one_year_ago, last_year_forecast, "sales", return_format=3)
    print("Setting Featured Products")
    for x in top_items:
        print(x)
        item = Product(x)
        if item.buffered_quantity_available > 0:
            item.set_featured(status='Y')
        else:
            item.set_featured(status='N')


update_featured_items()