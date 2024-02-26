from product_tools.products import *


def update_featured_items():
    top_items = create_top_items_report(one_year_ago, last_year_forecast, "sales", return_format=3)
    print("Setting Featured Products")
    for x in top_items:
        item = Product(x)
        if item.featured == "N":
            if item.buffered_quantity_available > 0:
                print(f"Setting item: {item.item_no} to featured.")
                item.set_featured(status='Y')
            else:
                item.set_featured(status='N')
