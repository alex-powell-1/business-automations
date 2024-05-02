from product_tools.products import *
from reporting import product_reports


# This module will set related products for items in ecommerce store.


def set_related_items_by_category(log_file):
    print(f"Setting related items: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    categories = get_product_categories_cp()
    for x in categories:
        # staff recommended Items for each category
        recommended_items = {
            "ANNUAL": [200838, 'BTSP4MP', 200566],
            "BULB": [10093],
            "CHRISTMAS": [],
            "DECIDUOUS": [10093, 'BTSP4MP', 'PERM20', 'PT4MP'],
            "DECOR": [],
            "EDIBLES": [10093, 'BTSP4MP', 'PT4MP', 'BR4', 'GT4MP'],
            "EVERGREEN": ['10093', 'BTSP4MP', 'PERM20', 'HT4MP', 'PT4MP'],
            "FLOWERING": ['BTSP4MP', 200566, 10093, 'PERM20'],
            "GRASSES": [],
            "GROUND": [],
            "HEALTH": [],
            "HOUSE": [],
            "PERENNIAL": [],
            "POTTERY": [],
            "SEEDS": [],
            "SERVICES": [],
            "SUPPLIES": [],
            "TOOLS": [],
            "TREES": [10093, 'BTSP4MP', 'PERM20'],
            "WORKSHOP": []
        }

        popular_items = product_reports.create_top_items_report(beginning_date=date_presets.two_weeks_ago,
                                                                ending_date=date_presets.today,
                                                                mode="quantity",
                                                                number_of_items=8,
                                                                category=x,
                                                                return_format=3)
        if popular_items is not None:
            related_items = recommended_items[x] + popular_items
        else:
            related_items = recommended_items[x]

        # Cast these as product IDs with no duplicates
        if related_items is not None:
            related_items_as_prod_id = set()
            for y in related_items:
                related_items_as_prod_id.add(get_bc_product_id(y))

            payload = {'related_products': list(related_items_as_prod_id)}

            products = get_products_by_category(x, ecomm_only=True)
            count = 1
            if products is not None:
                for z in products:
                    bc_update_product(get_bc_product_id(z), payload=payload)
                    print(f"Cat: {x}, {count}/{len(products)}, Updated Item: {z}", file=log_file)
                    count += 1

    print(f"Setting related items: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
