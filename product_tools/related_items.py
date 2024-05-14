from product_tools.products import *
from reporting import product_reports


# This module will set related products for items in ecommerce store.


def set_related_items_by_category(log_file):
    print(f"Setting related items: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    # Get all product categories as list from CP
    categories = get_product_categories_cp()
    # Set recommended products for each category. This for loop grabs the key to work with.
    for x in categories:
        # staff recommended items for each category. Counterpoint SKU, NOT BC product id.
        recommended_items = {
            "ANNUAL": [200838, 'BTSP4MP', 'JACKS'],
            "BULB": [10093],
            "CHRISTMAS": [],
            "DECIDUOUS": [10093, 'B0019', 'B0049', 'B0020'],
            "DECOR": [],
            "EDIBLES": [10093, 'B0019', 'B0020', 'BR4', 'GT4MP'],
            "EVERGREEN": ['10093', 'B0019', 'B0049', 'B0048', 'B0020'],
            "FLOWERING": ['B0019', 200566, 10093, 'B0049'],
            "GRASSES": ['B0020'],
            "GROUND": ['B0020'],
            "HEALTH": [],
            "HOUSE": [],
            "PERENNIAL": ['B0020'],
            "POTTERY": [],
            "SEEDS": ['GT4MP'],
            "SERVICES": [],
            "SUPPLIES": [],
            "TOOLS": [],
            "TREES": [10093, 'B0019', 'B0049'],
            "WORKSHOP": []
        }

        # Get the most popular items in this category during the last two weeks
        popular_items = product_reports.create_top_items_report(beginning_date=date_presets.two_weeks_ago,
                                                                ending_date=date_presets.today,
                                                                mode="quantity",
                                                                number_of_items=8,
                                                                category=x,
                                                                return_format=3)
        # If there are popular items, add them to the list
        if popular_items is not None:
            related_items = recommended_items[x] + popular_items
        else:
            related_items = recommended_items[x]

        # Cast these as product IDs with no duplicates
        if related_items is not None:
            related_items_as_prod_id = set()
            for y in related_items:
                product_id = get_bc_product_id(y)
                # if a popular item doesn't have a product id, filter it out of the set
                if product_id is not None:
                    related_items_as_prod_id.add(product_id)

            # create payload to send to big commerce
            payload = {'related_products': list(related_items_as_prod_id)}

            products = get_products_by_category(x, ecomm_only=True)
            count = 1
            if products is not None:
                for z in products:
                    big_products.bc_update_product(get_bc_product_id(z), payload=payload, log_file=log_file)
                    print(f"Cat: {x}, {count}/{len(products)}, Updated Item: {z}", file=log_file)
                    count += 1

    print(f"Setting related items: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
