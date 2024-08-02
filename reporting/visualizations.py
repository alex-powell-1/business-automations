from reporting import product_reports
import matplotlib.pyplot as plt
from setup.query_engine import QueryEngine as db
from setup.date_presets import *


def sales_over_time_multi(items, start_date, end_date, mode='quantity'):
    """Takes an array of item numbers and creates a data visualization for qty sold over a given time period"""
    y_label = ''
    result = {}
    if mode == 'sales':
        key_data = 'CALC_EXT_PRC'
        y_label = 'Revenue'

    elif mode == 'quantity':
        key_data = 'QTY_SOLD'
        y_label = 'Qty Sold'

    for x in items:
        query = f"""
        SELECT BUS_DAT, ITEM_NO, {key_data}
        FROM PS_TKT_HIST_LIN
        WHERE ITEM_NO = '{x}' AND BUS_DAT >= '{start_date} 00:00:00' and BUS_DAT <= '{end_date}'
        """
        response = db.query(query)
        if response is not None:
            for y in response:
                date = y[0].strftime('%m-%d')
                item = y[1]
                qty_sold = int(y[2])
                try:
                    result[date][item] += qty_sold
                except KeyError:
                    try:
                        result[date][item] = 0
                    except KeyError:
                        result[date] = {item: 0}
    dates = list(result.keys())
    legend = []
    for x in items:
        result_list = []
        for y in dates:
            try:
                result_list.append(result[y][x])
            except KeyError:
                result[y][x] = 0
                result_list.append(result[y][x])
        plt.plot(dates, result_list)
        legend.append(str(x))
    # Prepare the chart
    plt.title(f'Item Sales by {mode.title()}\n{start_date} - {end_date}')
    plt.xlabel('Dates')
    plt.ylabel(y_label)
    plt.legend(legend)
    plt.show()


def top_item_visualization(start_date, end_date, category, mode='sales'):
    items = product_reports.create_top_items_report(
        start_date, end_date, mode=mode, number_of_items=10, category=category, return_format=3
    )
    sales_over_time_multi(items=items, start_date=start_date, end_date=end_date, mode=mode)


# Example
# Creates graph of sales (by qty) of top trees from this time period last year.
top_item_visualization(one_year_ago, last_year_forecast, 'TREES', mode='quantity')
