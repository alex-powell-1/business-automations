import datetime
import os

from product_tools.products import get_ecomm_items_with_stock
from setup import creds
from setup.date_presets import *
from database import Database as db


def get_quantity_available(item, buffered=False):
    if buffered:
        query = f"""
        SELECT QTY_AVAIL, PROF_NO_1
        FROM VI_IM_ITEM_WITH_INV
        WHERE ITEM_NO = '{item}'
        """
        response = db.query(query)
        if response is not None:
            quantity = int(response[0][0])
            buffer = int(response[0][1]) if response[0][1] is not None and response[0][1] >= 0 else 0
            return quantity - buffer
    else:
        query = f"""
        SELECT QTY_AVAIL
        FROM IM_INV
        WHERE ITEM_NO = '{item}'
        """
        quantity = db.query(query)
        quantity = int(quantity[0][0])
        return quantity


def cost_of_goods_sold(start_date, stop_date, store):
    query = f"""
    SELECT SUM(TOT_EXT_COST)
    FROM PS_TKT_HIST
    WHERE TKT_DT >= '{start_date}' AND 
    TKT_DT <= '{stop_date}' AND STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        if response[0][0] is not None:
            return response[0][0]
        else:
            return 0


def get_non_web_visibile_items():
    query = """
    SELECT ITEM_NO, ADDL_DESCR_1, (QTY_AVAIL - PROF_NO_1) as BUFFERED_QTY
    FROM VI_IM_ITEM_WITH_INV
    WHERE (QTY_AVAIL-PROF_NO_1) > 0 AND IS_ECOMM_ITEM = 'Y'AND USR_CPC_IS_ENABLED = 'N'
    ORDER BY BUFFERED_QTY DESC
    """
    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['ADDL_DESCR_1']
            qty = int(item['BUFFERED_QTY'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def revenue_sales_report(start_date, stop_date, split=True, anna_mode=False, short=False):
    query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
    'select distinct PS_STR.STR_ID as GRP_ID, PS_STR.DESCR as GRP_DESCR
    from PS_STR
    where ( (1=1) )
    union
    select distinct VI_PS_TKT_HIST.STR_ID as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    where not exists(select 1 from PS_STR where PS_STR.STR_ID = VI_PS_TKT_HIST.STR_ID) and ( (1=1) ) and
    ( (1=1) ) and %ANYPERIODFILTER%',
    'select VI_PS_TKT_HIST.STR_ID as GRP_ID, %HISTCOLUMNS%
    from %HISTORY% where ( (1=1) ) and ( (1=1) ) and %PERIODFILTER%', '
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{stop_date}'')', '
    (1=0) ', ' (1=0) ', 0, 0, 'GRP_ID', 2
    """
    results = db.query(query)
    if results is not None:
        retail_sales = results[0][5]
        retail_valid_returns = results[0][9]
        retail_nonvalid_returns = results[0][10]
        web_sales = 0
        web_valid_returns = 0
        web_nonvalid_returns = 0
        if len(results) > 1:
            web_sales = results[1][5]
            web_valid_returns = results[1][9]
            web_nonvalid_returns = results[1][10]

        # for reports with BOTH retail and web sales separated
        if split:
            return (
                f"\n<p>In-Store Sales: ${"{:,}".format(retail_sales - retail_valid_returns - retail_nonvalid_returns)}<br>"
                f"E-Comm Sales: ${"{:,}".format(web_sales - web_valid_returns - web_nonvalid_returns)}</p>"
            )

        elif anna_mode:
            if start_date == stop_date:
                date_prefix = f'\n<p>{start_date:%b %d}<br>'
            else:
                date_prefix = f'\n<p><b>{start_date:%b %d} - {stop_date:%b %d}</b>:<br>'

            return (
                date_prefix + f"In-Store Sales: ${"{:,}".format(retail_sales - retail_valid_returns
                                                      - retail_nonvalid_returns)}<br>"
                f"E-Comm Sales: ${"{:,}".format(web_sales - web_valid_returns - web_nonvalid_returns)}<br>"
                f"Total: ${"{:,}".format((retail_sales + web_sales) -
                                             (web_valid_returns + web_nonvalid_returns) -
                                             (retail_valid_returns + retail_nonvalid_returns))}<br>"
                f"{get_total_tickets(start_date, stop_date)}</p>"
            )

        # for reports with combined reporting
        else:
            if short:
                return (
                    f"\n<p>{start_date:%B %Y}: "
                    f"${"{:,}".format((retail_sales + web_sales) -
                                          (web_valid_returns + web_nonvalid_returns) -
                                          (retail_valid_returns + retail_nonvalid_returns))}</p>"
                )
            else:
                return (
                    f"\n<p>{start_date} - {stop_date}: "
                    f"${"{:,}".format((retail_sales + web_sales) -
                                          (web_valid_returns + web_nonvalid_returns) -
                                          (retail_valid_returns + retail_nonvalid_returns))}"
                    f"{get_total_tickets(start_date, stop_date)}</p>"
                )
    else:
        return 'No Revenue Data Today'


def get_total_tickets(start_day, end_day):
    contents = ''
    data = {}
    categories = ['RETAIL', 'WHOLESALE']
    for category in categories:
        query = f"""
        SELECT count(tic.TOT)
        FROM PS_TKT_HIST tic
        INNER JOIN AR_CUST cust on tic.CUST_NO = cust.CUST_NO
        WHERE BUS_DAT >= '{start_day}' AND BUS_DAT <= '{end_day}' AND cust.CATEG_COD = '{category}'
        """
        response = db.query(query)
        if response is not None:
            data[category] = response[0][0]

    contents += f"\nTotal Retail Tickets: {data['RETAIL']}<br>"
    contents += f"\nTotal Wholesale Tickets: {data['WHOLESALE']}<br>"
    contents += f"\nTotal Tickets: {data['WHOLESALE'] + data['RETAIL']}"
    return contents


def get_list_of_current_photo_sku():
    """Returns a sorted list of unique filenames from ItemImages Folder"""
    list_of_files = os.listdir(creds.Company.product_images)
    list_of_sku = []
    for item in list_of_files[1:]:
        name = item.split('.')[0]
        sku = str(name.split('^')[0]).lower()  # lower case for comparison
        list_of_sku.append(sku)
    final_sku_list = sorted(set(list_of_sku))
    return final_sku_list


def create_top_items_report(
    beginning_date,
    ending_date,
    mode='sales',
    merged=False,
    binding_id='',
    number_of_items=15,
    category='ALL',
    return_format=1,
    in_stock_only=False,
):
    """creates top items report by sales or quantity"""
    # format 3 is a list of top item skus
    if return_format == 3:
        result = []
    else:
        # Format one and two are strings (html, and text based response)
        result = ''

    if category == 'ALL':
        category_var = ' (1=1) '
    else:
        category_var = f"(VI_PS_TKT_HIST_LIN.CATEG_COD = ''{category}'')"

    if mode == 'quantity':
        rank_filter = 'SLS_QTY_A - RTN_QTY_VALID_A - RTN_QTY_NONVALID_A'
        if return_format == 2:
            header = f'\nTop {number_of_items} (QTY):\n\n'
        else:
            header = f'\n<u>Top {number_of_items} (QTY):</u>'
    else:
        rank_filter = 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A'
        if return_format == 2:
            header = f'\nTop {number_of_items} (Sales):\n\n'
        else:
            header = f'\n<u>Top {number_of_items} (Sales):</u>'

    if return_format == 1 or return_format == 2:
        result += header

    if merged:
        item_filter = f"(IM_ITEM.USR_PROF_ALPHA_16 = ''{binding_id}'')"
    else:
        item_filter = '(1=1)'
    top_items_query = f"""
        "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
        'select distinct IM_ITEM.ITEM_NO as GRP_ID, IM_ITEM.DESCR as GRP_DESCR
        from IM_ITEM
        where ( {item_filter} )
        union select distinct VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, NULL as GRP_DESCR
        from %HISTORY%
        where not exists(select 1 from IM_ITEM where IM_ITEM.ITEM_NO = VI_PS_TKT_HIST_LIN.ITEM_NO)
        and ( (1=1) ) and (({category_var})) and %ANYPERIODFILTER%',
        'select VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, %HISTCOLUMNS%
        from %HISTORY%
        where ( (1=1) ) and (({category_var})) and %PERIODFILTER%', '
        (VI_PS_TKT_HIST.POST_DAT >= ''{beginning_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{ending_date}'')', '
        (1=0) ', ' (1=0) ', {number_of_items}, 0, '{rank_filter}', 2
        """

    top_items_by_sales = db.query(top_items_query)
    counter = 1
    if top_items_by_sales is not None:
        for item in top_items_by_sales:
            if mode == 'quantity':
                total = ''
                num_sold = int(item[4])
                num_valid_returns = item[7]
                num_invalid_returns = item[8]
                num_returns = int(num_valid_returns + num_invalid_returns)
                current_stock = get_quantity_available(item[0])
            else:
                revenue = item[2]
                num_sold = int(item[4])
                num_valid_returns = item[7]
                num_invalid_returns = item[8]
                num_returns = int(num_valid_returns + num_invalid_returns)
                amt_valid_return = item[9]
                amt_invalid_return = item[10]
                return_amount = amt_valid_return + amt_invalid_return
                total = round(revenue - return_amount, 2)
                current_stock = get_quantity_available(item[0])
            # html version
            if return_format == 1:
                if mode == 'quantity':
                    result += (
                        f'\n<p>#{counter}: {item[0]} - {item[1]}: {num_sold - num_returns} Units Sold - '
                        f'Current Stock: {current_stock}</p>'
                    )
                else:
                    result += (
                        f"\n<p>#{counter}: {item[0]} - {item[1]} - Revenue: ${"{:,}".format(total)} - "
                        f"Units Sold: {num_sold - num_returns} - "
                        f"Current Stock: {current_stock}</p>"
                    )
            # text version
            elif return_format == 2:
                if mode == 'quantity':
                    result += (
                        f'#{counter}: {item[0]} - {item[1]}: {num_sold - num_returns} Units Sold - '
                        f'Current Stock: {current_stock}\n'
                    )
                else:
                    result += (
                        f"#{counter}: {item[0]} - {item[1]} - Revenue: ${"{:,}".format(total)} - "
                        f"Units Sold: {num_sold - num_returns} - Current Stock: {current_stock}\n"
                    )
            # item numbers only
            elif return_format == 3:
                if in_stock_only:
                    if get_quantity_available(item[0], buffered=True) > 0:
                        result.append(item[0])
                else:
                    result.append(item[0])
            counter += 1
        return result
    else:
        if return_format == 3:
            return
        else:
            return f'\n<p>Top Items by {mode.title()} - No Data Today</p>'


def get_top_categories_by_sales(start_date, end_date, number_of_categories):
    query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
    'SELECT distinct IM_CATEG_COD.CATEG_COD as GRP_ID, IM_CATEG_COD.DESCR as GRP_DESCR
    FROM IM_CATEG_COD
    where ( (1=1) )
    union
    select distinct VI_PS_TKT_HIST_LIN.CATEG_COD as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    where not exists(
    select 1 from IM_CATEG_COD
    where IM_CATEG_COD.CATEG_COD = VI_PS_TKT_HIST_LIN.CATEG_COD) and ((VI_PS_TKT_HIST.STR_ID = ''1'')) and
    ( (1=1) ) and %ANYPERIODFILTER%', 'select VI_PS_TKT_HIST_LIN.CATEG_COD as GRP_ID, %HISTCOLUMNS%
    from %HISTORY% where ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) ) and %PERIODFILTER%', '
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date:%Y-%m-%d}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{end_date:%Y-%m-%d}'')', '
    (1=0) ', ' (1=0) ', {number_of_categories}, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """
    result = db.query(query)
    return result


def get_sales_rep_report(dates: Dates):
    query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1 'select distinct SY_USR.USR_ID as GRP_ID, SY_USR.NAM as GRP_DESCR
    from SY_USR where ( (1=1) ) and IS_SLS_REP = ''Y''
    union
    select distinct VI_PS_TKT_HIST_LIN.SLS_REP as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    where not exists(select 1 from SY_USR where SY_USR.USR_ID = VI_PS_TKT_HIST_LIN.SLS_REP and
    SY_USR.IS_SLS_REP = ''Y'') and ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) ) and
    %ANYPERIODFILTER%',
    'select VI_PS_TKT_HIST_LIN.SLS_REP as GRP_ID, %HISTCOLUMNS%
    from %HISTORY%
    where ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) ) and %PERIODFILTER%',
    ' (VI_PS_TKT_HIST.POST_DAT >= ''{dates.last_week_start}'') and
    (VI_PS_TKT_HIST.POST_DAT <= ''{dates.last_week_end}'')', ' (1=0) ', ' (1=0) ', 10, 0,
    'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """
    sales_rep_data = db.query(query)
    result = ''
    counter = 1
    if sales_rep_data is not None:
        for rep in sales_rep_data:
            name = rep[1]
            tickets = int(rep[3])
            revenue = round(float(rep[5]), 2)
            result += f"<p>{("#" + str(counter) + ": " + name + " - " + "Tickets: " + str(tickets) + " - " +
                             "Revenue: $" + str("{:,}".format(revenue)))}</p>"
            counter += 1
        return result
    else:
        return 'No Sales Rep Data'


def wholesale_total(start_date, stop_date, number=10):
    """Creates a list of top customer_tools (by sales) within a given time frame. Sorted by revenue."""
    query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
    'select distinct AR_CUST.CUST_NO as GRP_ID, AR_CUST.NAM as GRP_DESCR
    from AR_CUST
    where ((AR_CUST.CUST_NO <> ''CASH'' and AR_CUST.CATEG_COD = ''WHOLESALE''))
    union
    select distinct VI_PS_TKT_HIST.CUST_NO as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    where not exists(
    select 1 from AR_CUST where AR_CUST.CUST_NO = VI_PS_TKT_HIST.CUST_NO) and ((VI_PS_TKT_HIST.STR_ID = ''1'')) and
    ( (1=1) ) and %ANYPERIODFILTER%',
    'select VI_PS_TKT_HIST.CUST_NO as GRP_ID, %HISTCOLUMNS%
    from %HISTORY%
    where ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) ) and %PERIODFILTER%', '
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{stop_date}'')', '
    (1=0) ', ' (1=0) ', {number}, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2"""
    results = db.query(query)
    top_10_sum = 0
    sum = 0
    counter = 1
    if results is not None:
        for customer in results:
            sale_total = float(customer[2])
            sum += sale_total
            if counter <= 10:
                top_10_sum += sale_total
            counter += 1
        return top_10_sum, sum


def top_customer_report(start_date, stop_date, category, number=10):
    """Creates a list of top customer_tools (by sales) within a given time frame. Sorted by revenue."""
    query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
    'select distinct AR_CUST.CUST_NO as GRP_ID, AR_CUST.NAM as GRP_DESCR
    from AR_CUST
    where ((AR_CUST.CUST_NO <> ''CASH'' and AR_CUST.CATEG_COD = ''{category}''))
    union
    select distinct VI_PS_TKT_HIST.CUST_NO as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    where not exists(
    select 1 from AR_CUST where AR_CUST.CUST_NO = VI_PS_TKT_HIST.CUST_NO) and ((VI_PS_TKT_HIST.STR_ID = ''1'')) and
    ( (1=1) ) and %ANYPERIODFILTER%',
    'select VI_PS_TKT_HIST.CUST_NO as GRP_ID, %HISTCOLUMNS%
    from %HISTORY%
    where ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) ) and %PERIODFILTER%', '
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{stop_date}'')', '
    (1=0) ', ' (1=0) ', {number}, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2"""
    results = db.query(query)
    top_customer_id_list = []
    if results is not None:
        for customer in results:
            cust_no = customer[0]
            sale_total = float(customer[2])
            tickets = customer[3]
            customer_data = f"""
            select FST_NAM, LST_NAM, NAM, PHONE_1, EMAIL_ADRS_1
            FROM AR_CUST
            WHERE CUST_NO = '{cust_no}'
            """
            customer_details = db.query(customer_data)
            if customer_details is not None:
                first_name = customer_details[0][0]
                last_name = customer_details[0][1]
                full_name = customer_details[0][2]
                phone = customer_details[0][3]
                email = customer_details[0][4]
                top_customer_id_list.append(
                    [cust_no, first_name, last_name, full_name, sale_total, tickets, phone, email]
                )
            else:
                continue
        counter = 1
        result = ''
        if category == 'WHOLESALE':
            for x in top_customer_id_list:
                result += (
                    f"<p>#{counter}: {x[3]}, Tickets: {x[5]}, Revenue: ${"{:,}".format(x[4])}</p>"
                    f"<p>Name: {x[1]} {x[2]} Phone: {x[6]},  Email: {x[7]}</p><br>"
                )
                counter += 1
        else:
            for x in top_customer_id_list:
                result += (
                    f"<p>#{counter}: {x[3]}, Tickets: {x[5]}, Revenue: {"{:,}".format(x[4])}</p>"
                    f"<p>Phone: {x[6]},  Email: {x[7]}</p>"
                )
                counter += 1
        return result
    else:
        return (
            f'<p>No customer data available for {category.lower()} customer_tools from '
            f'{start_date:{Dates.date_format}} - {stop_date:{Dates.date_format}}.</p>'
        )


def get_missing_variant_names():
    from product_tools.products import get_binding_ids, get_variant_names

    binding_ids = get_binding_ids()
    for x in binding_ids:
        for y in get_variant_names(x):
            if y[1] is None:
                print(f'Item {y[0]} (Binding ID {x}) is missing its variant name.')


def get_missing_image_list_OLD():
    from product_tools.products import Product

    """Returns a sorted list of e-commerce items with missing images"""
    # all_e_comm_items = get_ecomm_items(mode=2)
    all_ecomm_items_with_stock = get_ecomm_items_with_stock()
    all_current_photos = get_list_of_current_photo_sku()
    missing_image_list_child = []
    missing_image_list_parent = []
    check_list = []

    if all_ecomm_items_with_stock is not None:
        if all_current_photos is not None:
            for x in all_ecomm_items_with_stock:
                # Construct objects to get item details
                item = Product(x)
                item_number = str(item.item_no).lower()  # lower case for comparison
                binding_key = item.binding_key
                if item_number not in all_current_photos:
                    # Add item objects to either list
                    missing_image_list_child.append(item)
                if binding_key != '' and binding_key not in all_current_photos and binding_key not in check_list:
                    missing_image_list_parent.append(item)
                    check_list.append(binding_key)
                else:
                    continue

        else:
            return 'No Pictures in ItemImages Folder'
    else:
        return '\n<p>No E-Comm Items</p>'

    # Section Header
    contents = f'\n<p><u>Total products with no image</u>: <b>{len(missing_image_list_child)}</b></p>'

    # Get Missing Child Images
    if len(missing_image_list_child) > 0:
        for item in missing_image_list_child:
            # Single Items
            if item.variant_name is None:
                if item.web_title is None:
                    contents += (
                        f'\n<p>{item.item_no}, Missing Web Title, Current Stock: {item.quantity_available}</p>'
                    )
                else:
                    # contents += f'\n<p>{item.item_no}, <a href="{item.item_url}">{item.web_title}</a>, Current Stock: {item.quantity_available}</p>'
                    contents += (
                        f'\n<p>{item.item_no}, {item.web_title}, Current Stock: {item.quantity_available}</p>'
                    )
            # Parent Items
            else:
                if item.web_title is None:
                    contents += (
                        f'\n<p>{item.item_no}, Missing Web Title, '
                        f'{item.variant_name}, Current Stock: {item.buffered_quantity_available}</p>'
                    )
                else:
                    # contents += (f'\n<p>{item.item_no}, <a href="{item.item_url}">{item.web_title}, '
                    #              f'{item.variant_name}</a>, Current Stock: {item.quantity_available}</p>')
                    contents += (
                        f'\n<p>{item.item_no}, {item.web_title}, '
                        f'{item.variant_name}, Current Stock: {item.quantity_available}</p>'
                    )
    else:
        contents += '\n<p>No Missing Images for Products</p>'

    # Get Missing Binding ID Images
    if len(missing_image_list_parent) > 0:
        # Section Header
        contents += (
            f'\n<p><br><u>Total binding IDs with no image</u>: <b>{len(missing_image_list_parent)}\n</b></p>'
        )
        for item in missing_image_list_parent:
            if item.web_title is None:
                contents += (
                    f'\n<p>{item.binding_key}, Missing Web Title, '
                    f'{item.variant_name}, Current Stock: {item.buffered_quantity_available}</p>'
                )
            else:
                contents += f'\n<p>{item.binding_key}, <a href="{item.item_url}">{item.web_title}</a></p>'

        return contents
    else:
        contents += '\n<p>No Missing Images for Binding IDs</p>'
        return contents


def get_missing_image_list() -> list[dict]:
    """Returns a list of items that have no image."""
    query = """
    SELECT img.ITEM_NO, item.ADDL_DESCR_1, item.QTY_AVAIL-item.PROF_NO_1 as 'BUFFERED_QTY' 
    FROM SN_SHOP_IMAGES img
    INNER JOIN VI_IM_ITEM_WITH_INV item on img.ITEM_NO = item.ITEM_NO
    WHERE IMAGE_NAME = 'coming-soon.jpg'
    ORDER BY BUFFERED_QTY DESC"""

    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['ADDL_DESCR_1']
            qty = int(item['BUFFERED_QTY'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def get_negative_items() -> list[dict]:
    """Creates a list of items that have negative stock values"""
    query = """
    SELECT ITEM_NO, LONG_DESCR, QTY_AVAIL
    FROM VI_IM_ITEM_WITH_INV
    WHERE QTY_AVAIL < 0
    ORDER BY QTY_AVAIL ASC
    """
    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['LONG_DESCR']
            qty = int(item['QTY_AVAIL'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def get_items_with_no_ecomm_category() -> list[dict]:
    """Creates a list of items that have no e-commerce category."""
    query = """
    SELECT item.ITEM_NO, item.ADDL_DESCR_1, inv.QTY_AVAIL
    FROM im_item item
    INNER JOIN im_inv inv
    ON item.ITEM_NO=inv.item_no
    LEFT JOIN EC_CATEG_ITEM ecomm
    ON item.ITEM_NO=ecomm.ITEM_NO
    WHERE item.IS_ECOMM_ITEM = 'Y' AND ecomm.CATEG_ID IS NULL
    ORDER BY inv.QTY_AVAIL DESC
    """
    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['ADDL_DESCR_1']
            qty = int(item['QTY_AVAIL'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def get_low_stock_items(number_of_items, dates: Dates):
    """Creates a sorted list of items with low stock. Sorted from the greatest revenue generated during a similar
    time period last year."""
    top_items_query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
    'select distinct IM_ITEM.ITEM_NO as GRP_ID, IM_ITEM.DESCR as GRP_DESCR
    from IM_ITEM
    INNER JOIN IM_INV on IM_ITEM.ITEM_NO = IM_INV.ITEM_NO where ( (1=1) )
    union
    select distinct VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    WHERE NOT EXISTS(select 1 from IM_ITEM where IM_ITEM.ITEM_NO = VI_PS_TKT_HIST_LIN.ITEM_NO) and
    ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) )
    and %ANYPERIODFILTER%',
    'select VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, %HISTCOLUMNS%
    from %HISTORY%
    where ( (1=1) ) and %PERIODFILTER%', ' (VI_PS_TKT_HIST.POST_DAT >= ''{dates.one_year_ago}'') and
    (VI_PS_TKT_HIST.POST_DAT <= ''{dates.last_year_forecast}'')', ' (1=0) ', ' (1=0) ', 500, 0,
    'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """
    top_items = db.query(top_items_query)
    top_items_list = []
    if top_items is not None:
        for item in top_items:
            sku = item[0]
            name = item[1]
            revenue = round(float(item[2]), 2)
            units_sold = int(item[4])
            top_items_list.append([sku, name, revenue, units_sold])
    else:
        return 'No Top Items'
    # Get Items with Low Stock
    low_stock_query = """
    SELECT item.ITEM_NO, inv.QTY_AVAIL
    FROM IM_ITEM item
    INNER JOIN IM_INV inv
    ON item.ITEM_NO = inv.ITEM_NO
    WHERE inv.QTY_AVAIL <= 50
    """
    items_with_low_stock = db.query(low_stock_query)
    low_stock_list = []
    low_stock_list_with_qty = []
    if items_with_low_stock is not None:
        for x in items_with_low_stock:
            low_stock_list.append(x[0])
            low_stock_list_with_qty.append([x[0], int(x[1])])

        # print(low_stock_list_with_qty)
        result_list = []

        for x in top_items_list:
            for item in low_stock_list_with_qty:
                if item[0] == x[0]:
                    # print(item)
                    x.append(item[1])
                    result_list.append(x)
        result = ''
        counter = 1
        for item in result_list[0:number_of_items]:
            result += (
                f'\n<p>#{counter}: {item[0]}, {item[1]}, Rev: ${round(item[2] / 1000, 1)}K, '
                f'Sold: {item[3]}, Current Stock: {item[4]}</p>'
            )
            counter += 1
        return result
    else:
        return '\n<p>No Low-Stock Items</p>'


def get_low_stock_items_NOT_IMPLEMENTED(number_of_items, dates: Dates) -> list[dict]:
    """Creates a sorted list of items with low stock. Sorted from the greatest revenue generated during a similar
    time period last year."""
    result = []
    top_items_query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
    'select distinct IM_ITEM.ITEM_NO as GRP_ID, IM_ITEM.DESCR as GRP_DESCR
    from IM_ITEM
    INNER JOIN IM_INV on IM_ITEM.ITEM_NO = IM_INV.ITEM_NO where ( (1=1) )
    union
    select distinct VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, NULL as GRP_DESCR
    from %HISTORY%
    WHERE NOT EXISTS(select 1 from IM_ITEM where IM_ITEM.ITEM_NO = VI_PS_TKT_HIST_LIN.ITEM_NO) and
    ((VI_PS_TKT_HIST.STR_ID = ''1'')) and ( (1=1) )
    and %ANYPERIODFILTER%',
    'select VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, %HISTCOLUMNS%
    from %HISTORY%
    where ( (1=1) ) and %PERIODFILTER%', ' (VI_PS_TKT_HIST.POST_DAT >= ''{dates.one_year_ago}'') and
    (VI_PS_TKT_HIST.POST_DAT <= ''{dates.last_year_forecast}'')', ' (1=0) ', ' (1=0) ', 500, 0,
    'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """
    top_items = db.query(top_items_query)
    top_items_list = []
    if top_items is not None:
        for item in top_items:
            sku = item[0]
            name = item[1]
            revenue = round(float(item[2]), 2)
            units_sold = int(item[4])
            top_items_list.append([sku, name, revenue, units_sold])
    else:
        return 'No Top Items'
    # Get Items with Low Stock
    low_stock_query = """
    SELECT item.ITEM_NO, inv.QTY_AVAIL
    FROM IM_ITEM item
    INNER JOIN IM_INV inv
    ON item.ITEM_NO = inv.ITEM_NO
    WHERE inv.QTY_AVAIL <= 50
    """
    items_with_low_stock = db.query(low_stock_query)
    low_stock_list = []
    low_stock_list_with_qty = []
    if items_with_low_stock is not None:
        for x in items_with_low_stock:
            low_stock_list.append(x[0])
            low_stock_list_with_qty.append([x[0], int(x[1])])

        # print(low_stock_list_with_qty)
        target_items = []

        for x in top_items_list:
            for item in low_stock_list_with_qty:
                if item[0] == x[0]:
                    # print(item)
                    x.append(item[1])
                    target_items.append(x)
        for item in target_items[0:number_of_items]:
            result.append(
                {"sku": item[0], "name": item[1], "revenue": round(item[2] / 1000, 1), "sold": item[3], "qty": item[4]}
                )
    
    return result

def get_non_web_enabled_items() -> list[dict]:
    """Creates a list of items with positive buffered stock that are not e-commerce enabled for review by staff.
    """
    query = """
    SELECT ITEM_NO, ADDL_DESCR_1, (QTY_AVAIL - PROF_NO_1) as BUFFERED_QTY
    FROM VI_IM_ITEM_WITH_INV
    WHERE IS_ECOMM_ITEM = 'N' AND (QTY_AVAIL - PROF_NO_1 > 0)
    ORDER BY BUFFERED_QTY DESC
    """
    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['ADDL_DESCR_1']
            qty = int(item['BUFFERED_QTY'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def get_inactive_items_with_stock() -> list[dict]:
    """Creates a list of items with a positive stock amount that are marked 'inactive' for review by staff."""
    query = """
    SELECT ITEM_NO, LONG_DESCR, QTY_AVAIL
    FROM VI_IM_ITEM_WITH_INV
    WHERE STAT = 'V' and QTY_AVAIL > 0
    ORDER BY QTY_AVAIL DESC
    """
    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['LONG_DESCR']
            qty = int(item['QTY_AVAIL'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def get_all_ecommerce_items():
    query = """
        SELECT IM_ITEM.ITEM_NO
        FROM IM_ITEM
        INNER JOIN IM_INV ON IM_ITEM.ITEM_NO = IM_INV.ITEM_NO
        WHERE IM_ITEM.STAT = 'A' and IM_ITEM.IS_ECOMM_ITEM = 'Y' AND
        IM_INV.QTY_AVAIL > 0 AND ADDL_DESCR_1 != 'EXCLUDE'
        ORDER BY IM_INV.QTY_AVAIL DESC
        """
    response = db.query(query)
    if response is not None:
        all_ecommerce_items = []
        for x in response:
            all_ecommerce_items.append(x[0])
        return all_ecommerce_items


def get_item_descriptions():
    query = """
    SELECT ITEM_NO, HTML_DESCR
    FROM EC_ITEM_DESCR
    """
    response = db.query(query)
    if response is not None:
        item_descriptions = {}
        for x in response:
            item_number = x[0]
            description = x[1]
            item_descriptions[item_number] = description
        return item_descriptions


def get_missing_item_descriptions(min_length) -> list[dict]:
    """Takes in a minimum character length and returns a list of items with descriptions less than that length."""
    query = f"""
    SELECT item.ITEM_NO, item.ADDL_DESCR_1, (item.QTY_AVAIL - item.PROF_NO_1) as 'BUFFERED_QTY'
    FROM EC_ITEM_DESCR ec
    RIGHT JOIN VI_IM_ITEM_WITH_INV item on ec.ITEM_NO = item.ITEM_NO
    WHERE item.IS_ECOMM_ITEM = 'Y' AND
    (item.QTY_AVAIL - item.PROF_NO_1 > 0) AND (DATALENGTH(HTML_DESCR) IS NULL
    or DATALENGTH(HTML_DESCR) < {min_length}) AND (item.IS_ADM_TKT = 'Y' OR item.USR_PROF_ALPHA_16 IS NULL)
    ORDER BY item.QTY_AVAIL DESC"""

    response = db.query(query, mapped=True)
    result = []
    if response['code'] == 200:
        for item in response['data']:
            sku = item['ITEM_NO']
            name = item['ADDL_DESCR_1']
            qty = int(item['BUFFERED_QTY'])
            result.append({"sku": sku, "name": name, "qty": qty})
    
    return result


def report_generator(
    dates: Dates,
    revenue=False,
    last_week_report=False,
    mtd_month_report=False,
    last_year_mtd_report=False,
    forecasting_report=False,
    top_items_by_category=False,
    low_stock_items_report=False,
    sales_rep_report=False,
    wholesale_report=False,
    cogs_report=False,
    year_to_date=False,
    title='Administrative Report',
):
    """Produces Text for Email Report"""

    # Title
    report = f'\n<h1><strong>{title}</strong></h1>\n' f'\n<h3>{datetime.now():%A %B %d, %Y}</h3>'

    if revenue:
        section_header = 'Revenue Report'
        report += f'\n<h2><strong>{section_header}</strong></h2>'
        # YESTERDAY TOTAL REVENUE
        # for all days but Monday: Give yesterday's total
        day_of_week = datetime.today().isoweekday()
        try:
            if day_of_week > 1:
                report += "\n<h4><strong>Yesterday's Total Revenue</strong></h4>"
                report += revenue_sales_report(
                    start_date=dates.yesterday, stop_date=dates.yesterday, split=False, anna_mode=True
                )

            # For Monday. This will send Saturday's data instead of closed day, Sunday.
            elif day_of_week == 1:
                saturday = dates.yesterday + relativedelta(days=-1)
                report += "\n<h4><strong>Saturday's Total Revenue</strong></h4>"
                report += revenue_sales_report(start_date=saturday, stop_date=saturday, split=False, anna_mode=True)
        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

        # CURRENT MONTH TOTAL REVENUE
        try:
            section_header = f'\n<h4><strong>{dates.month_start:%B} Revenue</strong></h4>'
            report += section_header
            for x in range(dates.years_to_show):
                report += revenue_sales_report(
                    start_date=dates.month_start + relativedelta(years=(x * -1)),
                    stop_date=dates.month_end + relativedelta(years=(x * -1)),
                    split=False,
                    short=True,
                )
        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

        # LAST MONTH TOTAL REVENUE
        try:
            section_header = f'\n<h4><strong>{dates.last_month_start:%B} Total Revenue</strong></h4>'
            report += section_header
            for x in range(dates.years_to_show):
                report += revenue_sales_report(
                    start_date=dates.last_month_start + relativedelta(years=(x * -1)),
                    stop_date=dates.last_month_end + relativedelta(years=(x * -1)),
                    split=False,
                    short=True,
                )
        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

        # report += "\n<h4><strong>Last Month Total</strong></h4>"
        # report += f"\n{revenue_sales_report(last_month_start, last_month_end, split=False, short=True)}"

        # Add Month-to-Date Revenue Data
        report += '<h4><strong>Month to Date</strong></h4>'
        try:
            for x in range(dates.years_to_show):
                dynamic_month_start = dates.month_start + relativedelta(years=(x * -1))
                dynamic_today = dates.today + relativedelta(years=(x * -1))

                # Create Dynamic Header
                report += f'<h5>{dynamic_month_start:%b %y}</h5>'
                # Get Data from SQL
                report += f'\n{revenue_sales_report(dynamic_month_start, dynamic_today, split=True)} \n'

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

        # Add Year-to-Date Revenue Data
        report += '<h4><strong>Year to Date</strong></h4>'
        try:
            for x in range(dates.years_to_show):
                dynamic_year_start = dates.year_start + relativedelta(years=(x * -1))
                dynamic_today = dates.today + relativedelta(years=(x * -1))
                # Create Dynamic YTD Header
                report += f'\n<h5>{dynamic_year_start:%x} - {dynamic_today:%x}</h5>'
                # Get Data
                report += f'\n{revenue_sales_report(dynamic_year_start, dynamic_today, split=True)}'

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

        # Add Last Six Weeks of Weekly Revenue (Sunday - Saturday each week)
        report += '\n<h4><strong>Weekly Revenue</strong></h4>' '\n<h5>Last Six Weeks</h5>'
        try:
            for x in range(dates.weeks_to_show):
                report += f'\n{revenue_sales_report(
                    start_date=dates.last_week_start + relativedelta(weeks=(x * -1)),
                    stop_date=dates.last_week_end + relativedelta(weeks=(x * -1)),
                    split=False, anna_mode=True)}'

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if cogs_report:
        section_header = (
            f'\n<h2><strong>Cost of Goods Sold Report</strong></h2>'
            f'\n<h5>{dates.last_week_start:{dates.date_format}} - {dates.last_week_end:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            report += f"\n<p>Store 1: ${"{:,}".format(cost_of_goods_sold(start_date=dates.last_week_start, 
                                                                         stop_date=dates.last_week_end, store="1"))}<br>"
            report += f"\nStore Web: ${"{:,}".format(cost_of_goods_sold(start_date=dates.last_week_start, 
                                                                        stop_date=dates.last_week_end, store="WEB"))}</p>"
        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if sales_rep_report:
        section_header = (
            f'\n<h2><strong>Sales Rep Report</strong></h2>'
            f'\n<h5>{dates.last_week_start:{dates.date_format}} - {dates.last_week_end:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            report += get_sales_rep_report(dates=dates)

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if wholesale_report:
        section_header = (
            f'\n<h2><strong>Wholesale Totals</strong></h2>'
            f'\n<h5>{dates.last_week_start:{dates.date_format}} - {dates.last_week_start:{dates.last_week_end}}</h5>'
        )
        report += section_header
        try:
            data = wholesale_total(dates.last_week_start, dates.last_week_end, 1000)
            report += f'<p>Revenue for Top 10 Wholesale Customers: ${data[0]}</p>'
            report += f'<p>Revenue for ALL Wholesale Customers: ${data[1]}</p>'

            section_header = (
                f'\n<h2><strong>Top Wholesale Customers</strong></h2>'
                f'\n<h5>{dates.last_week_start:{dates.date_format}} - {dates.last_week_start:{dates.last_week_end}}</h5>'
            )
            report += section_header
            report += top_customer_report(
                dates.last_week_start, dates.last_week_end, category='WHOLESALE', number=10
            )
        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if last_week_report:
        section_header = (
            f'\n<h2><strong>Last Week Report</strong></h2>'
            f'\n<h5>{dates.last_week_start:{dates.date_format}} - {dates.last_week_end:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            forecast_top_10_by_sales = create_top_items_report(dates.last_week_start, dates.last_week_end, 'sales')
            forecast_top_10_by_quantity = create_top_items_report(
                dates.last_week_start, dates.last_week_end, 'quantity'
            )
            report += forecast_top_10_by_sales
            report += forecast_top_10_by_quantity

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if mtd_month_report:
        section_header = (
            f'\n<h2><strong>Month to Date Report</strong></h2>'
            f'\n<h5>{dates.month_start:{dates.date_format}} - {dates.today:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            forecast_top_10_by_sales = create_top_items_report(dates.month_start, dates.today, 'sales')
            forecast_top_10_by_quantity = create_top_items_report(dates.month_start, dates.today, 'quantity')
            report += forecast_top_10_by_sales
            report += forecast_top_10_by_quantity

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if last_year_mtd_report:
        section_header = (
            f'\n<h2><strong>Last Year Month to Date Report</strong></h2>'
            f'\n<h5>{dates.month_start_last_year:{dates.date_format}} - {dates.one_year_ago:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            forecast_top_10_by_sales = create_top_items_report(
                dates.month_start_last_year, dates.one_year_ago, 'sales'
            )
            forecast_top_10_by_quantity = create_top_items_report(
                dates.month_start_last_year, dates.one_year_ago, 'quantity'
            )
            report += forecast_top_10_by_sales
            report += forecast_top_10_by_quantity

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if forecasting_report:
        section_header = (
            f'\n<h2><strong>{dates.forecast_days} Days Forecasting Report</strong></h2>'
            f'\n<h5>{dates.one_year_ago:{dates.date_format}} - {dates.last_year_forecast:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            forecast_top_10_by_sales = create_top_items_report(
                dates.one_year_ago, dates.last_year_forecast, 'sales'
            )
            forecast_top_10_by_quantity = create_top_items_report(
                dates.one_year_ago, dates.last_year_forecast, 'quantity'
            )
            report += forecast_top_10_by_sales
            report += forecast_top_10_by_quantity

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if low_stock_items_report:
        number_of_low_stock_items = 100
        section_header = (
            f'\n<h2><strong>Top {number_of_low_stock_items} Revenue Items with Low Stock</strong></h2>'
            f'\n<h5>{dates.one_year_ago:{dates.date_format}} - {dates.last_year_low_stock_window:{dates.date_format}}</h5>'
        )
        report += section_header
        try:
            report += get_low_stock_items(dates=dates, number_of_items=number_of_low_stock_items)

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if top_items_by_category:
        section_header = '\n<h2><strong>Last Week Top Items by Top Categories</strong></h2>'
        report += section_header
        category_list = []
        # Get Top Performing Categories by Revenue. Returns list of tuples ('TREES', '$3500')
        try:
            for items in get_top_categories_by_sales(dates.last_week_start, dates.last_week_end, 10):
                category_list.append((items[0], '$' + str('{:,}'.format(items[2]))))
            counter = 1
            for x in category_list:
                report += (
                    f'\n<p class="rank"><strong>Category Rank #{counter}: {x[0]}</strong><br>'
                    f'Total Revenue: {x[1]} from {dates.last_week_start:{dates.date_format}} - '
                    f'{dates.last_week_end:{dates.date_format}}<br></p>'
                )
                report += create_top_items_report(
                    dates.last_week_start, dates.last_week_end, number_of_items=10, mode='sales', category=x[0]
                )
                report += create_top_items_report(
                    dates.last_week_start, dates.last_week_end, number_of_items=10, mode='quantity', category=x[0]
                )
                counter += 1

        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    if year_to_date:
        try:
            for x in range(90):
                report += f'\n{revenue_sales_report(
                    start_date=dates.today + relativedelta(days=(x * -1)),
                    stop_date=dates.today + relativedelta(days=(x * -1)),
                    split=False, anna_mode=True)}'
        except Exception as err:
            report += f'<p>Error! Message: {err}</p>'

    return report


if __name__ == '__main__':
    print(get_missing_item_descriptions(10))
