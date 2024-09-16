import datetime
import os

from product_tools.products import get_ecomm_items_with_stock
from setup import creds
from setup import email_engine
from setup.admin_report_html import boiler_plate, css, body_start, body_end
from setup.date_presets import *
from database import Database as db


def get_quantity_available(item):
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


def revenue(start_date, stop_date):
    query = f"""
        "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
        'select distinct PS_STR.STR_ID as GRP_ID, PS_STR.DESCR as GRP_DESCR
        from PS_STR
        where ( (1=1) )
        union
        select distinct VI_PS_TKT_HIST.STR_ID as GRP_ID, NULL as GRP_DESCR
        from %HISTORY%
        where not exists(select 1 from PS_STR where PS_STR.STR_ID = VI_PS_TKT_HIST.STR_ID) and 
        ( (1=1) ) and ( (1=1) ) and %ANYPERIODFILTER%', 'select VI_PS_TKT_HIST.STR_ID as GRP_ID, 
        %HISTCOLUMNS% from %HISTORY% where ( (1=1) ) and ( (1=1) ) and %PERIODFILTER%', ' 
        (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{stop_date}'')', '
        (1=0) ', ' (1=0) ', 0, 0, 'GRP_ID', 2
        """
    response = db.query(query)
    if response is not None:
        result = {}
        for store in response:
            store_name = store[0]

            ticket_count = int(store[3])
            # Gross Sales
            sales = store[5]

            # Get Return Details
            valid_returns_amt = store[9]
            nonvalid_returns_amt = store[10]
            valid_returns_qty = store[7]
            nonvalid_returns_qty = store[8]
            cost = float(store[13])
            returns_qty = int(valid_returns_qty + nonvalid_returns_qty)

            # Subtract Returns
            extended_total = float(sales - (valid_returns_amt + nonvalid_returns_amt))

            result.update(
                {
                    store_name: {
                        'revenue': extended_total,
                        'tickets': ticket_count,
                        'returns': returns_qty,
                        'cost': cost,
                    }
                }
            )
        return result


def revenue_sales_report(start_date, stop_date, split=True, anna_mode=False, short=False):
    query = f"""
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
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
                date_prefix = f"\n<p>{datetime.strptime(start_date[0:10], '%Y-%m-%d').strftime(date_format)}<br>"
            else:
                date_prefix = (
                    f"\n<p>{datetime.strptime(start_date[0:10], '%Y-%m-%d').strftime(date_format)} - "
                    f"{datetime.strptime(stop_date[0:10], '%Y-%m-%d').strftime(date_format)}:<br>"
                )

            return (
                date_prefix + f"In-Store Sales: ${"{:,}".format(retail_sales - retail_valid_returns
                                                      - retail_nonvalid_returns)}<br>"
                f"E-Comm Sales: ${"{:,}".format(web_sales - web_valid_returns - web_nonvalid_returns)}<br>"
                f"Total: ${"{:,}".format((retail_sales + web_sales) -
                                             (web_valid_returns + web_nonvalid_returns) -
                                             (retail_valid_returns + retail_nonvalid_returns))}<br><br></p>"
            )

        # for reports with combined reporting
        else:
            if short:
                return (
                    f"\n<p>{datetime.strptime(start_date[0:10], '%Y-%m-%d').strftime("%B %Y")}: "
                    f"${"{:,}".format((retail_sales + web_sales) -
                                          (web_valid_returns + web_nonvalid_returns) -
                                          (retail_valid_returns + retail_nonvalid_returns))}</p>"
                )
            else:
                return (
                    f"\n<p>{datetime.strptime(start_date[0:10], '%Y-%m-%d').strftime(date_format)} - "
                    f"{datetime.strptime(stop_date[0:10], '%Y-%m-%d').strftime(date_format)}: "
                    f"${"{:,}".format((retail_sales + web_sales) -
                                          (web_valid_returns + web_nonvalid_returns) -
                                          (retail_valid_returns + retail_nonvalid_returns))}</p>"
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

    contents += f"\n<p>Total Retail Tickets: <b>{data['RETAIL']}</b></br>"
    contents += f"\nTotal Wholesale Tickets: <b>{data['WHOLESALE']}</b></br>"
    contents += f"\nTotal Tickets: <b>{data['WHOLESALE'] + data['RETAIL']}</b></p>"
    return contents


def get_list_of_current_photo_sku():
    """Returns a sorted list of unique filenames from ItemImages Folder"""
    list_of_files = os.listdir(creds.Company.product_images)
    list_of_sku = []
    for item in list_of_files[1:]:
        name = item.split('.')[0]
        sku = name.split('^')[0]
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
        "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
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
                result.append(item[0])
            counter += 1
        return result
    else:
        return f'\n<p>Top Items by {mode.title()} - No Data Today</p>'


def get_top_categories_by_sales(start, end, number_of_categories):
    query = f"""
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
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
    (VI_PS_TKT_HIST.POST_DAT >= ''{start}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{end}'')', '
    (1=0) ', ' (1=0) ', {number_of_categories}, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """
    result = db.query(query)
    return result


def get_sales_rep_report():
    query = f"""
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1 'select distinct SY_USR.USR_ID as GRP_ID, SY_USR.NAM as GRP_DESCR
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
    ' (VI_PS_TKT_HIST.POST_DAT >= ''{last_week_start}'') and
    (VI_PS_TKT_HIST.POST_DAT <= ''{last_week_end}'')', ' (1=0) ', ' (1=0) ', 10, 0,
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
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
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
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
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
            f'{reformat_time(start_date)} - {reformat_time(stop_date)}.</p>'
        )


def get_missing_variant_names():
    from product_tools.products import get_binding_ids, get_variant_names

    binding_ids = get_binding_ids()
    for x in binding_ids:
        for y in get_variant_names(x):
            if y[1] is None:
                print(f'Item {y[0]} (Binding ID {x}) is missing its variant name.')


def get_missing_image_list():
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
                item_number = item.item_no
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


def get_negative_items():
    """Creates a list of items that have negative stock values"""
    query = """
    SELECT IM_ITEM.ITEM_NO, IM_ITEM.ADDL_DESCR_1, IM_ITEM.USR_PROF_ALPHA_17, IM_INV.QTY_AVAIL
    FROM IM_ITEM
    INNER JOIN IM_INV ON IM_ITEM.ITEM_NO = IM_INV.ITEM_NO
    WHERE IM_INV.QTY_AVAIL < 0
    ORDER BY IM_INV.QTY_AVAIL ASC
    """
    response = db.query(query)
    result = ''
    counter = 1
    if response is not None:
        result += f'<p><u>Total items</u>: <b>{len(response)}</b></p>'
        for item in response:
            sku = item[0]
            name = item[1]
            qty = int(item[3])
            result += f'\n<p>#{counter}: {sku}, {name}, Current Stock: {qty}</p>'
            counter += 1
        return result
    else:
        return '<p>No Items</p>'


def get_items_with_no_ecomm_category():
    """Creates a list of items that have no e-commerce category."""
    query = """
    SELECT item.ITEM_NO, item.ADDL_DESCR_1, inv.qty_avail
    FROM im_item item
    INNER JOIN im_inv inv
    ON item.ITEM_NO=inv.item_no
    LEFT JOIN EC_CATEG_ITEM ecomm
    ON item.ITEM_NO=ecomm.ITEM_NO
    WHERE ecomm.CATEG_ID IS NULL AND
    STAT = 'A' AND
    ADDL_DESCR_1 != 'EXCLUDE' AND
    item.CATEG_COD != 'services'
    ORDER BY inv.QTY_AVAIL DESC
    """
    response = db.query(query)
    result = ''
    counter = 1
    if response is not None:
        result += f'<p><u>Total items</u>: <b>{len(response)}</b></p>'
        for item in response:
            sku = item[0]
            name = item[1]
            qty = int(item[2])
            result += f'\n<p>#{counter}: {sku}, {name}, Current Stock: {qty}</p>'
            counter += 1
        return result
    else:
        return '<p>No Items</p>'


def get_low_stock_items(number_of_items):
    """Creates a sorted list of items with low stock. Sorted from the greatest revenue generated during a similar
    time period last year."""
    top_items_query = f"""
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1
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
    where ( (1=1) ) and %PERIODFILTER%', ' (VI_PS_TKT_HIST.POST_DAT >= ''{one_year_ago}'') and
    (VI_PS_TKT_HIST.POST_DAT <= ''{last_year_forecast}'')', ' (1=0) ', ' (1=0) ', 500, 0,
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


def get_non_ecomm_enabled_items():
    """Creates a list of items with positive stock that are not e-commerce enabled for review by staff"""
    query = """
    SELECT item.item_no, item.ADDL_DESCR_1, inv.qty_avail
    FROM im_item item
    inner join IM_INV inv
    ON item.ITEM_NO=inv.ITEM_NO
    LEFT JOIN EC_CATEG_ITEM ecomm
    ON item.ITEM_NO=ecomm.ITEM_NO
    WHERE STAT = 'A' AND
    item.CATEG_COD != 'SERVICES' AND
    inv.QTY_AVAIL > 0 AND
    ADDL_DESCR_1 != 'EXCLUDE' AND
    IS_ECOMM_ITEM = 'N'
    ORDER BY inv.QTY_AVAIL DESC
    """
    response = db.query(query)
    result = ''
    counter = 1
    if response is not None:
        result += f'<p><u>Total items</u>: <b>{len(response)}</b></p>'
        for item in response:
            sku = item[0]
            name = item[1]
            qty = int(item[2])
            result += f'<p>#{counter}: {sku}, {name}, In-Stock: {qty}</p>'
            counter += 1
        return result
    else:
        return '<p>No Items</p>'


def get_inactive_items_with_stock():
    """Creates a list of items with a positive stock amount that are marked 'inactive' for review by staff."""
    query = """
    SELECT item.ITEM_NO, item.DESCR, QTY_AVAIL
    FROM IM_ITEM item
    INNER JOIN IM_INV inv on item.ITEM_NO = inv.ITEM_NO
    WHERE STAT = 'V' and QTY_AVAIL > 0
    ORDER BY QTY_AVAIL DESC
    """
    response = db.query(query)
    result = ''
    count = 1
    if response is not None:
        # Header
        result += f'\n<p><u>Total items</u>: <b>{len(response)}</b></p>'
        # Contents
        for x in response:
            result += f'\n<p>#{count}: {x[0]}, {x[1]}, Stock: {int(x[2])}</p>'
            count += 1
        return result
    else:
        return '\n<p>No Inactive Items with Stock</p>'


def reformat_time(time_string):
    """Re-formats from SQL friendly format to a USA friendly readable format"""
    cp_format = '%Y-%m-%d'
    return str(datetime.strptime(time_string, cp_format).strftime(date_format))


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
    # query = """
    # SELECT DATALENGTH(HTML_DESCR)
    # FROM EC_ITEM_DESCR
    # """
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


def get_missing_item_descriptions(min_length):
    from product_tools.products import Product

    query = f"""
    SELECT IM_ITEM.ITEM_NO
    FROM EC_ITEM_DESCR
    RIGHT OUTER JOIN IM_ITEM 
    ON EC_ITEM_DESCR.ITEM_NO = IM_ITEM.ITEM_NO
    INNER JOIN IM_INV
    ON IM_ITEM.ITEM_NO = IM_INV.ITEM_NO
    WHERE IM_ITEM.STAT = 'A' and IM_ITEM.IS_ECOMM_ITEM = 'Y' AND
    (IM_INV.QTY_AVAIL - IM_ITEM.PROF_NO_1 > 0) AND ADDL_DESCR_1 != 'EXCLUDE' AND (DATALENGTH(HTML_DESCR) IS NULL
    or DATALENGTH(HTML_DESCR) < {min_length}) AND (IM_ITEM.IS_ADM_TKT = 'Y' OR IM_ITEM.USR_PROF_ALPHA_16 IS NULL)
    ORDER BY IM_INV.QTY_AVAIL DESC"""

    response = db.query(query)
    if response is not None:
        result = ''
        list_size = len(response)

        # Section Header
        result += f'\n<p><u>Total products with no description</u>: <b>{list_size}</b></p>'
        counter = 1
        for x in response:
            item = Product(x[0])
            if item.buffered_quantity_available > 0:
                if item.item_url is not None:
                    print(f'{counter}/{list_size}: {item.item_no}, {item.web_title}')
                    result += (
                        f'\n<p>#{counter}: {item.item_no}, <a href="{item.item_url}">{item.web_title}</a>, '
                        f'Current Stock: {item.quantity_available}</p>'
                    )
                else:
                    result += (
                        f'\n<p>#{counter}: {item.item_no}, {item.web_title}, '
                        f'Current Stock: {item.quantity_available}</p>'
                    )
                counter += 1
        return result


def report_generator(
    revenue=False,
    last_week_report=False,
    mtd_month_report=False,
    last_year_mtd_report=False,
    forecasting_report=False,
    top_items_by_category=False,
    missing_images_report=False,
    negatives_report=False,
    ecomm_category_report=False,
    non_web_enabled_report=False,
    low_stock_items_report=False,
    sales_rep_report=False,
    wholesale_report=False,
    inactive_items_report=False,
    cogs_report=False,
    missing_descriptions_report=False,
    year_to_date=False,
    title='Administrative Report',
):
    """Produces Text for Email Report"""

    # Title
    report = f'\n<h1><strong>{title}</strong></h1>\n' f'\n<h3>{datetime.now():%A %B %d, %Y}</h3>'

    if revenue:
        section_header = 'Revenue Report'
        report += f'\n<h2><strong>{section_header}</strong></h2>'
        # for all days but Monday: Give yesterday's total
        day_of_week = datetime.today().isoweekday()
        if day_of_week > 1:
            report += "\n<h4><strong>Yesterday's Total Revenue</strong></h4>"
            report += revenue_sales_report(
                start_date=str((datetime.strptime(yesterday, '%Y-%m-%d'))),
                stop_date=str((datetime.strptime(yesterday, '%Y-%m-%d'))),
                split=False,
                anna_mode=True,
            )
        elif day_of_week == 1:
            saturday = str((datetime.strptime(yesterday, '%Y-%m-%d') + relativedelta(days=-1)))[:-9]
            report += "\n<h4><strong>Saturday's Total Revenue</strong></h4>"
            report += revenue_sales_report(
                start_date=str((datetime.strptime(saturday, '%Y-%m-%d'))),
                stop_date=str((datetime.strptime(saturday, '%Y-%m-%d'))),
                split=False,
                anna_mode=True,
            )

        # For first week of each month: add Last Month's Total Revenue with 3 year comparison
        day = datetime.now().day
        if 1 <= day <= 8:
            section_header = f"\n<h4><strong>{datetime.strptime(last_month_start, "%Y-%m-%d").strftime("%B")} Total Revenue</strong></h4>"
            report += section_header
            for x in range(years_to_show):
                report += revenue_sales_report(
                    start_date=str(
                        (datetime.strptime(last_month_start, '%Y-%m-%d') + relativedelta(years=(x * -1)))
                    ),
                    stop_date=str((datetime.strptime(last_month_end, '%Y-%m-%d') + relativedelta(years=(x * -1)))),
                    split=False,
                    short=True,
                )

        # Add Last Six Weeks of Weekly Revenue (Sunday - Saturday each week)
        report += '\n<h4><strong>Weekly Revenue</strong></h4>' '\n<h5>Last Six Weeks</h5>'
        for x in range(weeks_to_show):
            report += f"\n{revenue_sales_report(
                start_date=f"{last_week_start + relativedelta(weeks=(x * -1)):%Y-%m-%d}",
                stop_date=f"{last_week_end + relativedelta(weeks=(x * -1)):%Y-%m-%d}",
                split=False, anna_mode=True)}"

            report += get_total_tickets(
                start_day=str((datetime.strptime(last_week_start, '%Y-%m-%d') + relativedelta(weeks=(x * -1)))),
                end_day=str((datetime.strptime(last_week_end, '%Y-%m-%d') + relativedelta(weeks=(x * -1)))),
            )

        report += '\n<h4><strong>Last Month Total</strong></h4>'
        report += f'\n{revenue_sales_report(last_month_start, last_month_end, split=False, short=True)}'

        # Add Month-to-Date Revenue Data
        report += '<h4><strong>Month to Date</strong></h4>'
        for x in range(years_to_show):
            # Create Dynamic Header
            report += f"<h5>{str((datetime.strptime(month_start, "%Y-%m-%d") +
                                  relativedelta(years=(x * -1))).strftime("%b %y"))}</h5>"
            # Get Data from SQL
            if x == 0:
                report += f"""\n{(
                        revenue_sales_report(str((datetime.strptime(month_start, "%Y-%m-%d") +
                                                  relativedelta(years=(x * -1)))),

                                             str((datetime.strptime(today, "%Y-%m-%d") +
                                                  relativedelta(years=(x * -1)))),
                                             split=True) + "\n")}"""
            else:
                report += f"""\n{(
                        revenue_sales_report(str((datetime.strptime(month_start, "%Y-%m-%d") +
                                                  relativedelta(years=(x * -1)))),
                                             str((datetime.strptime(month_end, "%Y-%m-%d") +
                                                  relativedelta(years=(x * -1)))),
                                             split=True) + "\n")}"""

        # Add Year-to-Date Revenue Data
        report += '<h4><strong>Year to Date</strong></h4>'

        for x in range(years_to_show):
            # Create Dynamic YTD Header
            report += f"""\n<h5>{str((datetime.strptime(year_start, "%Y-%m-%d") +
                                      relativedelta(years=(x * -1))).strftime(date_format)) + " - " +
                                 str((datetime.strptime(today, "%Y-%m-%d") +
                                      relativedelta(years=(x * -1))).strftime(date_format))}</h5>"""
            # Get Data
            report += f"""\n{(
                revenue_sales_report(str((datetime.strptime(year_start, "%Y-%m-%d") +
                                          relativedelta(years=(x * -1)))),

                                     str((datetime.strptime(today, "%Y-%m-%d") +
                                          relativedelta(years=(x * -1)))),
                                     split=True))}"""
    if cogs_report:
        section_header = (
            f'\n<h2><strong>Cost of Goods Sold Report</strong></h2>'
            f'\n<h5>{reformat_time(last_week_start)} - {reformat_time(last_week_end)}</h5>'
        )
        report += section_header
        report += f"\n<p>Store 1: ${"{:,}".format(cost_of_goods_sold(last_week_start, last_week_end, "1"))}<br>"
        report += f"\nStore Web: ${"{:,}".format(cost_of_goods_sold(last_week_start, last_week_end, "WEB"))}</p>"

    if sales_rep_report:
        section_header = (
            f'\n<h2><strong>Sales Rep Report</strong></h2>'
            f'\n<h5>{reformat_time(last_week_start)} - {reformat_time(last_week_end)}</h5>'
        )
        report += section_header
        report += get_sales_rep_report()

    if wholesale_report:
        section_header = (
            f'\n<h2><strong>Wholesale Totals</strong></h2>'
            f'\n<h5>{reformat_time(last_week_start)} - {reformat_time(last_week_end)}</h5>'
        )
        report += section_header
        data = wholesale_total(last_week_start, last_week_end, 1000)
        report += f'<p>Revenue for Top 10 Wholesale Customers: ${data[0]}</p>'
        report += f'<p>Revenue for ALL Wholesale Customers: ${data[1]}</p>'

        section_header = (
            f'\n<h2><strong>Top Wholesale Customers</strong></h2>'
            f'\n<h5>{reformat_time(last_week_start)} - {reformat_time(last_week_end)}</h5>'
        )
        report += section_header
        report += top_customer_report(last_week_start, last_week_end, category='WHOLESALE', number=10)

    if last_week_report:
        section_header = (
            f'\n<h2><strong>Last Week Report</strong></h2>'
            f'\n<h5>{reformat_time(last_week_start)} - {reformat_time(last_week_end)}</h5>'
        )
        report += section_header
        forecast_top_10_by_sales = create_top_items_report(last_week_start, last_week_end, 'sales')
        forecast_top_10_by_quantity = create_top_items_report(last_week_start, last_week_end, 'quantity')
        report += forecast_top_10_by_sales
        report += forecast_top_10_by_quantity

    if mtd_month_report:
        section_header = (
            f'\n<h2><strong>Month to Date Report</strong></h2>'
            f'\n<h5>{reformat_time(month_start)} - {reformat_time(today)}</h5>'
        )
        report += section_header
        forecast_top_10_by_sales = create_top_items_report(month_start, today, 'sales')
        forecast_top_10_by_quantity = create_top_items_report(month_start, today, 'quantity')
        report += forecast_top_10_by_sales
        report += forecast_top_10_by_quantity

    if last_year_mtd_report:
        section_header = (
            f'\n<h2><strong>Last Year Month to Date Report</strong></h2>'
            f'\n<h5>{reformat_time(month_start_last_year)} - {reformat_time(one_year_ago)}</h5>'
        )
        report += section_header
        forecast_top_10_by_sales = create_top_items_report(month_start_last_year, one_year_ago, 'sales')
        forecast_top_10_by_quantity = create_top_items_report(month_start_last_year, one_year_ago, 'quantity')
        report += forecast_top_10_by_sales
        report += forecast_top_10_by_quantity

    if forecasting_report:
        section_header = (
            f'\n<h2><strong>{forecast_days} Days Forecasting Report</strong></h2>'
            f'\n<h5>{reformat_time(one_year_ago)} - {reformat_time(last_year_forecast)}</h5>'
        )
        report += section_header
        forecast_top_10_by_sales = create_top_items_report(one_year_ago, last_year_forecast, 'sales')
        forecast_top_10_by_quantity = create_top_items_report(one_year_ago, last_year_forecast, 'quantity')
        report += forecast_top_10_by_sales
        report += forecast_top_10_by_quantity

    if low_stock_items_report:
        section_header = (
            f'\n<h2><strong>Top {number_of_low_stock_items} Revenue Items with Low Stock</strong></h2>'
            f'\n<h5>{reformat_time(one_year_ago)} - {reformat_time(last_year_low_stock_window)}</h5>'
        )
        report += section_header
        report += get_low_stock_items(number_of_low_stock_items)

    if top_items_by_category:
        section_header = '\n<h2><strong>Last Week Top Items by Top Categories</strong></h2>'
        report += section_header
        category_list = []
        # Get Top Performing Categories by Revenue. Returns list of tuples ('TREES', '$3500')
        for items in get_top_categories_by_sales(last_week_start, last_week_end, 10):
            category_list.append((items[0], '$' + str('{:,}'.format(items[2]))))
        counter = 1
        for x in category_list:
            report += (
                f'\n<p class="rank"><strong>Category Rank #{counter}: {x[0]}</strong><br>'
                f'Total Revenue: {x[1]} from '
                f'{reformat_time(last_week_start)} - '
                f'{reformat_time(last_week_end)}<br></p>'
            )
            report += create_top_items_report(
                last_week_start, last_week_end, number_of_items=10, mode='sales', category=x[0]
            )
            report += create_top_items_report(
                last_week_start, last_week_end, number_of_items=10, mode='quantity', category=x[0]
            )
            counter += 1

    if year_to_date:
        for x in range(90):
            report += f"\n{revenue_sales_report(
                start_date=str((datetime.strptime(today, "%Y-%m-%d") + relativedelta(days=(x * -1)))),
                stop_date=str((datetime.strptime(today, "%Y-%m-%d") + relativedelta(days=(x * -1)))),
                split=False, anna_mode=True)}"

    return report
