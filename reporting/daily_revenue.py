from datetime import datetime

from jinja2 import Template

from setup import creds
from setup import date_presets
from setup import email_engine
from setup import query_engine


def get_stores():
    db = query_engine.QueryEngine()
    query = f"""
    SELECT DISTINCT STR_ID
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date_presets.yesterday}'
    """
    response = db.query_db(query)
    if response is not None:
        stores = []
        for x in response:
            stores.append(x[0])
        return stores


def get_transactions_by_pay_code(store, date):
    result = {}
    result.update({"store": store})
    # Get Cash Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'CASH' AND
    STR_ID = '{store}'
    """
    db = query_engine.QueryEngine()
    response = db.query_db(query)
    if response is not None:
        cash_revenue = float(response[0][0]) if response[0][0] else None
        cash_tickets = int(response[0][1]) if response[0][1] else None
        cash_results = {"revenue": cash_revenue, "tickets": cash_tickets}
        result.update({"cash": cash_results})

    # Get Checks Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'CHECK' AND
    STR_ID = '{store}'
    """
    db = query_engine.QueryEngine()
    response = db.query_db(query)
    if response is not None:
        check_revenue = float(response[0][0]) if response[0][0] else None
        check_tickets = int(response[0][1]) if response[0][1] else None
        check_results = {"revenue": check_revenue, "tickets": check_tickets}
        result.update({"check": check_results})

    # Get Credit Card Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD in ('BIG', 'DEBIT', 'DISCOVER', 'MC', 'VISA', 'AMEX')
    AND STR_ID = '{store}'
    """
    db = query_engine.QueryEngine()
    response = db.query_db(query)
    if response is not None:
        credit_revenue = float(response[0][0]) if response[0][0] else None
        credit_tickets = int(response[0][1]) if response[0][1] else None
        credit_results = {"revenue": credit_revenue, "tickets": credit_tickets}
        result.update({"credit": credit_results})

    # Get Gift Card Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'GIFT'
    AND STR_ID = '{store}'
    """
    db = query_engine.QueryEngine()
    response = db.query_db(query)
    if response is not None:
        gift_revenue = float(response[0][0]) if response[0][0] else None
        gift_tickets = int(response[0][1]) if response[0][1] else None
        gift_results = {"revenue": gift_revenue, "tickets": gift_tickets}
        result.update({"gift": gift_results})

    # Get LOYALTY
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'LOYALTY'
    AND STR_ID = '{store}'
    """
    db = query_engine.QueryEngine()
    response = db.query_db(query)
    if response is not None:
        loyalty_revenue = float(response[0][0]) if response[0][0] else None
        loyalty_tickets = int(response[0][1]) if response[0][1] else None
        loyalty_results = {"revenue": loyalty_revenue, "tickets": loyalty_tickets}
        result.update({"loyalty": loyalty_results})

    return result


def get_all_stores_sales_by_paycode(date):
    stores = get_stores()
    result = []
    for x in stores:
        result.append(get_transactions_by_pay_code(x, date))
    return result


def daily_revenue_report(store_data):
    with open("./reporting/templates/daily_revenue.html", "r") as file:
        template_str = file.read()

    jinja_template = Template(template_str)

    email_data = {
        "date": str(datetime.now().strftime("%A %B %d, %Y")),
        "store_data": store_data
    }

    email_content = jinja_template.render(email_data)

    email_engine.send_html_email(from_name=creds.company_name,
                                 from_address=creds.gmail_sales_user,
                                 from_pw=creds.gmail_sales_pw,
                                 recipients_list=creds.alex_only,
                                 subject="Test",
                                 content=email_content,
                                 product_photo=None,
                                 mode="related",
                                 logo=True)



