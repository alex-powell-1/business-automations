from setup import date_presets
from database import Database as db


def get_stores():
    query = f"""
    SELECT DISTINCT STR_ID
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date_presets.yesterday}'
    """
    response = db.query(query)
    if response is not None:
        stores = []
        for x in response:
            stores.append(x[0])
        return stores


def get_transactions_by_pay_code(store, date):
    result = {}
    result.update({'store': store})
    # Get Cash Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'CASH' AND
    STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        cash_revenue = float(response[0][0]) if response[0][0] else None
        cash_tickets = int(response[0][1]) if response[0][1] else None
        cash_results = {'revenue': cash_revenue, 'tickets': cash_tickets}
        result.update({'cash': cash_results})

    # Get Checks Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'CHECK' AND
    STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        check_revenue = float(response[0][0]) if response[0][0] else None
        check_tickets = int(response[0][1]) if response[0][1] else None
        check_results = {'revenue': check_revenue, 'tickets': check_tickets}
        result.update({'check': check_results})

    # Get Credit Card Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD in ('BIG', 'DEBIT', 'DISCOVER', 'MC', 'VISA', 'AMEX')
    AND STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        credit_revenue = float(response[0][0]) if response[0][0] else None
        credit_tickets = int(response[0][1]) if response[0][1] else None
        credit_results = {'revenue': credit_revenue, 'tickets': credit_tickets}
        result.update({'credit': credit_results})

    # Get Gift Card Tickets
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'GIFT'
    AND STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        gift_revenue = float(response[0][0]) if response[0][0] else None
        gift_tickets = int(response[0][1]) if response[0][1] else None
        gift_results = {'revenue': gift_revenue, 'tickets': gift_tickets}
        result.update({'gift': gift_results})

    # Get LOYALTY
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'LOYALTY'
    AND STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        loyalty_revenue = float(response[0][0]) if response[0][0] else None
        loyalty_tickets = int(response[0][1]) if response[0][1] else None
        loyalty_results = {'revenue': loyalty_revenue, 'tickets': loyalty_tickets}
        result.update({'loyalty': loyalty_results})

    # Get Store Credit
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00' and PAY_COD = 'STORE CRED'
    AND STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        store_credit_revenue = float(response[0][0]) if response[0][0] else None
        store_credit_tickets = int(response[0][1]) if response[0][1] else None
        store_credit_results = {'revenue': store_credit_revenue, 'tickets': store_credit_tickets}
        result.update({'store_credit': store_credit_results})

    # Get Deposits (liability, not revenue)
    query = f"""
        SELECT PAY_COD, AMT
        FROM PS_DOC_PMT
        WHERE PAY_DAT = '{date} 00:00:00' AND FINAL_PMT = 'N' AND STR_ID = '{store}'
        """
    response = db.query(query)
    if response is not None:
        deposit_dict = {}
        deposit_total = 0

        for x in response:
            pay_code = x[0].lower()
            amount = float(x[1])
            deposit_total += amount
            # deposit_list.append({"pay_code": pay_code, "amount": amount})
            deposits = {pay_code: amount}
            deposit_dict.update(deposits)

        deposit_dict.update({'total_deposits': deposit_total})

        result.update({'deposits': deposit_dict})

    # Get Gift Card Purchases (liability not revenue)
    query = f"""
    SELECT SUM(ORIG_AMT)
    FROM SY_GFC
    WHERE ORIG_DAT = '{date}' AND ORIG_STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        result.update({'gc_liability': float(response[0][0]) if response[0][0] else None})

    # Get Store Total
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00'
    AND STR_ID = '{store}'
    """
    response = db.query(query)
    if response is not None:
        store_total_revenue = float(response[0][0]) if response[0][0] else None
        store_total_tickets = int(response[0][1]) if response[0][1] else None
        store_total_results = {'revenue': store_total_revenue, 'tickets': store_total_tickets}
        result.update({'store_total': store_total_results})

    return result


def get_total_revenue(date):
    query = f"""
    SELECT SUM(NET_PMT_AMT), SUM(TKT_COUNT)
    FROM VI_TKT_HIST_PAY_COD
    WHERE POST_DAT = '{date} 00:00:00'
    """
    response = db.query(query)
    if response is not None:
        total_revenue = float(response[0][0]) if response[0][0] else None
        total_tickets = int(response[0][1]) if response[0][1] else None
        return {'revenue': total_revenue, 'tickets': total_tickets}


def get_order_deposit_total(date):
    # Get all partial order deposits from all pay codes and stores
    query = f"""
        SELECT SUM(AMT)
        FROM PS_DOC_PMT
        WHERE PAY_DAT = '{date} 00:00:00' AND FINAL_PMT = 'N' and PAY_COD_TYP = 'E'
        """
    response = db.query(query)
    if response is not None:
        return response[0][0]


def get_all_stores_sales_by_paycode(date):
    stores = get_stores()
    result = []
    for x in stores:
        result.append(get_transactions_by_pay_code(x, date))
    return result


def get_gc_purchase_total(date):
    query = f"""
        SELECT SUM(ORIG_AMT)
        FROM SY_GFC
        WHERE ORIG_DAT = '{date}'
        """
    response = db.query(query)
    if response is not None:
        return float(response[0][0]) if response[0][0] else None
