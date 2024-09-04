from datetime import datetime

import setup.date_presets
from setup import date_presets
from database import Database as db


def remove_refunds_from_sms_funnel(log_file):
    """Gets list of refunds from today"""
    print(f'Remove Online Refunds from SMS Funnel: Starting at {datetime.now():%H:%M:%S}', file=log_file)

    # Get all refunds from a given day
    query = f"""
    SELECT hist.BUS_DAT, cust.CUST_NO, cust.NAM, TKT_NO
    FROM PS_TKT_HIST hist
    Inner join AR_CUST cust on cust.cust_no = hist.cust_no
    WHERE TKT_NO like '%R1' AND BUS_DAT = '{setup.date_presets.today}'
    """

    response = db.query(query)
    if response is not None:
        for x in response:
            customer_number = x[1]
            event_number = x[2]
            ticket_number = x[3]
            last_sale_date = str(x[4])

            query = f"""
            SELECT hist.BUS_DAT, cust.CUST_NO, hist.EVENT_NO, TKT_NO
            FROM PS_TKT_HIST hist
            Inner join AR_CUST cust on cust.cust_no = hist.cust_no
            WHERE cust.CUST_NO = '{customer_number}' AND hist.TKT_NO not like '%{ticket_number[:-2]}%'
            AND hist.EVENT_NO != '{event_number}'
            ORDER BY hist.BUS_DAT DESC   
            """
            response = db.query(query)

            # If Customer has sales history prior to this event
            if response is not None:
                for x in response:
                    if has_refund(x[3]):
                        # Skip this iteration
                        continue
                    else:
                        # Print most recent timestamp of completed ticket
                        most_recent_timestamp = x[0]
                        break

                # Set Customer LST_SAL_DAT to most recent time stamp
                query = f"""
                UPDATE AR_CUST
                SET LST_SAL_DAT = '{most_recent_timestamp}'
                WHERE CUST_NO = '{customer_number}'
                """
                db.query(query)
                print(
                    f'Customer: {customer_number} last sale date changed from {last_sale_date} '
                    f'to {most_recent_timestamp}',
                    file=log_file,
                )

            # If customer has no sales history
            else:
                query = f"""
                UPDATE AR_CUST
                SET LST_SAL_DAT = NULL, FST_SAL_DAT = NULL, LST_SAL_AMT = NULL
                WHERE CUST_NO = '{customer_number}'
                """
                db.query(query)
                print(
                    f'Customer: {customer_number} last sale date changed from {last_sale_date} ' f'to NULL',
                    file=log_file,
                )
    else:
        print('No refunds today', file=log_file)

    print(f'Remove Online Refunds from SMS Funnel: Completed at {date_presets.today:%H:%M:%S}', file=log_file)
    print('-----------------------', file=log_file)


def has_refund(order_number) -> bool:
    """returns true if order has an associated refund"""
    query = f"""
    SELECT TKT_NO
    FROM PS_TKT_HIST
    WHERE TKT_NO like '{order_number}%'
    """
    response = db.query(query)
    if response is not None:
        for x in response:
            if x[0][-2] == 'R':
                return True
        return False
