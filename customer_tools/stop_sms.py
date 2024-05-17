from datetime import datetime

import setup.date_presets
from setup import date_presets
from setup.query_engine import QueryEngine

# from datetime import datetime
# from customer_tools.customer_tools import Customer, get_customer_number_by_phone
# from setup.create_log import create_customer_log
# from setup.creds import unsubscribed_sms

db = QueryEngine()


# def get_phones_to_unsubscribe():
#     data = pd.read_csv(r"\\MAINSERVER\Share\Twilio\Incoming\sms_log.csv")
#     messages = data.to_dict(orient="records")
#     result = []
#     for x in messages:
#         for y in ['stop', 'unsubscribe']:
#             if str(x['body']).replace(" ", "").lower() == y:
#                 result.append(format_phone(x['from_phone'], mode="Counterpoint"))
#     if len(result) > 0:
#         return result
#     else:
#         return None
#
#
# def unsubscribe_from_sms():
#     """Unsubscribes customer_tools from SMS marketing following a STOP or stop or unsubscribe request"""
#     target_phones = get_phones_to_unsubscribe()
#     if target_phones is not None:
#         count = 1
#         for x in target_phones:
#             customer_number = get_customer_number_by_phone(x)
#             x = Customer(customer_number)
#             if x.sms_subscribe == 'Y':
#                 query = f"""
#                 UPDATE AR_CUST
#                 SET INCLUDE_IN_MARKETING_MAILOUTS = 'N', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
#                 WHERE PHONE_1 = '{x.phone_1}'
#                 """
#                 db.query_db(query, commit=True)
#                 x.set_customer_details()
#                 if x.sms_subscribe == 'N':
#                     print(f"Customer {x.number} now unsubscribed")
#                     create_customer_log(customer_number=x.number,
#                                         first_name=x.first_name,
#                                         last_name=x.last_name,
#                                         name=x.name,
#                                         phone_1=x.phone_1,
#                                         status_1_col_name="unsubscribed",
#                                         status_1_data=f"Unsubscribed on {datetime.now().strftime("%x")}",
#                                         log_location=unsubscribed_sms)
#
#                     print(f"#{count}: Unsubscribed {x.name} (ID: {x.number}) from SMS Texts")
#                     count += 1
#                 else:
#                     print(f"{x.name} (CUST_NO: {x.number}) is still subscribed")
#             else:
#                 print(f"{x.first_name} {x.last_name} ({x.name}) (ID: {x.number}) already unsubscribed")
#     else:
#         return


def remove_refunds_from_sms_funnel(log_file):
    """Gets list of refunds from today"""
    print(f"Remove Online Refunds from SMS Funnel: Starting at {datetime.now():%H:%M:%S}", file=log_file)

    # Get all refunds from a given day
    query = f"""
    SELECT hist.BUS_DAT, cust.CUST_NO, cust.NAM, TKT_NO
    FROM PS_TKT_HIST hist
    Inner join AR_CUST cust on cust.cust_no = hist.cust_no
    WHERE TKT_NO like '%R1' AND BUS_DAT = '{setup.date_presets.today}'
    """

    response = db.query_db(query)
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
            response = db.query_db(query)

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
                db.query_db(query, commit=True)
                print(f"Customer: {customer_number} last sale date changed from {last_sale_date} "
                      f"to {most_recent_timestamp}", file=log_file)

            # If customer has no sales history
            else:
                query = f"""
                UPDATE AR_CUST
                SET LST_SAL_DAT = NULL, FST_SAL_DAT = NULL, LST_SAL_AMT = NULL
                WHERE CUST_NO = '{customer_number}'
                """
                db.query_db(query, commit=True)
                print(f"Customer: {customer_number} last sale date changed from {last_sale_date} "
                      f"to NULL", file=log_file)
    else:
        print("No refunds today", file=log_file)

    print(f"Remove Online Refunds from SMS Funnel: Completed at {date_presets.today:%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)


def has_refund(order_number) -> bool:
    """returns true if order has an associated refund"""
    query = f"""
    SELECT TKT_NO
    FROM PS_TKT_HIST
    WHERE TKT_NO like '{order_number}%'
    """
    response = db.query_db(query)
    if response is not None:
        for x in response:
            if x[0][-2] == 'R':
                return True
        return False
