import pandas as pd
from setup.sms_engine import format_phone
from setup.query_engine import QueryEngine
from datetime import datetime
from customers.customers import Customer, get_customer_number_by_phone
from setup.create_log import create_customer_log
from setup.creds import unsubscribed_sms

db = QueryEngine()


def get_phones_to_unsubscribe():
    data = pd.read_csv(r"\\MAINSERVER\Share\Twilio\Incoming\sms_log.csv")
    messages = data.to_dict(orient="records")
    result = []
    for x in messages:
        for y in ['stop', 'unsubscribe']:
            if str(x['body']).replace(" ", "").lower() == y:
                result.append(format_phone(x['from_phone'], mode="Counterpoint"))
    if len(result) > 0:
        return result
    else:
        return None


def unsubscribe_from_sms():
    target_phones = get_phones_to_unsubscribe()
    if target_phones is not None:
        count = 1
        for x in target_phones:
            customer_number = get_customer_number_by_phone(x)
            x = Customer(customer_number)
            if x.sms_subscribe == 'Y':
                query = f"""
                UPDATE AR_CUST
                SET INCLUDE_IN_MARKETING_MAILOUTS = 'N', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
                WHERE PHONE_1 = '{x.phone_1}'
                """
                db.query_db(query, commit=True)
                x.set_customer_details()
                if x.sms_subscribe == 'N':
                    print(f"Customer {x.number} now unsubscribed")
                    create_customer_log(customer_number=x.number,
                                        first_name=x.first_name,
                                        last_name=x.last_name,
                                        name=x.name,
                                        phone_1=x.phone_1,
                                        status_1_col_name="unsubscribed",
                                        status_1_data=f"Unsubscribed on {datetime.now().strftime("%x")}",
                                        log_location=unsubscribed_sms)

                    print(f"#{count}: Unsubscribed {x.name} (ID: {x.number}) from SMS Texts")
                    count += 1
                else:
                    print(f"{x.name} (CUST_NO: {x.number}) is still subscribed")
            else:
                print(f"{x.first_name} {x.last_name} ({x.name}) (ID: {x.number}) already unsubscribed")
    else:
        return


unsubscribe_from_sms()
