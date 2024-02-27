from setup.sms_engine import SMSEngine
from setup.query_engine import QueryEngine
from customers.customers import Customer
from setup import creds
from setup.create_log import create_sms_log
from sms.sms_messages import salutations, first_time_customers, returning_customers, wholesale_sms_messages
from sms import sms_queries
import random

db = QueryEngine()


def create_customer_text(query, msg_descr, msg, log_location, rewards_msg="",
                         image_url=None, msg_prefix=False, send_rwd_bal=True, test_mode=False, test_customer=False):
    """First SMS text send to new customer. Text will be delivered the day after first purchase"""
    prefix = ""
    if msg_prefix:
        prefix = message_prefix = f"{creds.company_name}: "
    if test_customer:
        customer_list = creds.test_customer
    else:
        # Get List of Customers
        response = db.query_db(sms_queries.query_start + query)

        if response is not None:
            customer_list = []
            for x in response:
                customer_list.append(x[0])
        else:
            return create_sms_log("NA", "NA", msg_descr,
                                  "No messages to send today.", log_location=log_location)

    for x in customer_list:
        cust = Customer(x)
        cust_no = cust.number
        to_phone = cust.phone_1
        first_name = cust.first_name
        reward_points = cust.rewards_points_balance

        # Check if they have rewards points.
        if reward_points > 0 and send_rwd_bal:
            rewards_msg = f"\nYour reward balance: ${reward_points}"

        message = (prefix + random.choice(salutations.greeting) + first_name + "! " +
                   msg + random.choice(salutations.farewell) + rewards_msg)
        # Send Text
        print(f"Sending Message to {cust.name}")
        engine = SMSEngine()
        engine.send_text(cust_no, to_phone, message, url=image_url, log_code=log_location, test_mode=test_mode)
