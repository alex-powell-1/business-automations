import random
from datetime import datetime

from customer_tools.customers import Customer
from setup import creds
from setup.create_log import create_sms_log
from setup.query_engine import QueryEngine
from setup.sms_engine import SMSEngine
from sms import sms_queries
from sms.sms_messages import salutations

db = QueryEngine()


def create_customer_text(
    query,
    msg_descr,
    msg,
    detail_log,
    general_log,
    rewards_msg="",
    image_url=None,
    msg_prefix=False,
    send_rwd_bal=True,
    test_mode=False,
    test_customer=False,
):
    """Get a list of customers and create custom text messages for each customer."""
    prefix = ""
    if msg_prefix:
        prefix = f"{creds.company_name}: "

    if test_customer:
        customer_list = creds.sms_automations["test_customer"]["test_list"]
    else:
        # Get List of Customers
        response = db.query_db(sms_queries.query_start + query)

        if response is not None:
            customer_list = []
            for x in response:
                customer_list.append(x[0])
        else:
            print("No messages to send today.", file=general_log)
            return create_sms_log(
                "NA",
                "NA",
                msg_descr,
                "No messages to send today.",
                log_location=detail_log,
            )

    for x in customer_list:
        # Reset rewards message
        rewards_msg = ""
        # Create customer object
        cust = Customer(x)
        cust_no = cust.number
        to_phone = cust.phone_1
        print(f"Sending Message to {cust.name} at {to_phone}")
        first_name = cust.first_name
        reward_points = cust.rewards_points_balance

        # Check if they have rewards points.

        if reward_points > 0 and send_rwd_bal:
            rewards_msg = f"\nYour reward balance: ${reward_points}"

        message = (
            prefix
            + random.choice(salutations.greeting)
            + first_name
            + "! "
            + msg
            + random.choice(salutations.farewell)
            + rewards_msg
        )

        # Send Text
        print(f"Sending Message to {cust.name}", file=general_log)
        engine = SMSEngine()
        engine.send_text(
            cust_no,
            to_phone,
            message,
            url=image_url,
            log_location=detail_log,
            test_mode=test_mode,
        )


def remove_wholesale_from_loyalty(log_file):
    """New customer templates automatically add new customer_tools to the BASIC program.
    This script will remove wholesale customer_tools from a loyalty program and set balance to 0."""

    print(
        f"Remove Wholesale From Loyalty: Starting at {datetime.now():%H:%M:%S}",
        file=log_file,
    )

    query = """
    UPDATE AR_CUST
    SET LOY_PGM_COD = NULL, LOY_PTS_BAL = '0', LOY_CARD_NO = 'VOID', LST_MAINT_DT = GETDATE()
    WHERE CATEG_COD = 'WHOLESALE'
    """
    db.query_db(query, commit=True)

    print(
        f"Remove Wholesale From Loyalty: Finished at {datetime.now():%H:%M:%S}",
        file=log_file,
    )
    print("-----------------------", file=log_file)
