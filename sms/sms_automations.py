import random
from datetime import datetime

from customer_tools.customers import Customer
from setup import creds
from database import Database
from setup.sms_engine import SMSEngine
from sms import sms_queries
from sms.sms_messages import salutations
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


def create_customer_text(
    origin,
    campaign,
    query,
    msg,
    rewards_msg='',
    image_url=None,
    msg_prefix=False,
    send_rwd_bal=True,
    test_mode=False,
    test_customer=False,
):
    """Get a list of customers and create custom text messages for each customer."""
    prefix = ''
    if msg_prefix:
        prefix = f'{creds.company_name}: '

    customer_list = []

    if test_customer:
        customer_list = creds.sms_automations['test_customer']['test_list']
    else:
        # Get List of Customers
        response = Database.query(sms_queries.query_start + query)

        if response is not None:
            customer_list = []
            for x in response:
                customer_list.append(x[0])
        else:
            error_handler.logger.info('No messages to send today.')

    for x in customer_list:
        # Reset rewards message
        rewards_msg = ''
        # Create customer object
        cust = Customer(x)
        cust_no = cust.number
        to_phone = cust.phone_1
        error_handler.logger.info(f'Sending Message to {cust.name} at {to_phone}')
        first_name = cust.first_name
        reward_points = cust.rewards_points_balance

        # Check if they have rewards points.

        if reward_points > 0 and send_rwd_bal:
            rewards_msg = f'\nYour reward balance: ${reward_points}'

        message = (
            prefix
            + random.choice(salutations.greeting)
            + first_name
            + '! '
            + msg
            + random.choice(salutations.farewell)
            + rewards_msg
        )

        # Send Text
        SMSEngine.send_text(
            origin=origin,
            campaign=campaign,
            category=cust.category,
            username='Automation',
            cust_no=cust_no,
            name=cust.name,
            to_phone=to_phone,
            message=message,
            url=image_url,
            test_mode=test_mode,
        )


def remove_wholesale_from_loyalty(log_file):
    """New customer templates automatically add new customer_tools to the BASIC program.
    This script will remove wholesale customer_tools from a loyalty program and set balance to 0."""

    print(f'Remove Wholesale From Loyalty: Starting at {datetime.now():%H:%M:%S}', file=log_file)

    query = """
    UPDATE AR_CUST
    SET LOY_PGM_COD = NULL, LOY_PTS_BAL = '0', LOY_CARD_NO = 'VOID', LST_MAINT_DT = GETDATE()
    WHERE CATEG_COD = 'WHOLESALE'
    """
    Database.query(query)

    print(f'Remove Wholesale From Loyalty: Finished at {datetime.now():%H:%M:%S}', file=log_file)
    print('-----------------------', file=log_file)
