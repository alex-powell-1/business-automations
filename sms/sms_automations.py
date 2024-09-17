import random

from customer_tools.customers import Customer
from setup import creds
from database import Database
from setup.sms_engine import SMSEngine
from sms.sms_messages import SMSMessages
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


def create_customer_text(
    origin, campaign, query, msg, rewards_msg='', image_url=None, msg_prefix=False, send_rwd_bal=True
):
    """Get a list of customers and create custom text messages for each customer."""
    prefix = ''
    if msg_prefix:
        prefix = f'{creds.Company.name}: '

    customer_list = []

    ############################
    ######## Test Mode #########
    ############################

    # Test Mode
    test_mode: bool = creds.SMSAutomations.test_mode

    ############################
    ###### Get Customers #######
    ############################
    # Test Customer
    if creds.SMSAutomations.TestCustomer.enabled:
        # Enable Test Customer in the config file to send a message to the customer(s) listed in the config file.
        customer_list = creds.SMSAutomations.TestCustomer.cust_list
    else:
        response = Database.query(query)

        if response is not None:
            customer_list = []
            for x in response:
                customer_list.append(x[0])
        else:
            error_handler.logger.info('No messages to send today.')

    messages_to_send = len(customer_list)
    count = 0
    ############################
    ###### Get Messages #######
    ############################
    for x in customer_list:
        # Reset rewards message
        rewards_msg = ''
        # Create customer object
        cust = Customer(x)
        cust_no = cust.number
        to_phone = cust.phone_1
        first_name = cust.first_name
        reward_points = cust.rewards_points_balance

        # Check if they have rewards points.

        if reward_points > 0 and send_rwd_bal:
            rewards_msg = f'\nYour reward balance: ${reward_points}'

        message = (
            prefix
            + random.choice(SMSMessages.greetings)
            + first_name
            + '! '
            + msg
            + random.choice(SMSMessages.farewells)
            + rewards_msg
        )
        count += 1
        error_handler.logger.info(
            f'{count}/{messages_to_send}: Sending Message to {cust.name} (CUST_NO: {cust_no}) at {to_phone}:\n{message}\n'
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
