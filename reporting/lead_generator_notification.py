import numpy as np
import pandas
from jinja2 import Template

import customer_tools.customers
from setup import creds
from setup.date_presets import *
from setup.email_engine import Email
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


def create_new_customers():
    """Send yesterday's entry's to Counterpoint as new customer_tools for further marketing.
    Will skip customer if email or phone is already in our system."""

    error_handler.logger.info('Create New Customers: Starting')

    with open(creds.design_lead_log, encoding='utf-8') as lead_file:
        # Dataframe for Log
        df = pandas.read_csv(lead_file)
        df = df.replace({np.nan: None})
        entries = df.to_dict('records')

        # Get yesterday submissions
        today_entries = []

        for x in entries:
            if x['date'][:10] == str(today):
                today_entries.append(x)

        if len(today_entries) > 0:
            print('HERE')
            for x in today_entries:
                first_name = x['first_name']
                last_name = x['last_name']
                phone_number = x['phone']
                email = x['email']
                street_address = x['street']
                city = x['city']
                state = x['state']
                zip_code = x['zip_code']
                if not customer_tools.customers.is_customer(email_address=x['email'], phone_number=x['phone']):
                    # Add new customer via NCR Counterpoint API
                    customer_number = customer_tools.customers.add_new_customer(
                        first_name=first_name,
                        last_name=last_name,
                        phone_number=phone_number,
                        email_address=email,
                        street_address=street_address,
                        city=city,
                        state=state,
                        zip_code=zip_code,
                    )
                    # Log on share
                    log_data = [
                        [
                            str(datetime.now())[:-7],
                            customer_number,
                            first_name,
                            last_name,
                            phone_number,
                            email,
                            street_address,
                            city,
                            state,
                            zip_code,
                        ]
                    ]
                    df = pandas.DataFrame(
                        log_data,
                        columns=[
                            'date',
                            'customer_number',
                            'first_name',
                            'last_name',
                            'phone_number',
                            'email',
                            'street',
                            'city',
                            'state',
                            'zip_code',
                        ],
                    )
                    create_log.write_log(df, creds.new_customer_log)
                    error_handler.logger.info(f'Created customer: {customer_number}: {first_name} {last_name}')
                else:
                    error_handler.logger.info(
                        f'{first_name} {last_name} is already a customer. Skipping customer creation.'
                    )
        else:
            error_handler.logger.info('No new customer_tools to add')

    error_handler.logger.info(f'Create New Customers: Finished at {datetime.now():%H:%M:%S}')
