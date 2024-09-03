from datetime import datetime
import pandas

from setup.utilities import PhoneNumber


def create_product_log(
    item_no,
    product_name,
    qty_avail,
    status_1_col_name,
    status_1_data,
    log_location,
    status_2_col_name='',
    status_2_data='',
):
    # Two Status
    if status_2_col_name == '':
        log_data = [[str(datetime.now())[:-7], item_no, product_name, qty_avail, status_1_data]]

        df = pandas.DataFrame(log_data, columns=['date', 'item_no', 'product_name', 'qty_avail', status_1_col_name])
    # One Status
    else:
        log_data = [[str(datetime.now())[:-7], item_no, product_name, qty_avail, status_1_data, status_2_data]]

        df = pandas.DataFrame(
            log_data, columns=['date', 'item_no', 'product_name', 'qty_avail', status_1_col_name, status_2_col_name]
        )

    write_log(df, log_location)


def create_customer_log(
    customer_number,
    first_name,
    last_name,
    name,
    phone_1,
    status_1_col_name,
    status_1_data,
    log_location,
    status_2_col_name='',
    status_2_data='',
):
    # Two Status
    if status_2_col_name == '':
        log_data = [
            [str(datetime.now())[:-7], customer_number, first_name, last_name, name, phone_1, status_1_data]
        ]

        df = pandas.DataFrame(
            log_data, columns=['date', 'cust_no', 'first_name', 'last_name', 'name', 'phone_1', status_1_col_name]
        )
    # One Status
    else:
        log_data = [
            [
                str(datetime.now())[:-7],
                customer_number,
                first_name,
                last_name,
                name,
                phone_1,
                status_1_data,
                status_2_data,
            ]
        ]

        df = pandas.DataFrame(
            log_data,
            columns=[
                'date',
                'cust_no',
                'first_name',
                'last_name',
                'name',
                'phone_1',
                status_1_col_name,
                status_2_col_name,
            ],
        )

    write_log(df, log_location)


def create_sms_log(cust_no, phone, sent_message, response, log_location):
    """Creates a log file on share server. Logs date, phone, message, and twilio response"""
    log_message = sent_message
    log_data = [
        [
            str(datetime.now())[:-7],
            cust_no,
            PhoneNumber(phone).to_cp(),
            log_message.strip().replace('\n', ''),
            response,
        ]
    ]
    df = pandas.DataFrame(log_data, columns=['date', 'cust_no', 'to_phone', 'body', 'response'])
    # Looks for file. If it has been deleted, it will recreate.

    write_log(df, log_location)


def write_log(dataframe, log_location):
    """Writes CSV log to share location"""
    # Looks for file. If it has been deleted, it will recreate.
    try:
        pandas.read_csv(log_location)
    except FileNotFoundError:
        dataframe.to_csv(log_location, mode='a', header=True, index=False)
    else:
        dataframe.to_csv(log_location, mode='a', header=False, index=False)
