from setup import creds
from datetime import datetime
import pandas


def create_product_log(item_no, product_name, qty_avail, status_1_col_name,
                       status_1_data, log_location, status_2_col_name="", status_2_data=""):
    # Two Status
    if status_2_col_name == "":
        log_data = [[str(datetime.now())[:-7], item_no, product_name, qty_avail, status_1_data]]

        df = pandas.DataFrame(log_data, columns=["date", "item_no", "product_name",
                                                 "qty_avail", status_1_col_name])
    # One Status
    else:
        log_data = [[str(datetime.now())[:-7], item_no, product_name, qty_avail, status_1_data, status_2_data]]

        df = pandas.DataFrame(log_data, columns=["date", "item_no", "product_name",
                                                 "qty_avail", status_1_col_name, status_2_col_name])

    # Looks for file. If it has been deleted, it will recreate it.
    try:
        pandas.read_csv(log_location)
    except FileNotFoundError:
        df.to_csv(log_location, mode='a', header=True, index=False)
    else:
        df.to_csv(log_location, mode='a', header=False, index=False)


def create_customer_log(customer_number, first_name, last_name, name, phone_1, status_1_col_name,
                        status_1_data, log_location, status_2_col_name="", status_2_data=""):
    # Two Status
    if status_2_col_name == "":
        log_data = [[str(datetime.now())[:-7], customer_number, first_name, last_name,
                     name, phone_1, status_1_data]]

        df = pandas.DataFrame(log_data, columns=["date", "cust_no", "first_name", "last_name",
                                                 "name", "phone_1", status_1_col_name])
    # One Status
    else:
        log_data = [[str(datetime.now())[:-7], customer_number, first_name, last_name,
                     name, phone_1, status_1_data, status_2_data]]

        df = pandas.DataFrame(log_data, columns=["date", "cust_no", "first_name", "last_name",
                                                 "name", "phone_1", status_1_col_name, status_2_col_name])

    # Looks for file. If it has been deleted, it will recreate it.
    try:
        pandas.read_csv(log_location)
    except FileNotFoundError:
        df.to_csv(log_location, mode='a', header=True, index=False)
    else:
        df.to_csv(log_location, mode='a', header=False, index=False)


def create_sms_log(cust_no, phone, sent_message, response, log_location):
    """ Creates a log file on share server. Logs date, phone, message, and twilio response"""
    log_message = sent_message
    log_data = [[str(datetime.now())[:-7], cust_no, format_phone(phone, mode="Counterpoint"), log_message.strip().replace("\n", ""), response]]
    df = pandas.DataFrame(log_data, columns=["date", "cust_no", "to_phone", "body", "response"])
    # Looks for file. If it has been deleted, it will recreate.

    try:
        pandas.read_csv(log_location)
    except FileNotFoundError:
        df.to_csv(log_location, mode='a', header=True, index=False)
    else:
        df.to_csv(log_location, mode='a', header=False, index=False)


def format_phone(phone_number, mode="Twilio", prefix=False):
    """Cleanses input data and returns masked phone for either Twilio or Counterpoint configuration"""
    phone_number_as_string = str(phone_number)
    # Strip away extra symbols
    formatted_phone = phone_number_as_string.replace(" ", "")  # Remove Spaces
    formatted_phone = formatted_phone.replace("-", "")  # Remove Hyphens
    formatted_phone = formatted_phone.replace("(", "")  # Remove Open Parenthesis
    formatted_phone = formatted_phone.replace(")", "")  # Remove Close Parenthesis
    formatted_phone = formatted_phone.replace("+1", "")  # Remove +1
    formatted_phone = formatted_phone[-10:]  # Get last 10 characters
    if mode == "Counterpoint":
        # Masking ###-###-####
        cp_phone = formatted_phone[0:3] + "-" + formatted_phone[3:6] + "-" + formatted_phone[6:10]
        return cp_phone
    else:
        if prefix:
            formatted_phone = "+1" + formatted_phone
        return formatted_phone