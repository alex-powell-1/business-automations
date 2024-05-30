from datetime import datetime

from customer_tools import stock_notification
from customer_tools.customers import set_contact_1

from setup import creds

# -----------------
# Driver for Business Automations
# -----------------

now = datetime.now()
day = now.day
hour = now.hour
minute = now.minute

sms_test_mode = False  # if true, will only write generated messages write to logs
sms_test_customer = False  # if true, will only send to single employee for testing

errors = 0

log_file = open(creds.business_automation_log, "a")

print("-----------------------", file=log_file)
print(f"Business Automations Starting at {now:%H:%M:%S}", file=log_file)
print("-----------------------", file=log_file)
try:
    try:
        sku = "BTSP4MP"
        stock_notification.send_email(greeting="hello", email="lukebbarrier06@outlook.com", item_number="BTSP4MP", coupon_code="10OFF", photo=creds.photo_path + f"/{sku}.jpg")
    except Exception as err:
        errors += 1
        print(f"Error: stock", file=log_file)
        print(err, file=log_file)
        print("-----------------------\n", file=log_file)

except KeyboardInterrupt:
    print("-----------------------", file=log_file)
    print(f"Process Terminated by User at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
else:
    print("-----------------------", file=log_file)
    print(f"Business Automations Complete at {datetime.now():%H:%M:%S}", file=log_file)
    total_seconds = (datetime.now() - now).total_seconds()
    minutes = total_seconds // 60
    seconds = round(total_seconds % 60, 2)
    print(f"Total time of operation: {minutes} minutes {seconds} seconds", file=log_file)
finally:
    print(f"Total Errors: {errors}", file=log_file)
    print("-----------------------\n\n\n", file=log_file)
    log_file.close()
