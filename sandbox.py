from reporting import product_reports
from setup import creds
from datetime import datetime
from product_tools import featured
from reporting import lead_generator_notification

# log_file = open(creds.business_automation_log, "a")
#
# print("-----------------------", file=log_file)
# print(f"TEST Automations Starting at {datetime.now():%H:%M:%S}", file=log_file)
# print("-----------------------", file=log_file)
#
# #product_reports.administrative_report(recipients=creds.admin_team, log_file=log_file)
#
# log_file.close()

from setup.query_engine import QueryEngine




set_contact_1()