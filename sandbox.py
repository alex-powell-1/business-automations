from reporting import product_reports
from setup import creds
from setup import date_presets
log_file = open(creds.business_automation_log, "a")
from datetime import datetime

print("-----------------------", file=log_file)
print(f"Business Automations Starting at {datetime.now():%H:%M:%S}", file=log_file)
print("-----------------------", file=log_file)

product_reports.administrative_report(recipients=creds.alex_only, log_file=log_file)
