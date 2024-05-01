from reporting import product_reports
from setup import creds
from datetime import datetime
from product_tools import featured

log_file = open(creds.business_automation_log, "a")

print("-----------------------", file=log_file)
print(f"Business Automations Starting at {datetime.now():%H:%M:%S}", file=log_file)
print("-----------------------", file=log_file)

product_reports.administrative_report(recipients=creds.admin_team, log_file=log_file)

# try:
#     featured.update_featured_items(log_file)
# except Exception as err:
#     print("Error: Featured Products", file=log_file)
#     print(err, file=log_file)
#     print("-----------------------\n", file=log_file)

log_file.close()
