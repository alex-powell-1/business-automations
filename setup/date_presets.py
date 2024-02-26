from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import calendar

# Date Presets
today = str((date.today()).strftime("%Y-%m-%d"))
two_weeks_ago = str((date.today() + relativedelta(weeks=-2)).strftime("%Y-01-01"))
six_months_ago = str((date.today() + relativedelta(months=-6)).strftime("%Y-%m-%d"))
last_year_start = str((date.today() + relativedelta(years=-1)).strftime("%Y-01-01"))
now = datetime.now().strftime("%x")
one_day_ago = date.today()+relativedelta(days=-1)
two_day_ago = date.today()+relativedelta(days=-2)
three_day_ago = date.today()+relativedelta(days=-3)
one_week_ago = date.today()+relativedelta(weeks=-1)

# ---- Last Week Report ---- #
# Sunday Values
if datetime.today().isoweekday() == 7:
    last_week_day_offset_start = -7
    last_week_day_offset_end = -1
# All Other Days - else block will output Sunday - Saturday of previous week
else:
    last_week_day_offset_start = -7 - (datetime.today().isoweekday())
    last_week_day_offset_end = -1 - (datetime.today().isoweekday())

# Revenue Report
# The revenue report will generate revenue data for the past number of weeks and years specified
weeks_to_show = 6
years_to_show = 3

# Forecasting
# This report will look at top revenue items during this season of last year (as defined by forecast days)
# Example: If today is 1/1/24 and forecast days is 45, it will show top earners for 1/1/23 - 2/15/23
forecast_days = 45
low_stock_window = 90
last_year_forecast = str((date.today() + relativedelta(years=-1, days=forecast_days)).strftime("%Y-%m-%d"))
last_year_low_stock_window = str((date.today() + relativedelta(years=-1, days=low_stock_window)).strftime("%Y-%m-%d"))

# Low Stock - How many items to show on report
number_of_low_stock_items = 100

# Date Presets
last_month_start = str((date.today() + relativedelta(months=-1)).strftime("%Y-%m-01"))
last_month_end = str((datetime.strptime(date.today().strftime("%Y-%m-01"), "%Y-%m-01") +
                      relativedelta(days=-1)).strftime("%Y-%m-%d"))
last_week_start = str((date.today() + relativedelta(days=last_week_day_offset_start)).strftime("%Y-%m-%d"))
last_week_end = str((date.today() + relativedelta(days=last_week_day_offset_end)).strftime("%Y-%m-%d"))
month_start = str((date.today()).strftime("%Y-%m-01"))

month_start_last_year = str((datetime.strptime(month_start, "%Y-%m-%d") +
                             relativedelta(years=-1)).strftime("%Y-%m-%d"))

month_end = (f"{datetime.now().year}-{datetime.now().month}-"
             f"{calendar.monthrange(datetime.now().year, datetime.now().month)[1]}")

year_start = str((date.today()).strftime("%Y-01-01"))
today = str((date.today()).strftime("%Y-%m-%d"))
yesterday = str((date.today() + relativedelta(days=-1)).strftime("%Y-%m-%d"))
one_year_ago = str((date.today() + relativedelta(years=-1)).strftime("%Y-%m-%d"))
two_years_ago = str((date.today() + relativedelta(years=-2)).strftime("%Y-%m-%d"))

date_format = "%x"

# SMS Related Dates

# For regular Coupon
coupon_expiration_day_3 = str((date.today() + relativedelta(weeks=+2, days=-3)).strftime("%x"))
# For Birthday Coupon - Expires 10th day of following month
birthday_coupon_expiration_day = str((date.today() + relativedelta(months=+1)).strftime("%m/10/%Y"))

