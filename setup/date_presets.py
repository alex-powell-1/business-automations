from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# Date Presets
business_start_date = datetime(2020, 1, 1)
business_start_today = datetime.today().replace(hour=8)
thirty_minutes_ago = datetime.now() + relativedelta(minutes=-30)
five_minutes_ago = datetime.now() + relativedelta(minutes=-5)

thirty_seconds_ago = datetime.now() + relativedelta(seconds=-30)
twenty_four_hours_ago = datetime.now() + relativedelta(hours=-24)
day_start = datetime.combine(date.today(), datetime.min.time())

today = date.today()
yesterday = today + relativedelta(days=-1)
two_weeks_ago = today + relativedelta(weeks=-2)
two_day_ago = date.today() + relativedelta(days=-2)
three_day_ago = date.today() + relativedelta(days=-3)
one_week_ago = date.today() + relativedelta(weeks=-1)


# Month
month_start = datetime(year=today.year, month=today.month, day=1)
month_end = month_start + relativedelta(months=+1, days=-1)
# Last Month
last_month_start = month_start + relativedelta(months=-1)
last_month_end = month_start + relativedelta(days=-1)

six_months_ago = today + relativedelta(months=-6)

# Year
one_year_ago = today + relativedelta(years=-1)
two_years_ago = today + relativedelta(years=-2)
month_start_last_year = month_start + relativedelta(years=-1)

year_start = datetime(today.year, 1, 1)
year_end = datetime(today.year, 12, 31)

last_year_start = year_start + relativedelta(years=-1)
last_year_end = year_end + relativedelta(years=-1)


# ---- Last Week Report ---- #
# Sunday Values
if datetime.today().isoweekday() == 7:
    last_week_day_offset_start = -7
    last_week_day_offset_end = -1
# All Other Days - else block will output Sunday - Saturday of previous week
else:
    last_week_day_offset_start = -7 - (datetime.today().isoweekday())
    last_week_day_offset_end = -1 - (datetime.today().isoweekday())

last_week_start = today + relativedelta(days=last_week_day_offset_start)
last_week_end = today + relativedelta(days=last_week_day_offset_end)
# ------------------------- #

# Revenue Report
# The revenue report will generate revenue data for the past number of weeks and years specified
weeks_to_show = 6
years_to_show = 3

# Forecasting
# This report will look at top revenue items during this season of last year (as defined by forecast days)
# Example: If today is 1/1/24 and forecast days is 45, it will show top earners for 1/1/23 - 2/15/23
forecast_days = 45
low_stock_window = 90
last_year_forecast = today + relativedelta(years=-1, days=forecast_days)
last_year_low_stock_window = today + relativedelta(years=-1, days=low_stock_window)

# Low Stock - How many items to show on report
number_of_low_stock_items = 100

date_format = '%x'

# SMS Related Dates
# For regular Coupon
coupon_expiration_day_3 = today + relativedelta(weeks=+2, days=-3)
# For Birthday Coupon - Expires 10th day of following month
birthday_coupon_expiration_day = month_start + relativedelta(months=+1, days=+9)

# Reporting Periods
reporting_periods = {
    'yesterday': {'start': yesterday, 'end': yesterday},
    'last_week': {'start': last_week_start, 'end': last_week_end},
}


class Dates:
    def __init__(self):
        self.now = datetime.now()
        self.minute = self.now.minute
        self.hour = self.now.hour
        self.day = self.now.day
        self.month = self.now.month
        self.year = self.now.year

        self.today = date.today()
        self.yesterday = self.today + relativedelta(days=-1)
        self.two_weeks_ago = self.today + relativedelta(weeks=-2)
        self.two_day_ago = self.today + relativedelta(days=-2)
        self.three_day_ago = self.today + relativedelta(days=-3)
        self.one_week_ago = self.today + relativedelta(weeks=-1)
        self.one_month_ago = self.today + relativedelta(months=-1)

        # Month
        self.month_start = datetime(year=self.today.year, month=self.today.month, day=1)
        self.month_end = self.month_start + relativedelta(months=+1, days=-1)
        # Last Month
        self.last_month_start = self.month_start + relativedelta(months=-1)
        self.last_month_end = self.month_start + relativedelta(days=-1)

        self.six_months_ago = self.today + relativedelta(months=-6)

        # Year
        self.one_year_ago = self.today + relativedelta(years=-1)
        self.two_years_ago = self.today + relativedelta(years=-2)
        self.month_start_last_year = self.month_start + relativedelta(years=-1)

        self.year_start = datetime(self.today.year, 1, 1)
        self.year_end = datetime(self.today.year, 12, 31)

        self.last_year_start = self.year_start + relativedelta(years=-1)
        self.last_year_end = self.year_end + relativedelta(years=-1)

        # ---- Last Week Report ---- #
        # Sunday Values
        if datetime.today().isoweekday() == 7:
            self.last_week_day_offset_start = -7
            self.last_week_day_offset_end = -1

        # All Other Days - else block will output Sunday - Saturday of previous week
        else:
            self.last_week_day_offset_start = -7 - (datetime.today().isoweekday())
            self.last_week_day_offset_end = -1 - (datetime.today().isoweekday())

        self.last_week_start = self.today + relativedelta(days=self.last_week_day_offset_start)
        self.last_week_end = self.today + relativedelta(days=self.last_week_day_offset_end)
        # ------------------------- #

        # Revenue Report
        # The revenue report will generate revenue data for the past number of weeks and years specified
        self.weeks_to_show = 6
        self.years_to_show = 3

        # Forecasting
        # This report will look at top revenue items during this season of last year (as defined by forecast days)
        # Example: If today is 1/1/24 and forecast days is 45, it will show top earners for 1/1/23 - 2/15/23
        self.forecast_days = 45
        self.low_stock_window = 90
        self.last_year_forecast = self.today + relativedelta(years=-1, days=self.forecast_days)
        self.last_year_low_stock_window = self.today + relativedelta(years=-1, days=self.low_stock_window)

        self.date_format = '%x'

        # SMS Related Dates
        # For regular Coupon
        self.coupon_expiration_day_3 = self.today + relativedelta(weeks=+2, days=-3)
        # For Birthday Coupon - Expires 10th day of following month
        self.birthday_coupon_expiration_day = self.month_start + relativedelta(months=+1, days=+9)

        # Reporting Periods
        self.reporting_periods = {
            'yesterday': {'start': self.yesterday, 'end': self.yesterday},
            'last_week': {'start': self.last_week_start, 'end': self.last_week_end},
        }
