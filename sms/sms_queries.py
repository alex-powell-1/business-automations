from setup.date_presets import *
from setup import creds


class SMSQueries:
    def __init__(self, dates: Dates):
        self.dates = dates
        self.q_start = f"""
        SELECT CUST_NO 
        FROM AR_CUST
        WHERE FST_NAM != 'Change' AND FST_NAM IS NOT NULL AND 
        ((PHONE_1 IS NOT NULL AND {creds.Table.CP.Customers.Column.sms_1_is_subscribed} = 'Y') OR
        (PHONE_2 IS NOT NULL AND {creds.Table.CP.Customers.Column.sms_2_is_subscribed} = 'Y')) AND 
        """


class FTCQueries(SMSQueries):
    """Queries for First Time Customers"""

    def __init__(self, dates: Dates):
        super().__init__(dates)
        self.text_1 = self.q_start + f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT = '{self.dates.yesterday}'"
        self.text_2 = self.q_start + f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT = '{self.dates.three_day_ago}'"
        self.text_3 = (
            self.q_start + f"CATEG_COD = 'RETAIL' AND PHONE_1 IS NOT NULL AND FST_SAL_DAT = '{dates.one_week_ago}'"
        )


class RCQueries(SMSQueries):
    """Queries for Returning Customers"""

    def __init__(self, dates: Dates):
        super().__init__(dates)

        self.text_1 = self.q_start + (
            f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{dates.yesterday}' AND LST_SAL_DAT = '{dates.yesterday}'"
        )
        self.text_2 = (
            self.q_start
            + f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{dates.three_day_ago}' AND LST_SAL_DAT = '{dates.three_day_ago}'"
        )
        self.text_3 = (
            self.q_start
            + f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{dates.one_week_ago}' AND LST_SAL_DAT = '{dates.one_week_ago}'"
        )


class WholesaleQueries(SMSQueries):
    """Queries for Wholesale Customers"""

    def __init__(self, dates: Dates):
        super().__init__(dates)
        self.text_1 = self.q_start + f"CATEG_COD = 'WHOLESALE' AND LST_SAL_DAT = '{dates.yesterday}'"


class BirthdayQueries(SMSQueries):
    """Queries for Birthday Customers"""

    def __init__(self, dates: Dates):
        super().__init__(dates)
        month = dates.today.month
        self.text_1 = self.q_start + f"(PROF_COD_2 = '{month}' OR PROF_COD_3 = '{month}')"
