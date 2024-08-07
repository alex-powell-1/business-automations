from setup.date_presets import *
from setup import creds

query_start = f"""
    SELECT CUST_NO 
    FROM AR_CUST
    WHERE FST_NAM != 'Change' AND FST_NAM IS NOT NULL AND 
    PHONE_1 IS NOT NULL AND {creds.sms_subscribe_status} = 'Y' AND 
    """

# Retail First Time Customers (ftc)
ftc_text_1 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT = '{yesterday}'"
ftc_text_2 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT = '{three_day_ago}'"
ftc_text_3 = f"CATEG_COD = 'RETAIL' AND PHONE_1 IS NOT NULL AND FST_SAL_DAT = '{one_week_ago}'"

# Returning Customers (rc)
rc_1 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{yesterday}' AND LST_SAL_DAT = '{yesterday}'"
rc_2 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{three_day_ago}' AND LST_SAL_DAT = '{three_day_ago}'"
rc_3 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{one_week_ago}' AND LST_SAL_DAT = '{one_week_ago}'"

test = "CUST_NO = '105786'"

# Wholesale
wholesale_1 = f"CATEG_COD = 'WHOLESALE' AND LST_SAL_DAT = '{yesterday}'"

# Birthday
month = today.month
birthday = f"(PROF_COD_2 = '{month}' OR PROF_COD_3 = '{month}')"
