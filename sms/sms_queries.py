from setup.date_presets import *

query_start = """
    SELECT CUST_NO 
    FROM AR_CUST
    WHERE FST_NAM != 'Change' AND FST_NAM IS NOT NULL AND 
    PHONE_1 IS NOT NULL AND INCLUDE_IN_MARKETING_MAILOUTS = 'Y' AND 
    """

# Retail First Time Customers (ftc)
ftc_text_1 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT = '{yesterday} 00:00:00.000'"
ftc_text_2 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT = '{three_day_ago} 00:00:00.000'"
ftc_text_3 = f"CATEG_COD = 'RETAIL' AND PHONE_1 IS NOT NULL AND FST_SAL_DAT = '{one_week_ago} 00:00:00.000'"

# Returning Customers (rc)
rc_1 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{one_day_ago} 00:00:00.000' AND LST_SAL_DAT = '{one_day_ago} 00:00:00.000'"
rc_2 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{three_day_ago} 00:00:00.000' AND LST_SAL_DAT = '{three_day_ago} 00:00:00.000'"
rc_3 = f"CATEG_COD = 'RETAIL' AND FST_SAL_DAT != '{one_week_ago} 00:00:00.000' AND LST_SAL_DAT = '{one_week_ago} 00:00:00.000'"

# Wholesale
wholesale_1 = f"CATEG_COD = 'WHOLESALE' AND LST_SAL_DAT = '{one_day_ago} 00:00:00.000'"

# Birthday
month = datetime.now().month
birthday = f"(PROF_COD_2 = '{month}' OR PROF_COD_3 = '{month}')"
