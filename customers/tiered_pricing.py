from datetime import datetime
from setup import creds
from setup.query_engine import QueryEngine
import pandas

console_logging = True

target_profile_code = "PROF_COD_1"

db = QueryEngine()


def create_customer_log(cust_no, business_name, total_sales, previous_tier, new_tier):
    log_data = [[str(datetime.now())[:-7], cust_no, business_name, total_sales, previous_tier, new_tier]]
    df = pandas.DataFrame(log_data, columns=["date", "business", "cust_no", "sales", "previous_tier", "new_tier"])
    log_location = creds.wholesale_pricing_tier_log

    # Looks for file. If it has been deleted, it will recreate it.
    try:
        pandas.read_csv(log_location)
    except FileNotFoundError:
        df.to_csv(log_location, mode='a', header=True, index=False)
    else:
        df.to_csv(log_location, mode='a', header=False, index=False)


def set_pricing_tier(customer_number, target_tier):
    # Set New Pricing Level
    query = f"""
                    UPDATE AR_CUST
                    SET {target_profile_code} = '{target_tier}'
                    WHERE CUST_NO = '{customer_number}'
                    """
    db.query_db(query, commit=True)


def get_pricing_tier(customer_number):
    # Get Current Pricing Tier Level
    query = f"""
                SELECT {target_profile_code}
                FROM AR_CUST
                WHERE CUST_NO = '{customer_number}'
                """
    tier = db.query_db(query)[0][0]
    return tier


def update_tiered_pricing(start_date, end_date):
    """Returns dictionary with accounts and total sales for past six months"""
    print("Updating Wholesale Accounts Tiered Pricing")
    query = f"""
    "{creds.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1 
    'select distinct AR_CUST.CUST_NO as GRP_ID, AR_CUST.NAM as GRP_DESCR 
    from AR_CUST where ((AR_CUST.CATEG_COD = ''WHOLESALE'')) 
    union 
    select distinct VI_PS_TKT_HIST.CUST_NO as GRP_ID, NULL as GRP_DESCR 
    from %HISTORY% 
    where not exists(select 1 from AR_CUST where AR_CUST.CUST_NO = VI_PS_TKT_HIST.CUST_NO) and ( (1=1) ) and ( (1=1) ) 
    and %ANYPERIODFILTER%', 'select VI_PS_TKT_HIST.CUST_NO as GRP_ID, %HISTCOLUMNS% from %HISTORY% where ( (1=1) ) and 
    ( (1=1) ) and %PERIODFILTER%', ' 
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{end_date}'')', ' (1=0) ', ' 
    (1=0) ', 0, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """
    wholesale_customers = db.query_db(query)
    if wholesale_customers is not None:
        for i in wholesale_customers:
            customer_number = i[0]
            business_name = i[1]
            total_sales = float(i[2])
            previous_pricing_tier = get_pricing_tier(customer_number)
            # Put Customers into Categories based on sales
            if total_sales >= 50000:
                target_pricing_tier = 'A'
            elif total_sales >= 25000:
                target_pricing_tier = 'B'
            elif total_sales >= 15000:
                target_pricing_tier = 'C'
            elif total_sales >= 7500:
                target_pricing_tier = 'D'
            else:
                target_pricing_tier = 'E'

            # If there hasn't been a change, then continue to next iteration
            if previous_pricing_tier == target_pricing_tier:
                continue

            # If there has been a change, set new tiered pricing level
            else:
                set_pricing_tier(customer_number, target_pricing_tier)
                # Get updated tier for logging
                new_pricing_tier = get_pricing_tier(customer_number)
                create_customer_log(customer_number, business_name, total_sales,
                                    previous_pricing_tier, new_pricing_tier)

                # Console Logging
                if console_logging:
                    print(f"{business_name} moved from level: {previous_pricing_tier} to level: {new_pricing_tier}")

        # Set all remaining wholesale accounts with no sales history to 'E'
        query = f"""
                UPDATE AR_CUST
                SET {target_profile_code} = 'E'
                WHERE CATEG_COD = 'WHOLESALE' and {target_profile_code} IS NULL
                """

        db.query_db(query, commit=True)

        print(f"Wholesale Accounts Tiered Pricing: Completed at {datetime.now()}")
        return

    else:
        return
