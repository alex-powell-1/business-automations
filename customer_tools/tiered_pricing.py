from datetime import datetime

import pandas

from customer_tools.customers import is_current_customer
from setup import creds
from setup.query_engine import QueryEngine
from setup import date_presets
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
    response = db.query_db(query, commit=True)


def get_pricing_tier(customer_number):
    # Get Current Pricing Tier Level
    query = f"""
            SELECT {target_profile_code}
            FROM AR_CUST
            WHERE CUST_NO = '{customer_number}'
            """
    response = db.query_db(query)
    if response is not None:
        tier = response[0][0]
    else:
        tier = None
    return tier


def reassess_tiered_pricing(start_date, end_date, log_file, demote=False):
    """Will promote and demote wholesale customers based on total spending last year"""
    print(f"Wholesale Accounts Tiered Pricing: Starting at {datetime.now():%H:%M:%S}", file=log_file)

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
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and 
    (VI_PS_TKT_HIST.POST_DAT <= ''{end_date}'')', ' (1=0) ', ' 
    (1=0) ', 0, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """

    wholesale_customers = db.query_db(query)

    if wholesale_customers is not None:
        for i in wholesale_customers:
            customer_number = i[0]
            # Check to see if the customer is still active
            valid_customer = is_current_customer(customer_number)
            if not valid_customer:
                continue
            # Valid Customer Flow
            else:
                # Set None type to empty string so it can be checked for government status
                business_name = i[1] if i[1] is not None else ''
                # Check if business is a government entity
                government = False
                for target in ['city of', 'town of', 'university', 'college', 'schools']:
                    if target in business_name.lower():
                        government = True
                if government:
                    target_pricing_tier = '2'
                # Non-Government Entity Flow
                else:
                    target_pricing_tier = 1
                    previous_pricing_tier = get_pricing_tier(customer_number)

                    # If there is no previous pricing tier, set to target tier to 1
                    if previous_pricing_tier is None:
                        set_pricing_tier(customer_number, target_pricing_tier)

                    try:
                        previous_pricing_tier = int(get_pricing_tier(customer_number))
                    except ValueError:
                        print(f"Error: {customer_number} has invalid pricing tier: {previous_pricing_tier}",
                              file=log_file)
                        continue
                    else:
                        total_sales = float(i[2])
                        # Put Customers into Categories based on sales
                        if total_sales >= 25000:
                            target_pricing_tier = 5
                        elif total_sales >= 125000:
                            target_pricing_tier = 4
                        elif total_sales >= 5000:
                            target_pricing_tier = 3
                        elif total_sales >= 1000:
                            target_pricing_tier = 2

                    # If there hasn't been a change, then continue to next iteration
                    if previous_pricing_tier == target_pricing_tier:
                        continue

                    # If demote is set to True, then demote customers
                    elif previous_pricing_tier > target_pricing_tier:
                        if not demote:
                            print(f"Demote set to false. {customer_number}: {business_name} - target tier: {target_pricing_tier} "
                                  f"but last year's sales is at level {previous_pricing_tier}.", file=log_file)
                            continue
                    # If there has been a change, set new tiered pricing level
                    set_pricing_tier(customer_number, target_pricing_tier)
                    # Get updated tier for logging
                    new_pricing_tier = get_pricing_tier(customer_number)
                    create_customer_log(customer_number, business_name, total_sales,
                                        previous_pricing_tier, new_pricing_tier)

                    # Log Change
                    print(
                        f"{customer_number}: {business_name} moved from level: "
                        f"{previous_pricing_tier} to level: {new_pricing_tier}",
                        file=log_file)

        print(f"Tiered pricing set For all wholesale accounts with history from "
              f"{start_date:%m/%d:%Y}-{end_date:%m/%d:%Y}", file=log_file)

        # Set all remaining wholesale accounts with no sales history to '1'
        query = f"""
                UPDATE AR_CUST
                SET {target_profile_code} = '1'
                WHERE CATEG_COD = 'WHOLESALE' and {target_profile_code} IS NULL
                """

        db.query_db(query, commit=True)
        print("'Level 1' set for all wholesale accounts with no sales history", file=log_file)
    else:
        print("No Wholesale Accounts Found", file=log_file)

    print(f"Wholesale Accounts Tiered Pricing: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
