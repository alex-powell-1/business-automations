from datetime import datetime

from customer_tools.customers import is_current_customer, Customer
from setup import creds
from database import Database as db
from setup.error_handler import ScheduledTasksErrorHandler as error_handler

console_logging = True


def get_government_customers():
    query = """
    SELECT CUST_NO
    FROM AR_CUST
    WHERE CATEG_COD = 'WHOLESALE' and 
    (NAM like '%town of%' or 
    NAM like '%city of%' or 
    NAM like '%university%' or
    NAM like '%schools%' or
    NAM like '%college')"""
    response = db.query(query)
    if response is not None:
        return [i[0] for i in response]


def set_government_pricing_tier():
    query = """
    UPDATE AR_CUST
    SET PROF_ALPHA_1 = '2'
    WHERE CATEG_COD = 'WHOLESALE' and 
    (NAM like '%town of%' or 
    NAM like '%city of%' or 
    NAM like '%university%' or
    NAM like '%schools%' or
    NAM like '%college')"""
    db.query(query)


def reassess_tiered_pricing(start_date, end_date, demote=False):
    """Will promote and demote wholesale customers based on total spending last year"""
    error_handler.logger.info(f'Wholesale Accounts Tiered Pricing: Starting at {datetime.now():%H:%M:%S}')
    # Step 1: Set all wholesale accounts with no price tier to '1'. This will be the default tier and will correct
    # any accounts that have been missed during account creation.

    correct_null_query = """
    UPDATE AR_CUST
    SET PROF_ALPHA_1 = '1'
    WHERE CATEG_COD = 'WHOLESALE' and (PROF_ALPHA_1 IS NULL or PROF_ALPHA_1 = '')
    """
    db.query(correct_null_query)
    error_handler.logger.info("All Wholesale Accounts with no pricing tier set to '1'")

    # Set all government customers to pricing tier 2
    set_government_pricing_tier()
    error_handler.logger.info('All Government Customers set to Pricing Tier 2')

    # Step 2: Get all wholesale customers with sales history during the period
    error_handler.logger.info(f'Assessment Start Date: {start_date:%m/%d/%Y}')
    error_handler.logger.info(f'Assessment End Date: {end_date:%m/%d/%Y}')

    sales_history_query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1 
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

    wholesale_customers_during_period = db.query(sales_history_query)

    if wholesale_customers_during_period is not None:
        for i in wholesale_customers_during_period:
            customer_number = i[0]

            # Check to see if the customer is still active. If not, skip.
            if not is_current_customer(customer_number):
                continue

            # Valid Customers
            else:
                # create customer object for each customer
                customer = Customer(customer_number)

                # Check if this is a government customer. If so, skip
                if customer.number in get_government_customers():
                    # print(f"{customer.name}({customer_number}) is a government cust. "
                    #       f"Level: {customer.pricing_tier}. Skipping...", file=log_file)
                    continue

                # Check if this is a pricing tier 1 customer. If so, skip. Level 1 doesn't promote or demote.
                if customer.pricing_tier == 1:
                    # print(f"{customer.name}({customer_number}) is at level 1. Skipping...", file=log_file)
                    continue
                # All other customers are valid for tiered pricing
                else:
                    # Get customer sales data
                    total_sales = float(i[2])
                    # base case
                    target_pricing_tier = 2
                    # If there is no previous pricing tier, set to target tier to 1
                    if customer.pricing_tier is None:
                        customer.set_pricing_tier(target_tier=target_pricing_tier)
                    # Put Customers into Categories based on sales
                    if total_sales >= 25000:
                        target_pricing_tier = 5
                    elif total_sales >= 15000:
                        target_pricing_tier = 4
                    elif total_sales >= 5000:
                        target_pricing_tier = 3
                    # print(customer_number, business_name, current_pricing_tier, target_pricing_tier)
                    # If there hasn't been a change, then continue to next iteration
                    if customer.pricing_tier == target_pricing_tier:
                        continue
                    # Demote Customer Tier
                    elif customer.pricing_tier > target_pricing_tier:
                        # If demote is set to True, then demote customers. This will only be set to True on
                        # reassessment of last year's sales at the beginning of the year.
                        # Generally, it will run on false.
                        # print(f"Target for demotion found! "
                        #       f"{customer.name}(Cust No: {customer_number})", file=log_file)
                        if not demote:
                            # print(f"Demote set to false. Skipping demotion", file=log_file)
                            continue
                        else:
                            # Demote Customer Tier
                            customer.set_pricing_tier(target_tier=target_pricing_tier)
                            error_handler.logger.info(
                                f'Demoting {customer.name}(Cust No: {customer_number} from level: '
                                f'{customer.pricing_tier} to level: {target_pricing_tier}'
                            )
                    else:
                        # Promote Customer Tier
                        customer.set_pricing_tier(target_tier=target_pricing_tier)

        error_handler.logger.info(
            f'Tiered pricing set For all wholesale accounts with history from '
            f'{start_date:%m/%d:%Y}-{end_date:%m/%d:%Y}'
        )

    else:
        error_handler.logger.warn('No Wholesale Accounts Found')

    error_handler.logger.info(f'Wholesale Accounts Tiered Pricing: Completed at {datetime.now():%H:%M:%S}')


if __name__ == '__main__':
    cust_nos = get_government_customers()
    for x in cust_nos:
        print(x)
