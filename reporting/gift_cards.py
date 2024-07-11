from setup.query_engine import QueryEngine

db = QueryEngine()


def get_redemptions(start_date, end_date=None, log_individually=False):
    if end_date:
        where_filter = f"DAT >= '{start_date}' AND DAT <= '{end_date}'"
    else:
        where_filter = f"DAT >= '{start_date}'"
    query = f"""SELECT GFC_NO, AMT, DAT
                FROM SY_GFC_ACTIV
                WHERE ACTIV_TYP = 'R' AND DAT >= '{start_date}' AND DAT <= '{end_date}'"""
    response = db.query_db(query=query)
    if response is None:
        return 0
    total_redemptions = 0
    for x in response:
        total_redemptions -= x[1]
    return total_redemptions


def get_gift_card_liability(start_date, end_date, log_individually=False):
    """Get the total gift card liability as of a given date."""
    print('\n\nGift Card Liability Report')
    print(f'Start Date: {start_date}')
    print(f'End Date: {end_date}')
    print('-----------------------------------')

    query = """SELECT GFC_NO, ORIG_AMT FROM SY_GFC"""

    # Step 1: Get total gift card liability.
    response = db.query_db(query=query)
    card_list = []
    total_liability = 0

    for x in response:
        card_no = x[0]
        orig_amt = x[1]
        card_list.append((card_no, orig_amt))

    for card in card_list:
        total_liability += card[1]

    # Step 2: Get redemptions prior to the start date. Deduct these from total liability.
    for card in card_list:
        if log_individually:
            print(f'Card: {card[0]} - Original Amount: ${card[1]}')

        query = f"""SELECT AMT 
                    FROM SY_GFC_ACTIV 
                    WHERE ACTIV_TYP = 'R' AND DAT < '{start_date}' AND GFC_NO = '{card[0]}'"""
        response = db.query_db(query=query)
        redemptions = 0
        if response is not None:
            for x in response:
                if log_individually:
                    print(f'\tRedemption: ${x[0]}')
                redemptions += x[0]
            if log_individually:
                print(f'\tTotal Redemptions: ${redemptions}\n')
            total_liability += redemptions

    print(f'Total Liability on {start_date}: ${total_liability}\n')

    # Get redemptions
    redemptions_during_period = 0
    for card in card_list:
        if log_individually:
            print(f'Card: {card[0]} - Original Amount: ${card[1]}')
        query = f"""SELECT AMT 
                    FROM SY_GFC_ACTIV 
                    WHERE ACTIV_TYP = 'R' AND DAT >= '{start_date}' AND DAT <= '{end_date}' AND GFC_NO = '{card[0]}'"""
        response = db.query_db(query=query)
        redemptions = 0
        if response is not None:
            for x in response:
                if log_individually:
                    print(f'\tRedemption: ${x[0]}')
                redemptions += x[0]
            if log_individually:
                print(f'\tTotal Redemptions: ${redemptions}\n')
            total_liability += redemptions
            redemptions_during_period += redemptions

    print(f'Total Redemptions During Period({start_date} - {end_date}): ${redemptions_during_period}\n')
    print(f'Total Liability After Redemptions: ${total_liability}')


get_gift_card_liability(start_date='2024-01-01', end_date='2024-03-31', log_individually=False)
get_gift_card_liability(start_date='2024-04-01', end_date='2024-06-30', log_individually=False)

# class GiftCardReport:
#     def __init__(self):
#         self.all_time_liability = 0
#         self.orig_amt = orig_amt
#         self.redemptions = 0


# print(get_redemptions(start_date='2024-01-01', end_date='2024-03-31'))
