from database import Database as db


def get_redemptions(card_no, start_date, end_date=None, before=False, log_individually=False):
    """Get the total redemptions for a given date range. If before, then it will get all redemptions before the start date."""
    if before:
        where_filter = f"DAT < '{start_date}'"
    else:
        if end_date:
            where_filter = f"DAT >= '{start_date}' AND DAT <= '{end_date}'"
        else:
            where_filter = f"DAT >= '{start_date}'"

    query = f"""SELECT GFC_NO, AMT, DAT
                FROM SY_GFC_ACTIV
                WHERE ACTIV_TYP = 'R' AND {where_filter} AND GFC_NO = '{card_no}'"""

    response = db.query(query=query)
    if response is None:
        return 0
    total_redemptions = 0
    for x in response:
        total_redemptions -= x[1]
    return total_redemptions


def get_all_gift_cards():
    query = """SELECT GFC_NO, ORIG_AMT FROM SY_GFC"""
    response = db.query(query=query)
    return [{'card_no': x[0], 'orig_amt': x[1]} for x in response] if response else []


def get_gift_card_liability(start_date, end_date, log_individually=False):
    """Get the total gift card liability as of a given date."""
    print('\n\nGift Card Liability Report')
    print(f'Start Date: {start_date}')
    print(f'End Date: {end_date}')
    print('-----------------------------------')
    # Step 1: Get all gift cards and calculate total liability
    card_list = get_all_gift_cards()
    all_time_gift_card_liability = 0

    for card in card_list:
        all_time_gift_card_liability += card['orig_amt']

    # Step 2: Get redemptions prior to the start date. Deduct these from total liability.
    redemptions_prior_to_start_date = 0

    for card in card_list:
        redemptions = get_redemptions(
            card_no=card['card_no'], start_date=start_date, before=True, log_individually=log_individually
        )
        redemptions_prior_to_start_date += redemptions

    opening_liability = all_time_gift_card_liability - redemptions_prior_to_start_date

    print(f'Total Liability on {start_date}: ${opening_liability}')

    # Get redemptions
    redemptions_during_period = 0
    for card in card_list:
        redemptions = get_redemptions(
            card_no=card['card_no'], start_date=start_date, end_date=end_date, log_individually=log_individually
        )
        redemptions_during_period += redemptions

    print(f'Total Redemptions During Period({start_date} - {end_date}): ${redemptions_during_period}')
    print(f'Total Liability After Redemptions: ${opening_liability - redemptions_during_period}')
