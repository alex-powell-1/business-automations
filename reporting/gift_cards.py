from setup.query_engine import QueryEngine

db = QueryEngine()


def get_gift_card_liability(date):
	"""Get the total gift card liability as of a given date."""
	query = f"SELECT GFC_NO, ORIG_AMT FROM SY_GFC WHERE ORIG_DAT < '{date}"
	response = db.query_db(query=query)
	card_list = []
	total_liability = 0
	for x in response:
		card_no = x[0]
		orig_amt = x[1]
		card_list.append((card_no, orig_amt))

	for card in card_list:
		total_liability += card[1]
	print(f'Total Liability Before Redemptions: ${total_liability}')

	# Get redemptions
	for card in card_list:
		print(f'Card: {card[0]} - Original Amount: ${card[1]}')
		query = f"SELECT AMT FROM SY_GFC_ACTIV WHERE ACTIV_TYP = 'R' AND DAT < '{date}' AND GFC_NO = '{card[0]}'"
		response = db.query_db(query=query)
		redemptions = 0
		if response is not None:
			for x in response:
				print(f'\tRedemption: ${x[0]}')
				redemptions += x[0]
			print(f'\tTotal Redemptions: ${redemptions}')
			total_liability += redemptions
			print()

	print(f'Total Liability as of {date}: ${total_liability}')
