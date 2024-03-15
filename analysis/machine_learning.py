from setup.query_engine import QueryEngine
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

db = QueryEngine()


def get_tickets():
    query = """
    SELECT TKT_NO
    FROM PS_TKT_HIST
    ORDER BY TKT_DT DESC
    """
    response = db.query_db(query)
    if response is not None:
        tickets = []
        for x in response:
            tickets.append(x[0])
        return tickets


def get_ticket_items(ticket_number):
    query = f"""
    SELECT ITEM_NO FROM
    PS_TKT_HIST_LIN
    WHERE TKT_NO = '{ticket_number}'
    """
    response = db.query_db(query)
    if response is not None:
        ticket_items = []
        for x in response:
            ticket_items.append(x[0])
        return ticket_items


def get_distinct_items():
    query = """
    SELECT DISTINCT TOP 10 ITEM_NO
    FROM PS_TKT_HIST_LIN
    """
    response = db.query_db(query)
    if response is not None:
        distinct_items = []
        for x in response:
            distinct_items.append(x[0])
        return distinct_items


def get_tickets_with_item(item):
    query = f"""
    SELECT TKT_NO
    FROM PS_TKT_HIST_LIN
    WHERE ITEM_NO = '{item}'"""
    response = db.query_db(query)
    if response is not None:
        tickets = []
        for x in response:
            tickets.append(x[0])
        return tickets


def ticket_dataset():

    distinct_items = get_distinct_items()
    # for item in distinct_items:
    for item in ['10200']:
        print(f"ITEM NUMBER: {item}")
        dataset = []
        tickets_with_item = get_tickets_with_item(item)
        for ticket in tickets_with_item:
            ticket_items = get_ticket_items(ticket)
            # ticket_items.remove(item)
            dataset.append(ticket_items)
        transactions = dataset
        encoder = TransactionEncoder()
        encoded_array = encoder.fit(transactions).transform(transactions)
        df_itemsets = pd.DataFrame(encoded_array, columns=encoder.columns_)
        frequent_itemsets = apriori(df_itemsets, min_support=0.06, use_colnames=True)
        frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda itemset: len(itemset))
        return frequent_itemsets[frequent_itemsets['length'] >= 2]

print(ticket_dataset())
