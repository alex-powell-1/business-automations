from integration.shopify_api import Shopify
from integration.cp_api import HoldOrder
from integration.database import Database

from setup.error_handler import ProcessInErrorHandler

logger = ProcessInErrorHandler.logger
error_handler = ProcessInErrorHandler.error_handler


############################################################
## TODO: Add error handling and logging to all functions. ##
############################################################


# This function should be called when a draft order is created.
def on_draft_created(draft_id):
    """This function should be called when a draft order is created."""
    try:
        lines = HoldOrder.get_lines_from_draft_order(draft_id)
        cust_no = Shopify.Order.Draft.get_cust_no(draft_id)
        pl = HoldOrder.create(lines=lines, cust_no=cust_no)

        response = HoldOrder.post_pl(payload=pl)

        if (
            response is None
            or response['ErrorCode'] != 'SUCCESS'
            or response['Documents'] is None
            or len(response['Documents']) == 0
        ):
            print('Could not post document')
            return

        doc_id = response['Documents'][0]['DOC_ID']

        query = f"""
            INSERT INTO SN_DRAFT_ORDERS
            (DOC_ID, DRAFT_ID)
            VALUES
            ('{doc_id}', '{draft_id}')
        """

        return Database.db.query(query)
    except Exception as e:
        print('Something went wrong: ', e)
        return


# This function should be called when a draft order is updated.
def on_draft_updated(draft_id):
    """This function should be called when a draft order is updated."""
    query = f"""
        SELECT DOC_ID FROM SN_DRAFT_ORDERS WHERE DRAFT_ID = '{draft_id}'
    """

    response = Database.db.query(query)

    if response is None or len(response) == 0 or len(response[0]) == 0:
        print("Couldn't find order")
        return

    doc_id = response[0][0]

    query = f"""
        DELETE FROM PS_DOC_HDR
        WHERE DOC_ID = '{doc_id}'
    """

    Database.db.query(query)

    query = f"""
        DELETE FROM SN_DRAFT_ORDERS
        WHERE DOC_ID = '{doc_id}'
    """

    Database.db.query(query)

    return on_draft_created(draft_id)