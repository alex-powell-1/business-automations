from integration.shopify_api import Shopify
from integration.cp_api import HoldOrder
from integration.database import Database

from traceback import format_exc as tb

from setup.error_handler import ProcessInErrorHandler

logger = ProcessInErrorHandler.logger
error_handler = ProcessInErrorHandler.error_handler


def get_paid_status(draft_id):
    logger.info(f'Getting paid status for draft order {draft_id}...')
    try:
        return Shopify.Order.Draft.get(draft_id)['node']['status'] == 'COMPLETED'
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error getting paid status for draft order {draft_id}: {e}',
            origin='draft_orders',
            traceback=tb(),
        )


def get_hold_id(draft_id):
    logger.info(f'Getting hold id for draft order {draft_id}...')

    query = f"""
        SELECT DOC_ID FROM SN_DRAFT_ORDERS WHERE DRAFT_ID = '{draft_id}'
    """

    try:
        response = Database.db.query(query)
        return response[0][0]
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error getting hold id for draft order {draft_id}: {e}', origin='draft_orders', traceback=tb()
        )
        return


def get_draft_id(hold_id):
    logger.info(f'Getting draft id for hold order {hold_id}...')

    query = f"""
        SELECT DRAFT_ID FROM SN_DRAFT_ORDERS WHERE DOC_ID = '{hold_id}'
    """

    try:
        response = Database.db.query(query)
        return response[0][0]
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error getting draft id for hold order {hold_id}: {e}', origin='draft_orders', traceback=tb()
        )
        return


def delete_hold(hold_id):
    logger.info(f'Deleting hold order {hold_id}...')

    query1 = f"""
    DELETE FROM PS_DOC_HDR
    WHERE DOC_ID = '{hold_id}'
    """

    query2 = f"""
    DELETE FROM SN_DRAFT_ORDERS
    WHERE DOC_ID = '{hold_id}'
    """

    try:
        Database.db.query(query1)
        Database.db.query(query2)

        logger.success(f'Hold order {hold_id} deleted.')

        return True
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error deleting hold order {hold_id}: {e}', origin='draft_orders', traceback=tb()
        )
        return


def get_doc_id_from_hold_id(hold_id):
    logger.info(f'Getting doc id from hold order {hold_id}...')

    def get_info():
        query = f"""
        SELECT CUST_NO, TKT_DT
        FROM PS_DOC_HDR
        WHERE DOC_ID = '{hold_id}'
        """

        try:
            response = Database.db.query(query)
            return (response[0][0], response[0][1])
        except Exception as e:
            error_handler.add_error_v(
                error=f'Error getting customer number and ticket date for hold order {hold_id}: {e}',
                origin='draft_orders',
                traceback=tb(),
            )
            return

    cust_no, tkt_dt = get_info()

    query = f"""
    SELECT DOC_ID FROM PS_DOC_HDR
    WHERE
    TKT_DT >= '{tkt_dt}' AND
    CUST_NO = '{cust_no}' AND
    DOC_TYP = 'T'
    """

    try:
        response = Database.db.query(query)

        logger.success(f'Doc id from hold order {hold_id} retrieved.')

        return response[0][0]
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error getting hold order id for customer {cust_no} and ticket date {tkt_dt}: {e}',
            origin='draft_orders',
            traceback=tb(),
        )
        return None


def on_draft_completed(draft_id):
    logger.info(f'Hold order completed. Deleting hold order {draft_id}...')

    hold_id = get_hold_id(draft_id)
    delete_hold(hold_id)


def check_cp_closed_orders():
    logger.info('Checking for closed hold orders...')

    query = """
    SELECT DOC_ID, DRAFT_ID FROM SN_DRAFT_ORDERS
    """

    try:
        response = Database.db.query(query)

        if response is None:
            logger.info('No closed hold orders found.')
            return

        logger.info(f'Found {len(response)} closed hold orders.')

        for row in response:
            hold_id = row[0]
            draft_id = row[1]

            doc_id = get_doc_id_from_hold_id(hold_id)

            if doc_id is None:
                return

            Shopify.Order.Draft.delete(draft_id)
            try:
                delete_hold(hold_id)
            except:
                pass

            query = f"""
            DELETE FROM SN_DRAFT_ORDERS
            WHERE DOC_ID = '{hold_id}'
            """

            Database.db.query(query)

            logger.success(f'Associated draft deleted for hold order: {hold_id}')
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error checking for closed orders: {e}', origin='draft_orders', traceback=tb()
        )


# This function should be called when a draft order is created.
def on_draft_created(draft_id):
    check_cp_closed_orders()

    logger.info(f'Creating hold order for draft order {draft_id}...')

    """This function should be called when a draft order is created."""
    try:
        logger.info('Getting information from draft order...')

        lines = HoldOrder.get_lines_from_draft_order(draft_id)
        cust_no = Shopify.Order.Draft.get_cust_no(draft_id)
        pl = HoldOrder.create(lines=lines, cust_no=cust_no)

        logger.success('Info retrieved.')
        logger.info('Posting hold order...')

        response = HoldOrder.post_pl(payload=pl)

        if (
            response is None
            or response['ErrorCode'] != 'SUCCESS'
            or response['Documents'] is None
            or len(response['Documents']) == 0
        ):
            error_handler.add_error_v(error='Could not post document', origin='draft_orders')
            return

        doc_id = response['Documents'][0]['DOC_ID']

        logger.success('Posted hold order with doc id: {doc_id}')

        query = f"""
            INSERT INTO SN_DRAFT_ORDERS
            (DOC_ID, DRAFT_ID)
            VALUES
            ('{doc_id}', '{draft_id}')
        """

        return Database.db.query(query)
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error creating draft order {draft_id}: {e}', origin='draft_orders', traceback=tb()
        )
        return


# This function should be called when a draft order is updated.
def on_draft_updated(draft_id):
    check_cp_closed_orders()

    logger.info(f'Updating hold order for draft order {draft_id}...')

    """This function should be called when a draft order is updated."""
    if get_paid_status(draft_id):
        logger.info('Hold order is paid. Deleting...')
        return on_draft_completed(draft_id)

    doc_id = get_hold_id(draft_id)

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

    logger.success(f'Deleted hold order for draft order {draft_id}')

    return on_draft_created(draft_id)
