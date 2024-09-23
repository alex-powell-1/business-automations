from integration.shopify_api import Shopify
from integration.cp_api import HoldOrder
from database import Database

from traceback import format_exc as tb

from setup.error_handler import ProcessInErrorHandler

import datetime

import threading

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
        response = Database.query(query)
        return response[0][0]
    except Exception as e:
        logger.warn(f'Error getting hold id for draft order {draft_id}: {e}')
        return


def get_draft_id(hold_id):
    logger.info(f'Getting draft id for hold order {hold_id}...')

    query = f"""
        SELECT DRAFT_ID FROM SN_DRAFT_ORDERS WHERE DOC_ID = '{hold_id}'
    """

    try:
        response = Database.query(query)
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
        Database.query(query1)
        Database.query(query2)

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
            response = Database.query(query)
            try:
                return (response[0][0], response[0][1])
            except:
                return
        except Exception as e:
            error_handler.add_error_v(
                error=f'Error getting customer number and ticket date for hold order {hold_id}: {e}',
                origin='draft_orders',
                traceback=tb(),
            )
            return

    return get_info()

    # query = f"""
    # SELECT DOC_ID FROM PS_DOC_HDR
    # WHERE
    # TKT_DT >= '{tkt_dt}' AND
    # CUST_NO = '{cust_no}' AND
    # DOC_TYP = 'T'
    # """

    # try:
    #     response = Database.query(query)

    #     logger.success(f'Doc id from hold order {hold_id} retrieved.')

    #     try:
    #         return response[0][0]
    #     except:
    #         return None
    # except Exception as e:
    #     error_handler.add_error_v(
    #         error=f'Error getting hold order id for customer {cust_no} and ticket date {tkt_dt}: {e}',
    #         origin='draft_orders',
    #         traceback=tb(),
    #     )
    #     return None


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
        response = Database.query(query)

        if response is None:
            logger.info('No hold orders found.')
            return

        logger.info(f'Found {len(response)} hold orders.')

        for row in response:
            hold_id = row[0]
            draft_id = row[1]

            doc_id = get_doc_id_from_hold_id(hold_id)

            if doc_id is not None:
                logger.info(f'Skipping draft: {draft_id}')
                continue

            logger.info(f'Deleting draft: {draft_id}')

            Shopify.Order.Draft.delete(draft_id)

            query = f"""
            DELETE FROM SN_DRAFT_ORDERS
            WHERE DOC_ID = '{hold_id}'
            """

            Database.query(query)

            logger.success(f'Associated draft deleted for hold order: {hold_id}')
    except Exception as e:
        error_handler.add_error_v(
            error=f'Error checking for closed orders: {e}', origin='draft_orders', traceback=tb()
        )


def format_date(date: str):
    date: datetime.datetime = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')

    # convert to local time
    date = date.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)

    year = date.year
    month = date.month
    day = date.day
    hour = date.hour
    minute = date.minute
    second = date.second

    year -= 2000

    hour = hour - 12 if hour > 12 else hour

    minute = str(minute).zfill(2)
    second = str(second).zfill(2)

    date = f'{month}-{day}-{year} {hour}:{minute}:{second}'

    return date


# This function should be called when a draft order is created.
def on_draft_created(draft_id, eh=ProcessInErrorHandler):
    """This function should be called when a draft order is created."""

    check_cp_closed_orders()

    eh.logger.info(f'Creating hold order for draft order {draft_id}...')

    try:
        eh.logger.info('Getting information from draft order...')

        def get_lines():
            global lines
            lines = HoldOrder.get_lines_from_draft_order(draft_id)

        def get_cust_no():
            global cust_no
            cust_no = Shopify.Order.Draft.get_cust_no(draft_id)

        lines_thread = threading.Thread(target=get_lines)
        cust_no_thread = threading.Thread(target=get_cust_no)

        lines_thread.start()
        cust_no_thread.start()

        lines_thread.join()
        cust_no_thread.join()

        def get_doc():
            global doc
            doc = HoldOrder.create(lines=lines, cust_no=cust_no)

        def get_note():
            global note
            note = Shopify.Order.Draft.get_note(draft_id)

        def get_events():
            global events
            events = Shopify.Order.Draft.get_events(draft_id)

        doc_thread = threading.Thread(target=get_doc)
        note_thread = threading.Thread(target=get_note)
        events_thread = threading.Thread(target=get_events)

        doc_thread.start()
        note_thread.start()
        events_thread.start()

        doc_thread.join()
        note_thread.join()
        doc.add_note(note)

        events_thread.join()
        for event in events:
            doc.add_note(f"[{format_date(event['createdAt'])}] {event['message']}", 'TIMELINE')

        pl = doc.get()

        eh.logger.success('Info retrieved.')
        eh.logger.info('Posting hold order...')

        def get_discount():
            global discount
            discount = Shopify.Order.Draft.get_discount(draft_id)

        def get_sub_tot():
            global sub_tot
            sub_tot = Shopify.Order.Draft.get_subtotal(draft_id)

        discount_thread = threading.Thread(target=get_discount)
        sub_tot_thread = threading.Thread(target=get_sub_tot)

        discount_thread.start()
        sub_tot_thread.start()

        discount_thread.join()
        sub_tot_thread.join()

        response = HoldOrder.post_pl(payload=pl, discount=discount, sub_tot=sub_tot)

        if (
            response is None
            or response['ErrorCode'] != 'SUCCESS'
            or response['Documents'] is None
            or len(response['Documents']) == 0
        ):
            eh.error_handler.add_error_v(error='Could not post document', origin='draft_orders')
            eh.error_handler.add_error_v(error=str(response), origin='draft_orders')
            return

        doc_id = response['Documents'][0]['DOC_ID']

        eh.logger.success(f'Posted hold order with doc id: {doc_id}')

        query = f"""
            INSERT INTO SN_DRAFT_ORDERS
            (DOC_ID, DRAFT_ID)
            VALUES
            ('{doc_id}', '{draft_id}')
        """

        return Database.query(query)
    except Exception as e:
        eh.error_handler.add_error_v(
            error=f'Error creating draft order {draft_id}: {e}', origin='draft_orders', traceback=tb()
        )
        return


# This function should be called when a draft order is updated.
def on_draft_updated(draft_id, eh=ProcessInErrorHandler):
    check_cp_closed_orders()

    eh.logger.info(f'Updating hold order for draft order {draft_id}...', origin='draft_orders-->on_draft_updated')

    """This function should be called when a draft order is updated."""
    if get_paid_status(draft_id):
        logger.info('Hold order is paid. Deleting...')
        return on_draft_completed(draft_id)

    doc_id = get_hold_id(draft_id)

    query = f"""
        DELETE FROM PS_DOC_HDR
        WHERE DOC_ID = '{doc_id}'
    """

    response = Database.query(query)
    if response['code'] == 200:
        eh.logger.success(f'Deleted PS_DOC_HDR DOC_ID: {doc_id}', origin='draft_orders-->on_draft_updated')
    elif response['code'] == 201:
        eh.logger.warn(f'PS_DOC_HDR DOC_ID:{doc_id} not found', origin='draft_orders-->on_draft_updated')
    else:
        eh.error_handler.add_error(
            error=f'Error deleting PS_DOC_HDR DOC_ID: {doc_id}: {response["message"]}', origin='draft_orders'
        )

    query = f"""
        DELETE FROM SN_DRAFT_ORDERS
        WHERE DOC_ID = '{doc_id}'
    """

    response = Database.query(query)
    if response['code'] == 200:
        eh.logger.success(
            f'Deleted hold order for draft order {draft_id}', origin='draft_orders-->on_draft_updated'
        )
    elif response['code'] == 201:
        eh.logger.warn(f'Hold order not found for draft order {draft_id}', origin='draft_orders-->on_draft_updated')
    else:
        eh.error_handler.add_error(f'Error deleting hold order for draft order {draft_id}: {response["message"]}')
    return on_draft_created(draft_id)
