from database import Database as db
from setup import creds
from setup.date_presets import *


def get_top_items(start_date, end_date, number_of_items):
    query = f"""
    "{creds.SQL.DATABASE}"."dbo"."USP_RPT_SA_BY_X";1 
    'select distinct IM_ITEM.ITEM_NO as GRP_ID, IM_ITEM.DESCR as GRP_DESCR 
    from IM_ITEM where ( (1=1) ) 
    union 
    select distinct VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, NULL as GRP_DESCR 
    from %HISTORY% where not exists(select 1 from IM_ITEM 
    where IM_ITEM.ITEM_NO = VI_PS_TKT_HIST_LIN.ITEM_NO) and ( (1=1) ) and ( (1=1) ) and %ANYPERIODFILTER%', 
    'select VI_PS_TKT_HIST_LIN.ITEM_NO as GRP_ID, %HISTCOLUMNS% from %HISTORY% 
    where ( (1=1) ) and ( (1=1) ) and %PERIODFILTER%', ' 
    (VI_PS_TKT_HIST.POST_DAT >= ''{start_date}'') and (VI_PS_TKT_HIST.POST_DAT <= ''{end_date}'')', ' (1=0) ', ' 
    (1=0) ', {number_of_items}, 0, 'SLS_EXT_PRC_A - RTN_EXT_PRC_VALID_A - RTN_EXT_PRC_NONVALID_A', 2
    """

    results = db.query(query)
    result_list = []
    if results is not None:
        for x in results:
            result_list.append(x[0])
        return result_list
    else:
        return


def get_product_details(item_number, parent=False):
    if parent:
        query = f"""
                SELECT item.item_no, item.LONG_DESCR, item.USR_ALWAYS_ONLINE, inv.QTY_AVAIL
                FROM IM_ITEM item
                INNER JOIN IM_INV inv on inv.ITEM_NO=item.ITEM_NO
                WHERE item.USR_PROF_ALPHA_16 = '{item_number}'
                """
        return (db.query(query)[0][0], db.query(query)[0][1], db.query(query)[0][2], int(db.query(query)[0][3]))
    else:
        query = f"""
        SELECT item.item_no, item.LONG_DESCR, item.USR_ALWAYS_ONLINE, inv.QTY_AVAIL
        FROM IM_ITEM item
        INNER JOIN IM_INV inv ON inv.ITEM_NO=item.ITEM_NO
        WHERE item.ITEM_NO = '{item_number}'
        """
        return (db.query(query)[0][0], db.query(query)[0][1], db.query(query)[0][2], int(db.query(query)[0][3]))


def get_binding_id(list_of_items):
    """Get List of Binding IDs for given list of product_tools"""
    binding_id_list = []
    for x in list_of_items:
        query = f"""
        SELECT USR_PROF_ALPHA_16
        FROM IM_ITEM
        WHERE ITEM_NO = '{x}' and USR_PROF_ALPHA_16 IS NOT NULL
        """
        result = db.query(query)
        if result is not None:
            binding_id_list.append(result[0][0])
        else:
            continue
    return binding_id_list
