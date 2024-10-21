# models.py
class PromotionModel:
    def __init__(self, promo):
        self.grp_typ = promo['GRP_TYP']
        self.grp_cod = promo['GRP_COD']
        self.group_seq_no = promo['GRP_SEQ_NO']
        self.descr = promo['DESCR']
        self.cust_filt = promo['CUST_FILT']
        self.no_beg_date = True if promo['NO_BEG_DAT'] == 'Y' else False
        self.beg_date = promo['BEG_DAT']
        self.beg_time_flag = int(promo['BEG_TIM_FLG']) if promo['BEG_TIM_FLG'] else 0
        self.no_end_date = True if promo['NO_END_DAT'] == 'Y' else False
        self.end_dat = promo['END_DAT']
        self.end_time_flag = int(promo['END_TIM_FLG']) if promo['END_TIM_FLG'] else 0
        self.lst_maint_dt = promo['LST_MAINT_DT']
        self.is_enabled = True if promo['ENABLED'] == 'Y' else False
        self.mix_match_code = promo['MIX_MATCH_COD']
