from setup.webDAV_engine import WebDAVJsonClient
from integration.database import Database


class Promotions:
	def __init__(self):
		self.db = Database.db
		self.promotions = []
		self.get_promotions()
		print(self.promotions)

	def get_promotions(self):
		# Get list of promotions from IM_PRC_GRP
		response = self.db.query_db('SELECT GRP_COD FROM IM_PRC_GRP')
		promotions = [x[0] for x in response] if response else []
		if promotions:
			for promotion in promotions:
				self.promotions.append(self.Promotion(promotion))

	def get_promotion(self, grp_cod):
		for promotion in self.promotions:
			if promotion.grp_cod == grp_cod:
				return promotion
		return None

	class Promotion:
		def __init__(self, grp_cod):
			self.grp_cod = grp_cod
			self.description = None
			self.start_date = None
			self.end_date = None
			self.enabled = False
			self.mix_match_code = None
			self.group_type = None
			self.group_seq_no = None
			self.items = []

		class PromotionLineItem:
			def __init__(self, rule_sequence_number):
				self.grp_type = None
				self.grp_cod = None
				self.rul_seq_no = rule_sequence_number
				self.descr = None
				self.descr_upr = None
				self.cust_filt = None
				self.item_filt = None
				self.sal_filt = None
				self.lst_maint_dt = None
				self.lst_maint_usr_id = None
				self.is_custom = None
				self.cust_no = None
				self.item_no = None
				self.price_break_descr = None
				self.user_bogo_twoofer = None
				self.require_full_group_for_bogo = None
				# From IM_PRC_RUL_BRK
				self.min_qty = None
				self.prc_meth = None  # Price Method: D for Discount, F for Fixed Price
				self.prc_basis = None  # Price Basis: 1 for price 1, ! for none
				self.amt_or_pct = None  # Amount or Percentage: number


promo = Promotions()
