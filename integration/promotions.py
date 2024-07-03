from datetime import datetime
from setup.webDAV_engine import WebDAVJsonClient
from integration.database import Database
from setup.error_handler import ProcessOutErrorHandler
import requests
from setup import creds


class Promotions:
	def __init__(self, last_sync=None):
		self.last_sync = last_sync
		self.db = Database.db
		self.error_handler = ProcessOutErrorHandler.error_handler
		self.logger = ProcessOutErrorHandler.logger

		self.promotions = []
		self.get_promotions()

	def get_promotions(self):
		# Get list of promotions from IM_PRC_GRP
		response = self.db.query_db('SELECT GRP_COD FROM IM_PRC_GRP')
		promotions = [x[0] for x in response] if response else []
		if promotions:
			# Get promotion details from IM_PRC_GRP and IM_PRC_GRP_RUL
			for promo in promotions:
				query = f"""
                SELECT TOP 1 GRP.GRP_TYP, GRP.GRP_COD, GRP.GRP_SEQ_NO, GRP.DESCR, GRP.CUST_FILT, GRP.BEG_DAT, 
                GRP.END_DAT, GRP.LST_MAINT_DT, GRP.ENABLED, GRP.MIX_MATCH_COD, RUL.RUL_SEQ_NO
                FROM IM_PRC_GRP GRP INNER JOIN IM_PRC_RUL RUL ON GRP.GRP_COD = RUL.GRP_COD
                WHERE GRP.GRP_COD = '{promo}' and GRP.GRP_TYP = 'P'
                ORDER BY RUL.RUL_SEQ_NO DESC
                """
				response = self.db.query_db(query=query)
				promo_data = [x for x in response] if response else []
				if promo_data:
					for data in promo_data:
						self.promotions.append(self.Promotion(promo=data))

	def sync(self):
		for promotion in self.promotions:
			if promotion.lst_maint_dt > self.last_sync:
				self.logger.info(
					f'Promotion {promotion.grp_cod} has been updated since last sync. Getting updated data.'
				)
				promotion.get_price_rules()

	@staticmethod
	def bc_get_promotions(id=None):
		if id:
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions/{id}'
		else:
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions'
		response = requests.get(url, headers=creds.bc_api_headers)
		if response.status_code == 200:
			return response.json()['data']

	class Promotion:
		def __init__(self, promo):
			self.grp_typ = promo[0]
			self.grp_cod = promo[1]
			self.group_seq_no = promo[2]
			self.descr = promo[3]
			self.cust_filt = promo[4]
			self.beg_dat = promo[5]
			self.end_dat = promo[6]
			self.lst_maint_dt = promo[7]
			self.enabled = promo[8]
			self.mix_match_code = promo[9]
			self.price_rule_count = promo[10]
			self.max_uses = None
			self.price_rules = []

		def __str__(self) -> str:
			result = f'Group Code: {self.grp_cod}\n'
			result += f'Description: {self.descr}\n'
			result += f'Customer Filter: {self.cust_filt}\n'
			result += f'Begin Date: {self.beg_dat}\n'
			result += f'End Date: {self.end_dat}\n'
			result += f'Last Maintenance Date: {self.lst_maint_dt}\n'
			result += f'Enabled: {self.enabled}\n'
			result += f'Mix Match Code: {self.mix_match_code}\n'
			result += f'Price Rule Count: {self.price_rule_count}\n'
			return result

		def get_price_rules(self):
			if self.enabled == 'Y':
				print('Getting Price Rules')
				query = f"""
                SELECT RUL.GRP_TYP, RUL.GRP_COD, RUL.RUL_SEQ_NO, RUL.DESCR, RUL.CUST_FILT, RUL.ITEM_FILT, 
                RUL.SAL_FILT, RUL.IS_CUSTOM, RUL.USE_BOGO_TWOFER, RUL.REQ_FULL_GRP_FOR_BOGO_TWOFER
                FROM IM_PRC_RUL RUL
                WHERE RUL.GRP_COD = '{self.grp_cod}'
                """
				response = Database.db.query_db(query)
				if response:
					counter = 1
					for rule in response:
						self.price_rules.append(self.PriceRule(rule))
						counter += 1

		def bc_create_promotion(self):
			payload = self.create_payload()
			url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/promotions'
			response = requests.post(url, headers=creds.bc_api_headers, json=payload)
			if response.status_code == 201:
				self.logger.success(f'Promotion {self.grp_cod} created successfully.')
			else:
				self.error_handler.add_error_v(
					error=f'Error: {response.status_code}\n' f'{response.text}', origin='Promotion Creation'
				)

		def create_payload(self):
			"""C"""
			response = {
				'name': self.descr,
				'rules': [
					{
						'action': {'cart_value': {'discount': {'percentage_amount': '100'}}},
						'strategy': 'LEAST_EXPENSIVE',
						'add_free_item': False,
						'include_items_considered_by_condition': False,
						'exclude_items_on_sale': False,
						'items': {'products': [1, 2, 3]},
						'apply_once': False,
						'stop': False,
						'condition': {'cart': {'items': {'products': [1, 2, 3]}, 'minimum_quantity': 1}},
					}
				],
				'status': 'ENABLED' if self.enabled == 'Y' else 'DISABLED',
				'start_date': '2005-12-30T01:02:03+00:00',
				'end_date': '2025-12-30T01:02:03+00:00',
				'stop': False,
				'can_be_used_with_other_promotions': False,
				'currency_code': 'USD',
				'coupon_overrides_automatic_when_offering_higher_discounts': True,
				'redemption_type': 'COUPON',
			}
			if self.max_uses:
				response['max_uses'] = self.max_uses

			return response

		class PriceRule:
			def __init__(self, rule):
				self.grp_typ = rule[0]
				self.grp_cod = rule[1]
				self.rul_seq_no = rule[2]
				self.descr = rule[3]
				self.cust_filt = rule[4]
				self.item_filt = rule[5]
				self.sal_filt = rule[6]
				self.is_custom = rule[7]
				self.use_bogo_twoofer = rule[8]
				self.req_full_group_for_bogo = rule[9]
				self.price_breaks = []
				self.items = []
				self.get_price_breaks()
				self.get_items()

			def __str__(self) -> str:
				result = f'Rule Sequence Number: {self.rul_seq_no}\n'
				result += f'Description: {self.descr}\n'
				result += f'Customer Filter: {self.cust_filt}\n'
				result += f'Item Filter: {self.item_filt}\n'
				result += f'Sale Filter: {self.sal_filt}\n'
				result += f'Is Custom: {self.is_custom}\n'
				result += f'Use BOGO Twoofer: {self.use_bogo_twoofer}\n'
				result += f'Require Full Group for BOGO Twoofer: {self.req_full_group_for_bogo}\n'
				return result

			def get_price_breaks(self):
				query = f"""
                    SELECT MIN_QTY, PRC_METH, PRC_BASIS, AMT_OR_PCT
                    FROM IM_PRC_RUL_BRK
                    WHERE GRP_COD = '{self.grp_cod}' AND RUL_SEQ_NO = {self.rul_seq_no}
                    """
				response = Database.db.query_db(query)
				if response:
					for break_data in response:
						self.price_breaks.append(self.PriceBreak(break_data))

			def get_items(self):
				if self.item_filt:
					where_filter = f'WHERE {self.item_filt}'
				else:
					where_filter = ''
				query = f'SELECT ITEM_NO FROM IM_ITEM {where_filter}'
				response = Database.db.query_db(query)
				if response:
					for item in response:
						item_no = item[0]
						# print(f'Rule: {self.grp_cod} Item: {item_no}')
						self.items.append(item_no)
						pass

			class PriceBreak:
				def __init__(self, break_data):
					self.min_qty = break_data[0]
					self.prc_meth = break_data[1]
					self.prc_basis = break_data[2]
					self.amt_or_pct = break_data[3]


if __name__ == '__main__':
	promo = Promotions(last_sync=datetime(2024, 6, 1))
	promo.sync()
