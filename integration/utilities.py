import json
from integration.database import query_engine
import re

from datetime import datetime
from email.utils import formatdate


def convert_to_rfc2822(date: datetime):
    return formatdate(int(date.timestamp()))


def country_to_country_code(country):
    country_codes = {
        "United States": "US",
        "Canada": "CA",
        "Mexico": "MX",
        "United Kingdom": "GB",
    }

    return country_codes[country] if country in country_codes else country


def pretty_print(response):
    """Takes in a JSON object and returns an indented"""
    print(json.dumps(response, indent=4))


def get_all_binding_ids():
    db = query_engine.QueryEngine()
    """Returns a list of unique and validated binding IDs from the IM_ITEM table."""

    response = db.query_db(
        "SELECT DISTINCT USR_PROF_ALPHA_16 "
        "FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'"
        "AND USR_PROF_ALPHA_16 IS NOT NULL"
    )

    def valid(binding_id):
        return re.match(r"B\d{4}", binding_id)

    return [binding[0] for binding in response if valid(binding[0])]
