import json


def country_to_country_code(country):
    country_codes = {
        "United States": "US",
        "Canada": "CA",
        "Mexico": "MX",
        "United Kingdom": "GB"
    }

    return country_codes[country] if country in country_codes else country


def pretty_print(response):
    """Takes in a JSON object and returns an indented"""
    print(json.dumps(response, indent=4))
