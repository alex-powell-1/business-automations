import json

def pretty_print(response):
    return json.dumps(response.json(), indent=4)