import requests
from setup import creds
from requests.auth import HTTPDigestAuth


def upload_file(file, server_url):
    data = open(file, 'rb')
    file_name = file.split("/")[-1]
    url = server_url + file_name
    requests.put(url, data=data, auth=HTTPDigestAuth(creds.web_dav_user, creds.web_dav_pw))
    print(f"Inventory uploaded to {url}")
