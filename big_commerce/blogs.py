from setup import creds
import pandas
import requests


def bc_create_blog():
    data = pandas.read_csv('../blog.csv')
    blog_records = data.to_dict("records")
    for x in blog_records:
        body = x['body']
        title = x['title']

        url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/blog/posts"

        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        payload = {
            "title": title,
            "body": body,
            "is_published": False
        }
        response = requests.post(url=url, headers=headers, json=payload)
        print(response.text)


def bc_delete_blog_posts(blog_id):
    url = f" https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/blog/posts/{blog_id}"
    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    response = requests.delete(url=url, headers=headers)
    print(response)
