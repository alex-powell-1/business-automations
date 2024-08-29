import json
import requests
from setup import creds


class Webhooks:
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/hooks'
    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    def get(id='', ids_only=False):
        response = requests.get(f'{Webhooks.url}/{id}', headers=Webhooks.headers)
        if response.status_code == 200:
            if ids_only:
                return [hook['id'] for hook in response.json()['data']]

            return json.dumps(response.json(), indent=4)

    def create(scope, destination):
        payload = {'scope': scope, 'destination': destination}
        response = requests.post(Webhooks.url, json=payload, headers=Webhooks.headers)
        print(response.content, response.status_code)
        if response.status_code == 200:
            return json.dumps(response.json(), indent=4)

    def update(hook_id):
        url = f'{Webhooks.url}/{hook_id}'
        payload = {'destination': creds.api_endpoint + '/bc'}
        response = requests.put(url=f'{Webhooks.url}/{hook_id}', json=payload, headers=Webhooks.headers)
        print(url, response.content, response.status_code)
        if response.status_code == 200:
            return json.dumps(response.json(), indent=4)

    def delete(hook_id=None, all=False):
        if all:
            hook_ids = Webhooks.get(ids_only=True)
            for hook in hook_ids:
                response = requests.delete(url=f'{Webhooks.url}/{hook}', headers=Webhooks.headers)
                if response.status_code == 200:
                    print(f'Webhook {hook_id} deleted')
            return 'All webhooks deleted'
        elif hook_id:
            response = requests.delete(url=f'{Webhooks.url}/{hook_id}', headers=Webhooks.headers)
            if response.status_code == 200:
                return f'Webhook {hook_id} deleted'


if __name__ == '__main__':
    print(Webhooks.get())
