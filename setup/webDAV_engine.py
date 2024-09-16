import requests
from setup import creds
from requests.auth import HTTPDigestAuth
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


class WebDAVClient:
    def __init__(self, user, pw):
        self.server_url = creds.web_dav_server
        self.auth = HTTPDigestAuth(user, pw)
        self.logger = error_handler.logger
        self.error_handler = error_handler.error_handler

    def get_file(self, file_path):
        response = requests.get(f'{self.server_url}/{file_path}', auth=self.auth)
        if response.status_code == 200:
            success = True
            if response.headers['Content-Type'] == 'application/json':
                return success, response.json()
            else:
                return success, response.content
        else:
            success = False
            error_handler.error_handler.add_error_v(
                f'Failed to get file. Status Code: {response.status_code}, Response: {response.text}'
            )
            return success, response.text

    def upload_file(self, file):
        data = open(file, 'rb')
        file_name = file.split('/')[-1]
        url = self.server_url + '/content/' + file_name
        response = requests.put(url, data=data, auth=self.auth)
        if 200 <= response.status_code < 300:
            error_handler.logger.success(f'Inventory upload successful. Status Code: {response.status_code}')
        else:
            error_handler.error_handler.add_error_v(f'Inventory upload failed. Status Code: {response.status_code}')

    def update_file(self, file_path):
        return self.upload_file(file_path)  # Re-uploading a file can be used for updating

    def remove_file(self, file_name):
        url = f'{self.server_url}/{file_name}'
        response = requests.delete(url, auth=self.auth)
        return self._handle_response(response, 'remove')

    def _handle_response(self, response, action):
        if 200 <= response.status_code < 300:
            f'File {action} successful. Status Code: {response.status_code}'
        else:
            return False, f'File {action} failed. Status Code: {response.status_code}, Response: {response.text}'


class WebDAVJsonClient:
    def __init__(self, user, pw):
        self.server_url = creds.web_dav_server
        self.auth = HTTPDigestAuth(user, pw)
        self.logger = error_handler.logger
        self.error_handler = error_handler.error_handler

    def get_json_file(self, file_path):
        url = f'{self.server_url}/{file_path}'
        response = requests.get(url, auth=self.auth)
        if response.status_code == 200 and response.headers['Content-Type'] == 'application/json':
            return True, response.json()
        else:
            return False, f'Failed to get JSON file. Status Code: {response.status_code}, Response: {response.text}'

    def update_json_file(self, file_path, json_data):
        url = f'{self.server_url}/{file_path}'
        response = requests.put(url, json=json_data, auth=self.auth)
        if 200 <= response.status_code < 300:
            return True, f'JSON file updated successfully. Code: {response.status_code}'
        else:
            return (
                False,
                f'Failed to update JSON file. Status Code: {response.status_code}, Response: {response.text}',
            )

    def delete_json_file(self, file_path):
        url = f'{self.server_url}/{file_path}'
        response = requests.delete(url, auth=self.auth)
        if 200 <= response.status_code < 300:
            return True, f'JSON file deleted successfully. Code: {response.status_code}'
        else:
            return (
                False,
                f'Failed to delete JSON file. Status Code: {response.status_code}, Response: {response.text}',
            )

    def add_property(self, file_path, property_name, property_value, sub_property=None):
        success, data = self.get_json_file(file_path)
        if success:
            if sub_property:
                if property_name in data:
                    data[property_name][sub_property] = property_value
                else:
                    return False, f"Property '{property_name}' not found."
            else:
                data[property_name] = property_value

            return self.update_json_file(file_path, data)
        else:
            return False, data

    def add_object(self, file_path, object_name, object_data):
        success, data = self.get_json_file(file_path)
        if success:
            data[object_name] = object_data
            return self.update_json_file(file_path, data)
        else:
            return False, data

    def remove_property(self, file_path, property_name, sub_property=None):
        success, data = self.get_json_file(file_path)
        if success:
            if property_name in data:
                if sub_property:
                    if sub_property in data[property_name]:
                        del data[property_name][sub_property]
                    else:
                        return False, f"Sub-property '{sub_property}' not found."
                else:
                    del data[property_name]
                return self.update_json_file(file_path, data)
            else:
                return False, f"Property '{property_name}' not found."
        else:
            return False, data


if __name__ == '__main__':
    dav = WebDAVJsonClient()
    response = dav.get_json_file(creds.promotion_config)
    print(response[1]['promotions']['bogo'] == 'Buy one, get one free!')

    # response = dav.remove_property(creds.promotion_config, property_name='promotions', sub_property='Promo 4')
    # print(response)

    # # response = dav.get_json_file(creds.promotion_config)
    # # print(response)

    # response = dav.add_property(
    # 	creds.promotion_config, property_name='promotions', sub_property='Promo 5', property_value='Promo 5 Test'
    # )
    # print()
    # print()
    # response = dav.get_json_file(creds.promotion_config)
    # print(response)
