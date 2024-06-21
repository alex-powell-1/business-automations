import time
import base64


class Session:
    def __init__(self, password):
        # no username for this application
        # self.username = username
        self.password = password
        self.token = base64.b64encode(str(time.time()).encode()).decode()
        # Set Expiration to 1 Hour
        self.expires = time.time() + 60 * 60


SESSIONS = []
