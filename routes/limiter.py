from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from setup import creds

limiter = Limiter(key_func=get_remote_address, default_limits=[creds.API.default_rate])
