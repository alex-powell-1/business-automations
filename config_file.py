import json
import os
import platform
from dotenv import load_dotenv

load_dotenv()

config = os.getenv('WIN_CONFIG_PATH') if platform.system() == 'Windows' else os.getenv('UNIX_CONFIG_PATH')
try:
    with open(config) as f:
        config_data = json.load(f)
except FileNotFoundError:
    if not platform.system() == 'Windows':
        # Local development
        os.system(f'open {os.getenv('CPSQL_FILESERVER')}')
