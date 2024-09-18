from setup import creds
from datetime import datetime
from setup.error_handler import ProcessOutErrorHandler
from setup.utilities import PhoneNumber, convert_path_to_raw
from setup.sms_engine import SMSEngine
import subprocess


def sync_on_demand(phone_number):
    error_handler = ProcessOutErrorHandler.error_handler
    phone_number = PhoneNumber(phone_number).to_twilio()
    SMSEngine.send_text(
        origin='SERVER', campaign='SYNC_ON_DEMAND', to_phone=phone_number, message='Syncing data. Please wait...'
    )
    phone_response = None

    try:
        file = creds.BatchFiles.sync
        path = convert_path_to_raw(creds.BatchFiles.directory)
        p = subprocess.Popen(args=file, cwd=path, shell=True)
        stdout, stderr = p.communicate()

    except Exception as e:
        error_handler.add_error_v(error=f'Error: {e}', origin='sync_on_demand')
        phone_response = 'Sync failed. Please check logs.'

    else:
        phone_response = f'Sync completed successfully at {datetime.now():%m/%d/%Y %H:%M:%S}'
    finally:
        SMSEngine.send_text(origin='sync_on_demand', to_phone=phone_number, message=phone_response)
