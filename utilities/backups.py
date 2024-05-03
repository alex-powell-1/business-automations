import errno
import shutil
from datetime import datetime

from setup import creds


def offsite_backups(log_file):
    print(f"Offsite Backups: Starting at {datetime.now():%H:%M:%S}", file=log_file)

    backups = {
        "logs": {
            "src": creds.logs,
            "dst": creds.offsite_logs
        },
        "configuration": {
            "src": creds.configuration,
            "dst": creds.offsite_configuration
        }
    }

    for backup in backups:
        print(f"Starting Backup for {backup}", file=log_file)
        try:
            # For Folders
            shutil.copytree(backups[backup]['src'], backups[backup]['dst'])
        except OSError as exc:
            if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                # For Files
                shutil.copy(backups[backup]['src'], backups[backup]['dst'])
            else:
                print(f"OSError: {exc}", file=log_file)
        except Exception as err:
            print(f"Error: {err}", file=log_file)
            continue
        else:
            print(f"Finished Backup for {backup}", file=log_file)

    print(f"Offsite Backups: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
