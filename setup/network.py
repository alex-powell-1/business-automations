import os
from icmplib import ping
import requests
from setup import sms_engine
from setup import creds
from datetime import datetime

hosts = ["https://www.google.com/", "1.1.1.1", "8.8.8.8"]


def check_for_connection(hostname: str, log_file):
    host = ping(hostname, count=5, interval=0.2)
    if host.packets_sent == host.packets_received:
        print(f"{hostname} is connected.", file=log_file)
    else:
        print(f"{hostname} is not connected.", file=log_file)
    return host.packets_sent == host.packets_received


def restart_server_if_disconnected(log_file):
    print(f"Business Automation Health Check: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    if (not check_for_connection(hosts[0], log_file) and check_for_connection(hosts[1], log_file)
            and check_for_connection(hosts[2], log_file)):
        print("No Internet Connection. Rebooting.", file=log_file)
        print("-----------------------", file=log_file)
        log_file.close()
        os.system("shutdown -t 2 -r -f")
    else:
        print("Server is connected to internet. Will continue.", file=log_file)
        print(f"Business Automation Health Check: Completed at {datetime.now():%H:%M:%S}", file=log_file)
        print("-----------------------", file=log_file)


def health_check(log_file):
    print(f"Flask Server Health Check: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    url = f"{creds.ngrok_domain}/health"
    response = requests.get(url=url)
    if response.status_code != 200:
        print(f"Flask server is not running. Restart the server: {creds.flask_server_name}", file=log_file)
        sms = sms_engine.SMSEngine()
        sms.send_text("none",
                      to_phone=creds.my_phone,
                      message=f"Flask server is not running. Restart the server: {creds.flask_server_name}",
                      log_location=creds.sms_utility_log,
                      create_log=False,
                      test_mode=False)
    else:
        print("Flask server is running.", file=log_file)

    print(f"Flask Server Health Check: Completed at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
