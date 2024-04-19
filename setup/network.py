import os
from icmplib import ping

hosts = ["https://www.google.com/", "1.1.1.1", "8.8.8.8"]


def check_for_connection(hostname: str, log_file):
    host = ping(hostname, count=5, interval=0.2)
    if host.packets_sent == host.packets_received:
        print(f"{hostname} is connected.", file=log_file)
    else:
        print(f"{hostname} is not connected.", file=log_file)
    return host.packets_sent == host.packets_received


def restart_server_if_disconnected(log_file):
    print("Checking for internet connection...", file=log_file)
    if (not check_for_connection(hosts[0], log_file) and check_for_connection(hosts[1], log_file)
            and check_for_connection(hosts[2], log_file)):
        print("No Internet Connection. Rebooting.", file=log_file)
        print("-----------------------", file=log_file)
        log_file.close()
        os.system("shutdown -t 2 -r -f")
    else:
        print("Server is connected to internet. Will continue.", file=log_file)
        print("-----------------------", file=log_file)
