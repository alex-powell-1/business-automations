import os
from icmplib import ping

hosts = ["https://www.google.com/", "1.1.1.1", "8.8.8.8"]


def check_for_connection(hostname: str):
    host = ping(hostname, count=5, interval=0.2)
    if host.packets_sent == host.packets_received:
        print(f"{hostname} is connected.")
    else:
        print(f"{hostname} is not connected.")
    return host.packets_sent == host.packets_received


def restart_server_if_disconnected():
    if not check_for_connection(hosts[0]) and check_for_connection(hosts[1]) and check_for_connection(hosts[2]):
        print("Rebooting...")
        os.system("shutdown -t 2 -r -f")
    else:
        print("Server is connected to internet")
