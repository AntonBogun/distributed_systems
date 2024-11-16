#scan all ips and pick a non-127.0.0.1 ipv4 one
import psutil
import socket
import sys

def get_non_localhost_ip():
    addrs = psutil.net_if_addrs()
    for interface, addr_list in addrs.items():
        for addr in addr_list:
            if addr.family == socket.AF_INET and addr.address != "127.0.0.1":
                return addr.address
    return None

if __name__ == "__main__":
    ip_address = get_non_localhost_ip()
    if ip_address:
        print(f"{ip_address}")
    else:
        print("No non-localhost IPv4 address found", file=sys.stderr)
        exit(1)