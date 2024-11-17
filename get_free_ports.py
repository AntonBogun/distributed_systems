import argparse
import socket
import sys

def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))  # Bind to a free port provided by the OS
    port = s.getsockname()[1]  # Get the port number
    s.close()  # Close the socket
    return port

def get_free_ports(n):
    ports = []
    for _ in range(min(n, 16)):
        ports.append(get_free_port())
    return ports

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Print n (at most 16) free ports.')
    parser.add_argument('n', type=int, help='Number of free ports to find')
    args = parser.parse_args()

    if args.n < 1:
        print("Number of free ports must be at least 1", file=sys.stderr)
        exit(1)
    if args.n > 16:
        print("Number of free ports must be at most 16", file=sys.stderr)
        exit(1)
    free_ports = get_free_ports(args.n)
    if len(free_ports) == 0:
        print("No free ports found", file=sys.stderr)
        exit(1)
    if len(free_ports) < args.n:
        print(f"Found only {len(free_ports)} free ports", file=sys.stderr)
        exit(1)
    for port in free_ports:
        print(port)