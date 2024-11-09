import socket

def is_port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False

port = 8080
if is_port_free(port):
    print(f"Port {port} is free.")
else:
    print(f"Port {port} is in use.")