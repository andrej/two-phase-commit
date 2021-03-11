import socket
from contextlib import closing


def hostname_port_type(inp):
    if ":" in inp:
        hostname, port = inp.split(":")
    else:
        hostname = inp
        port = 12345
    return hostname, int(port)


def find_free_port():
    """Source: https://stackoverflow.com/a/45690594/602216"""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
