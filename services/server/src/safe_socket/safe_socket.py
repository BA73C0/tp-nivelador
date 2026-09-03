import socket


def recv_all(socket: socket.socket, n: int) -> bytes:
    buffer = bytearray(n)
    bytes_read = 0

    while bytes_read < n:
        chunk = socket.recv(n - bytes_read)
        buffer[bytes_read : bytes_read + len(chunk)] = chunk
        bytes_read += len(chunk)

    return bytes(buffer)


def send_all(socket: socket.socket, data: bytes) -> None:
    bytes_sent = 0

    while bytes_sent < len(data):
        sent = socket.send(data[bytes_sent:])
        bytes_sent += sent
