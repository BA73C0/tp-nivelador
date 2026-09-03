import socket


def recv_all(socket: socket.socket, n: int) -> bytes:
    buffer = bytearray(n)
    bytes_read = 0

    while bytes_read < n:
        chunk = socket.recv(n - bytes_read)
        if chunk == b"":
            raise ConnectionError("Socket connection closed while receiving data")
        buffer[bytes_read : bytes_read + len(chunk)] = chunk
        bytes_read += len(chunk)

    return bytes(buffer)


def send_all(socket: socket.socket, data: bytes) -> None:
    bytes_sent = 0

    while bytes_sent < len(data):
        sent = socket.send(data[bytes_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection closed while sending data")
        bytes_sent += sent
