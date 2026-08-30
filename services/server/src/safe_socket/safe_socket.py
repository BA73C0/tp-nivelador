import socket

HEADER_SIZE = 3
MAX_MESSAGE_SIZE = 65535 - HEADER_SIZE
MAX_RETRY = 3

def recv_message(socket: socket.socket) -> bytes:
    msg, _ = _recv(socket)
    
    return msg

def recv_all(socket: socket.socket, n: int) -> bytes:
    buffer = bytearray(n)
    bytes_read = 0
    retry_count = 0

    while bytes_read < n:
        if retry_count >= MAX_RETRY:
            raise ConnectionError("max retry count reached while receiving data")

        chunk = socket.recv(n - bytes_read)

        if len(chunk) == 0:
            retry_count += 1
        else:
            retry_count = 0

        buffer[bytes_read : bytes_read + len(chunk)] = chunk
        bytes_read += len(chunk)

    return bytes(buffer)

def _recv(socket: socket.socket) -> tuple[bytes, int]:
    header = recv_all(socket, HEADER_SIZE)
    msg_len = (header[0] << 8) | header[1]
    end = header[2]

    msg = recv_all(socket, msg_len)

    while end == 0:
        next_msg, next_end = _recv(socket)
        msg += next_msg
        end = next_end

    return msg, end


def send_message(socket: socket.socket, data: bytes) -> None:
    view = memoryview(data)
    bytes_sent = 0

    while len(view) - bytes_sent > MAX_MESSAGE_SIZE:
        _send(socket, view[bytes_sent : bytes_sent + MAX_MESSAGE_SIZE], 0)
        bytes_sent += MAX_MESSAGE_SIZE

    if bytes_sent < len(view):
        _send(socket, view[bytes_sent:], 1)

def send_all(socket: socket.socket, data: bytes) -> None:
    bytes_sent = 0
    retry_count = 0

    while bytes_sent < len(data):
        if retry_count >= MAX_RETRY:
            raise ConnectionError("max retry count reached while sending data")

        sent = socket.send(data[bytes_sent:])

        if sent == 0:
            retry_count += 1
        else:
            retry_count = 0

        bytes_sent += sent

def _send(socket: socket.socket, data: memoryview, end: int) -> None:
    msg_size = len(data)
    header = bytes([msg_size >> 8, msg_size & 0xFF, end])

    send_all(socket, memoryview(header))
    send_all(socket, data)
