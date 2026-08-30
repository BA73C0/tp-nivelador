import socket

HEADER_SIZE = 3
MAX_MESSAGE_SIZE = 65535 - HEADER_SIZE
MAX_RETRY = 3


def recv_all(socket: socket.socket) -> bytes:
    msg, _ = _recv_all(socket)

    return msg


def _recv_all(socket: socket.socket) -> tuple[bytes, int]:
    header = _recv(socket, HEADER_SIZE)
    msg_len = (header[0] << 8) | header[1]
    end = header[2]

    msg = _recv(socket, msg_len)

    while end == 0:
        next_msg, next_end = _recv_all(socket)
        msg += next_msg
        end = next_end

    return msg, end


def _recv(socket: socket.socket, size: int) -> bytes:
    buffer = bytearray(size)
    bytes_read = 0
    retry_count = 0

    while bytes_read < size:
        if retry_count >= MAX_RETRY:
            raise ConnectionError("max retry count reached while receiving data")

        chunk = socket.recv(size - bytes_read)

        if len(chunk) == 0:
            retry_count += 1
        else:
            retry_count = 0

        buffer[bytes_read : bytes_read + len(chunk)] = chunk
        bytes_read += len(chunk)

    return bytes(buffer)


def send_all(socket: socket.socket, data: bytes) -> None:
    view = memoryview(data)
    bytes_sent = 0

    while len(view) - bytes_sent > MAX_MESSAGE_SIZE:
        _send(socket, view[bytes_sent : bytes_sent + MAX_MESSAGE_SIZE], 0)
        bytes_sent += MAX_MESSAGE_SIZE

    if bytes_sent < len(view):
        _send(socket, view[bytes_sent:], 1)


def _send(socket: socket.socket, data: memoryview, end: int) -> None:
    msg_size = len(data)
    header = bytes([msg_size >> 8, msg_size & 0xFF, end])

    _write_full(socket, memoryview(header))
    _write_full(socket, data)


def _write_full(socket: socket.socket, data: memoryview) -> None:
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
