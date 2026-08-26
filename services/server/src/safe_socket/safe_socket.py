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


def send_all(socket: socket.socket, bytes: bytes) -> None:
    while len(bytes) > MAX_MESSAGE_SIZE:
        _send(socket, bytes[:MAX_MESSAGE_SIZE], 0)
        bytes = bytes[MAX_MESSAGE_SIZE:]

    if len(bytes) > 0:
        _send(socket, bytes, 1)


def _send(socket: socket.socket, bytes: bytes, end: int) -> None:
    size = len(bytes) + HEADER_SIZE
    msg_size = len(bytes)
    msg = bytearray()
    msg.append(msg_size >> 8)
    msg.append(msg_size & 0xFF)
    msg.append(end)
    msg.extend(bytes)

    bytes_sent = 0
    retry_count = 0

    while bytes_sent < size:
        if retry_count >= MAX_RETRY:
            raise ConnectionError("max retry count reached while sending data")

        sent = socket.send(msg[bytes_sent:])

        if sent == 0:
            retry_count += 1
        else:
            retry_count = 0

        bytes_sent += sent
