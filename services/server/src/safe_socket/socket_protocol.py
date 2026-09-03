import socket

try:
    import logger
except ModuleNotFoundError:
    from services.server.src import logger
from .safe_socket import recv_all, send_all

HEADER_SIZE = 5
BYTES_HEADER_SIZE = 2
MAX_MESSAGE_SIZE = 2 ** (8 * BYTES_HEADER_SIZE) - HEADER_SIZE
MAX_RETRIES = 3

# Solo va a haber un mensaje en vuelo para simplificar el protocolo.
MAX_MSG_IN_FLIGHT = 1


class SocketPacket:
    def __init__(self, data: bytes, end: int, msg_id: int, is_ack: bool):
        self.data = data
        self.end = end
        self.msg_id = msg_id
        self.is_ack = is_ack


class SocketProtocol:
    def __init__(
        self,
        socket: socket.socket,
        max_msg_in_flight: int = MAX_MSG_IN_FLIGHT,
        max_retries: int = MAX_RETRIES,
    ):
        self.max_retries = max_retries
        self.retries = 0
        self.socket = socket
        self.in_flight = {}
        self.last_sent_id = 0
        self.last_received_id = -1
        self.max_msg_in_flight = max_msg_in_flight
        self.shutting_down = False

    def close(self) -> None:
        self.shutting_down = True

        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        self.socket.close()

    def _next_msg_id(self) -> int:
        self.last_sent_id = (self.last_sent_id + 1) % 256
        return self.last_sent_id

    def wait_for_ack(self) -> None:
        while len(self.in_flight) >= self.max_msg_in_flight:
            try:
                packet = self._recv()
                if packet.is_ack:
                    if packet.msg_id in self.in_flight:
                        del self.in_flight[packet.msg_id]
                    else:
                        raise ValueError(
                            f"Received ACK for unknown message ID: {packet.msg_id}"
                        )
                else:
                    raise ValueError(
                        f"Received non-ACK message when expecting ACK: {packet.msg_id}"
                    )

            except (TimeoutError, socket.timeout):
                if self.shutting_down:
                    raise TimeoutError("Socket is shutting down, cannot wait for ACK")

                self.retries += 1

                if self.retries > self.max_retries:
                    raise TimeoutError("Maximum retries exceeded while waiting for ACK")

                msg_id, data = next(iter(self.in_flight.items()))
                self.send_message(data, msg_id, False)

    def send_message(
        self, data: bytes, msg_id: int | None = None, wait_for_ack: bool = True
    ) -> None:
        view = memoryview(data)
        bytes_sent = 0

        if msg_id is None:
            msg_id = self._next_msg_id()

        while len(view) - bytes_sent > MAX_MESSAGE_SIZE:
            self._send(
                view[bytes_sent : bytes_sent + MAX_MESSAGE_SIZE], 0, msg_id, False
            )
            bytes_sent += MAX_MESSAGE_SIZE

        if bytes_sent < len(view):
            self._send(view[bytes_sent:], 1, msg_id, False)

        self.in_flight[msg_id] = view

        if wait_for_ack:
            self.wait_for_ack()

    def _send(self, data: memoryview, end: int, msg_id: int, is_ack: bool) -> None:
        msg_size = len(data)
        header = bytes([msg_size >> 8, msg_size & 0xFF, end, msg_id, int(is_ack)])

        send_all(self.socket, memoryview(header))
        send_all(self.socket, data)

    def recv_message(self) -> bytes:
        retries = 0

        while True:
            packet = self._recv()
            is_duplicate = packet.msg_id == self.last_received_id

            if is_duplicate:
                logger.info(
                    "recv-message",
                    logger.LogResult.fail,
                    "duplicate",
                    f"message {packet.msg_id}",
                )
                retries += 1

                if retries > self.max_retries:
                    raise TimeoutError("Too many duplicate messages received, giving up.")
            else:
                self.last_received_id = packet.msg_id

            if not packet.is_ack:
                self._send(memoryview(b""), 1, packet.msg_id, True)

                if not is_duplicate:
                    return packet.data

    def _recv(self) -> SocketPacket:
        header = recv_all(self.socket, HEADER_SIZE)
        msg_len = (header[0] << 8) | header[1]
        end = header[2]
        msg_id = header[3]
        is_ack = header[4]

        msg = recv_all(self.socket, msg_len)

        self.retries = 0

        while end == 0:
            next_packet = self._recv()

            if next_packet.msg_id != msg_id:
                raise ValueError(
                    f"Message ID mismatch: expected {msg_id}, got {next_packet.msg_id}"
                )

            msg += next_packet.data
            end = next_packet.end

        return SocketPacket(msg, end, msg_id, is_ack)
