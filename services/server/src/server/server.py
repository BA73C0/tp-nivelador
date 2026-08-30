import socket
import threading
import logger
import signal
import safe_socket
from enum import IntEnum
from selectors import DefaultSelector, EVENT_READ

from lottery.bet import Bet
from lottery.lottery import Lottery

_FINISH_MESSAGE = "FIN DE APUESTAS"
_BETS_FILE = "/tmp/bets.csv"
_CLIENT_SOCKET_TIMEOUT = 1  # seconds

class ServerCodes(IntEnum):
    SUCCESS = 0
    CLIENT_DISCONNECTED = -1
    BETS_END = -2
    INTERRUPT_READ = -3


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.lottery = Lottery(_BETS_FILE)
        self.shutting_down = False
        self.agencies_ready = 0
        self.servers_interrupt_sockets = []

    def _handle_client(self, client_socket, agencies_quorum, file_lock, interrupt_read):
        action = "handle-client"
        agency_id = None
        
        try:

            logger.info(action, logger.LogResult.in_progress)

            agency_id = self._recv_messages(client_socket, agencies_quorum, file_lock, interrupt_read)

            if agency_id == ServerCodes.INTERRUPT_READ:
                logger.info(
                    action, logger.LogResult.fail, "interrupt", "received"
                )
                return
            elif agency_id == ServerCodes.CLIENT_DISCONNECTED:
                logger.info(
                    action, logger.LogResult.fail, "client", "disconnected"
                )
                return

            with agencies_quorum:
                while self.agencies_ready < self.agency_quorum_min:
                    if self.shutting_down:
                        logger.info(
                            "waiting-quorum", logger.LogResult.fail, "shutting", "down"
                        )
                        return
                    agencies_quorum.wait()

            for bet in self.lottery.load_bets():
                if bet.agency_id == agency_id and self.lottery.has_won(bet):
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "winner",
                        f"{bet.first_name} {bet.last_name}",
                    )
                    try:
                        safe_socket.send_message(client_socket, bet_to_str(bet).encode("utf-8"))
                    except (TimeoutError, socket.timeout):
                        if self.shutting_down:
                            logger.info(
                                action, logger.LogResult.fail, "shutting", "down"
                            )
                            return
                        logger.info(
                            action, logger.LogResult.fail, "client", "timeout"
                        )
                        return

            try:
                safe_socket.send_message(client_socket, _FINISH_MESSAGE.encode("utf-8"))
            except (TimeoutError, socket.timeout):
                if self.shutting_down:
                    logger.info(
                        action, logger.LogResult.fail, "shutting", "down"
                    )
                    return
                logger.info(
                    action, logger.LogResult.fail, "client", "timeout"
                )
                return

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail
            )
            raise e

        finally:
            client_socket.close()
            interrupt_read.close()

    def _recv_messages(self, client_socket, agencies_quorum, file_lock, interrupt_read) -> int:
        action = "handle-client"
        agency_id = None

        sel = DefaultSelector()
        sel.register(interrupt_read, EVENT_READ)
        sel.register(client_socket, EVENT_READ)
        
        try:

            logger.info(action, logger.LogResult.in_progress)

            while True:
                for key, _ in sel.select():
                    with agencies_quorum:
                        if self.shutting_down:
                            logger.info(
                                action, logger.LogResult.fail, "shutting", "down"
                            )
                            return ServerCodes.INTERRUPT_READ
                
                    if key.fileobj == interrupt_read:
                        logger.info(
                            action, logger.LogResult.fail, "interrupt", "received"
                        )
                        interrupt_read.recv(1)
                        return ServerCodes.INTERRUPT_READ
                    
                    if key.fileobj == client_socket:
                        try:
                            agency, end_code = self._recv_client_bet_batch(client_socket, agencies_quorum, file_lock)
                        except (TimeoutError, socket.timeout):
                            if self.shutting_down:
                                logger.info(
                                    action, logger.LogResult.fail, "shutting", "down"
                                )
                                return ServerCodes.INTERRUPT_READ
                            logger.info(
                                action, logger.LogResult.fail, "client", "timeout"
                            )
                            return ServerCodes.CLIENT_DISCONNECTED

                        if agency_id is None and agency is not None:
                            agency_id = agency

                        if end_code is not None and end_code == ServerCodes.BETS_END:
                            return agency_id
                        elif end_code is not None and end_code == ServerCodes.CLIENT_DISCONNECTED:
                            logger.info(
                                action, logger.LogResult.fail, "client", "disconnected"
                            )
                            return end_code

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail
            )
            raise e

        finally:
            sel.unregister(interrupt_read)
            sel.unregister(client_socket)
            sel.close()

    def _recv_client_bet_batch(self, client_socket, agencies_quorum, file_lock) -> tuple[int, int]:
        client_message = safe_socket.recv_message(client_socket)
        action = "receive-client-bet-batch"
        agency_id = None
        
        if not client_message:
            logger.info(
                action,
                logger.LogResult.fail
            )
            return None, ServerCodes.CLIENT_DISCONNECTED
        
        if client_message == b"FIN DE APUESTAS":
            with agencies_quorum:
                self.agencies_ready += 1
                agencies_quorum.notify_all()
            return None, ServerCodes.BETS_END

        bets = [str_to_bet(bet) for bet in client_message.decode("utf-8").split(";")]

        if agency_id is None:
            agency_id = bets[0].agency_id

        with file_lock:
            self.lottery.store_bets(bets)

        return agency_id, None

    def _handler_sigterm(self, signum, frame):
        logger.info("signal-handler", logger.LogResult.in_progress, "SIGTERM", "received")
        for interrupt_write in self.servers_interrupt_sockets:
            interrupt_write.send(b'\0')

    def run(self):
        agencies_quorum = threading.Condition()
        file_lock = threading.Lock()

        interrupt_read, interrupt_write = socket.socketpair()

        self.servers_interrupt_sockets.append(interrupt_write)

        signal.signal(signal.SIGTERM, self._handler_sigterm)

        clients_threads = self._accept_connection(agencies_quorum, file_lock, interrupt_read)

        for thread in clients_threads:
            thread.join()

        for interrupt_write in self.servers_interrupt_sockets:
            interrupt_write.close()

        return clients_threads

    def _accept_connection(self, agencies_quorum, file_lock, interrupt_read) -> list[threading.Thread]:
        action = "accept-connection"
        clients_threads = []

        sel = DefaultSelector()
        sel.register(interrupt_read, EVENT_READ)

        try:

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                sel.register(server_socket, EVENT_READ)
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen()
                
                logger.info(action, logger.LogResult.in_progress)

                while True:
                    for key, _ in sel.select():
                        if key.fileobj == interrupt_read:
                            logger.info(
                                action, logger.LogResult.fail, "interrupt", "received"
                            )
                            interrupt_read.recv(1)
                            sel.unregister(server_socket)
                            return clients_threads
                        
                        if key.fileobj == server_socket:
                            try:
                                logger.info(action, logger.LogResult.in_progress)
                                client_socket, _ = server_socket.accept()
                                client_socket.settimeout(_CLIENT_SOCKET_TIMEOUT)
                            except Exception as e:
                                sel.unregister(server_socket)
                                logger.error(action, logger.LogResult.fail)
                                raise e
                            
                            interrupt_read_thread, interrupt_write = socket.socketpair()
                            self.servers_interrupt_sockets.append(interrupt_write)
            
                            logger.info(action, logger.LogResult.success)

                            thread = threading.Thread(target=self._handle_client, args=(client_socket, agencies_quorum, file_lock, interrupt_read_thread))
                            clients_threads.append(thread)
            
                            thread.start()

            return clients_threads
        
        except Exception as e:
            logger.error(action, logger.LogResult.fail)
            raise e

        finally:
            sel.unregister(interrupt_read)
            sel.close()
            interrupt_read.close()
            server_socket.close()

            # Aviso a todos los hilos que están esperando en agencies_quorum que deben terminar
            with agencies_quorum:
                self.shutting_down = True
                agencies_quorum.notify_all()


def str_to_bet(bet_str: str):
    bet_data = bet_str.split(",")
    return Bet(
        int(bet_data[0]),
        bet_data[1],
        bet_data[2],
        int(bet_data[3]),
        bet_data[4],
        int(bet_data[5]),
    )

def bet_to_str(bet: Bet):
    return f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
