import socket
import threading
import logger
import signal
import safe_socket
from enum import IntEnum

from lottery.bet import Bet
from lottery.lottery import Lottery

_FINISH_MESSAGE = "FIN DE APUESTAS"
_BETS_FILE = "/output/bets.csv"
_SOCKET_TIMEOUT = 1  # seconds
_MAX_RETRIES = 3  # maximum number of retries for socket operations

class ServerCodes(IntEnum):
    SUCCESS = 0
    CLIENT_DISCONNECTED = -1
    BETS_END = -2
    SHUTTING_DOWN = -3


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.lottery = Lottery(_BETS_FILE)
        self.shutting_down = False
        self.agencies_ready = 0

    def _handle_client(self, client_socket, agencies_quorum, file_lock):
        action = "handle-client"

        try:
            logger.info(action, logger.LogResult.in_progress)
            agency_id = self._recv_messages(
                client_socket, agencies_quorum, file_lock
            )

            if self._should_finish_client_handler(action, agency_id):
                return
            
            if not self._wait_agencies_quorum(agencies_quorum):
                return
            
            if not self._send_winning_bets(client_socket, agency_id, file_lock):
                return

            self._send_client_message(
                client_socket, _FINISH_MESSAGE.encode("utf-8"), action
            )

        except Exception as e:
            logger.error(action, logger.LogResult.fail)
            raise e

        finally:
            client_socket.close()

    def _should_finish_client_handler(self, action: str, agency_id: int) -> bool:
        if agency_id == ServerCodes.SHUTTING_DOWN:
            logger.info(action, logger.LogResult.fail, "interrupt", "received")
            return True
        if agency_id == ServerCodes.CLIENT_DISCONNECTED:
            logger.info(action, logger.LogResult.fail, "client", "disconnected")
            return True
        return False

    def _wait_agencies_quorum(self, agencies_quorum) -> bool:
        with agencies_quorum:
            while self.agencies_ready < self.agency_quorum_min:
                if self.shutting_down:
                    logger.info(
                        "waiting-quorum", logger.LogResult.fail, "shutting", "down"
                    )
                    return False
                agencies_quorum.wait()
        return True

    def _send_winning_bets(self, client_socket, agency_id: int, file_lock: threading.Lock) -> bool:
        action = "handle-client"

        with file_lock:
            bets = self.lottery.load_bets()

        for bet in bets:
            if bet.agency_id != agency_id or not self.lottery.has_won(bet):
                continue

            logger.info(
                action,
                logger.LogResult.success,
                "winner",
                f"{bet.first_name} {bet.last_name}",
            )
            message = bet_to_str(bet).encode("utf-8")

            if not self._send_client_message(client_socket, message, action):
                return False
            
        return True

    def _send_client_message(self, client_socket, message: bytes, action: str) -> bool:
        retries = 0

        while True:
            try:
                safe_socket.send_message(client_socket, message)
                retries = 0 
                return True
            
            except (TimeoutError, socket.timeout):
                if self.shutting_down:
                    logger.info(action, logger.LogResult.fail, "shutting", "down")
                    return False

                if retries > _MAX_RETRIES:
                    logger.info(action, logger.LogResult.fail, "retries", "exceeded")
                    return False

                retries += 1

    def _recv_messages(self, client_socket, agencies_quorum, file_lock) -> int:
        action = "handle-client"
        agency_id = None

        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                if self.shutting_down:
                    logger.info(action, logger.LogResult.fail, "shutting", "down")
                    return ServerCodes.SHUTTING_DOWN

                agency_id_batch, end_code = self._recv_client_bet_batch(
                    client_socket, agencies_quorum, file_lock
                )
                
                if agency_id_batch is not None and agency_id is None:
                    agency_id = agency_id_batch

                if end_code is not None:
                    if end_code == ServerCodes.BETS_END:
                        return agency_id
                    return end_code

        except Exception as e:
            logger.error(action, logger.LogResult.fail)
            raise e

    def _recv_client_bet_batch(self, client_socket, agencies_quorum, file_lock) -> tuple[int, int]:
        action = "receive-client-bet-batch"
        retries = 0

        while True:
            try:
                client_message = safe_socket.recv_message(client_socket)

                if not client_message:
                    logger.info(action, logger.LogResult.fail)
                    return None, ServerCodes.CLIENT_DISCONNECTED

                # Reset de la cantidad de reintentos después de recibir un mensaje exitosamente
                retries = 0 

                if client_message == b"FIN DE APUESTAS":
                    with agencies_quorum:
                        self.agencies_ready += 1
                        agencies_quorum.notify_all()
                    return None, ServerCodes.BETS_END

                bets = [str_to_bet(bet) for bet in client_message.decode("utf-8").split(";")]

                with file_lock:
                    self.lottery.store_bets(bets)

                return bets[0].agency_id, None

            except (TimeoutError, socket.timeout):
                if self.shutting_down:
                    logger.info(action, logger.LogResult.fail, "shutting", "down")
                    return None, ServerCodes.SHUTTING_DOWN

                if retries > _MAX_RETRIES:
                    logger.info(action, logger.LogResult.fail, "retries", "exceeded")
                    return None, ServerCodes.CLIENT_DISCONNECTED

                retries += 1

    def _handler_sigterm(self, signum, frame):
        logger.info("signal-handler", logger.LogResult.in_progress, "SIGTERM", "received")
        self.shutting_down = True

    def run(self):
        agencies_quorum = threading.Condition()
        file_lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handler_sigterm)

        clients_threads = self._accept_connection(
            agencies_quorum, file_lock
        )

        for thread in clients_threads:
            thread.join()

        return clients_threads

    def _accept_connection(self, agencies_quorum, file_lock) -> list[threading.Thread]:
        action = "accept-connection"
        clients_threads = []

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((self.server_host, self.server_port))
                server_socket.settimeout(_SOCKET_TIMEOUT)
                server_socket.listen()
                logger.info(action, logger.LogResult.in_progress)

                while True:
                    try:
                        logger.info(action, logger.LogResult.in_progress)
                        client_socket, _ = server_socket.accept()
                        client_socket.settimeout(_SOCKET_TIMEOUT)

                        logger.info(action, logger.LogResult.success)
                        thread = threading.Thread(
                            target=self._handle_client,
                            args=(client_socket, agencies_quorum, file_lock),
                        )
                        clients_threads.append(thread)
                        thread.start()

                    except (TimeoutError, socket.timeout):
                        if self.shutting_down:
                            logger.info(action, logger.LogResult.fail, "shutting", "down")
                            break

                        continue

                    except Exception as e:
                        logger.error(action, logger.LogResult.fail)
                        raise e

            return clients_threads

        except Exception as e:
            logger.error(action, logger.LogResult.fail)
            raise e

        finally:
            with agencies_quorum:
                agencies_quorum.notify_all()

class ReceiveResult:
    def __init__(self, agency_id: int, end_code: int) -> None:
        self.agency_id = agency_id
        self.end_code = end_code


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
