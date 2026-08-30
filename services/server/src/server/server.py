import socket
import threading
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

_FINISH_MESSAGE = "FIN DE APUESTAS"
_BETS_FILE = "/output/bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.lottery = Lottery(_BETS_FILE)
        self.agencies_ready = 0

    def _handle_client(self, client_socket, agencies_quorum, file_lock):
        action = "handle-client"
        agency_id = None
        
        try:

            logger.info(action, logger.LogResult.in_progress)

            while True:
                client_message = safe_socket.recv_all(client_socket)

                if not client_message:
                    logger.info(
                        action,
                        logger.LogResult.success
                    )
                    return
                
                if client_message == b"FIN DE APUESTAS":
                    with agencies_quorum:
                        self.agencies_ready += 1
                        agencies_quorum.notify_all()
                    break

                bets = [str_to_bet(bet) for bet in client_message.decode("utf-8").split(";")]

                if agency_id is None:
                    agency_id = bets[0].agency_id

                with file_lock:
                    self.lottery.store_bets(bets)

            with agencies_quorum:
                while self.agencies_ready < self.agency_quorum_min:
                    agencies_quorum.wait()

            for bet in self.lottery.load_bets():
                if bet.agency_id == agency_id and self.lottery.has_won(bet):
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "winner",
                        f"{bet.first_name} {bet.last_name}",
                    )
                    safe_socket.send_all(client_socket, bet_to_str(bet).encode("utf-8"))

            safe_socket.send_all(client_socket, _FINISH_MESSAGE.encode("utf-8"))

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail
            )
            raise e

    def run(self):
        action = "accept-connection"

        clients_threads = []
        agencies_quorum = threading.Condition()
        file_lock = threading.Lock()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                thread = threading.Thread(target=self._handle_client, args=(client_socket, agencies_quorum, file_lock))
                clients_threads.append(thread)

                thread.start()

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
