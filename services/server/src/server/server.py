import socket
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

_MAX_MESSAGE_SIZE = 10000
_FINISH_MESSAGE = "FIN DE APUESTAS"
_BETS_FILE = "/output/bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(_BETS_FILE)

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        
        try:

            logger.info(action, logger.LogResult.in_progress)

            while True:
                client_message = safe_socket.recv_all(
                    client_socket, _MAX_MESSAGE_SIZE
                )

                if not client_message:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return
                
                message_amount += 1

                if client_message == b"FIN DE APUESTAS":
                    break

                self.lottery.store_bets([str_to_bet(client_message.decode("utf-8"))])

            for bet in self.lottery.load_bets():
                if self.lottery.has_won(bet):
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
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def run(self):
        action = "accept-connection"
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

                self._handle_client(client_socket)

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
