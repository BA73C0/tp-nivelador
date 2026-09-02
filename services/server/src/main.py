import os
import sys

import logger
import server

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
AGENCY_QUORUM_MIN = int(os.environ["AGENCY_QUORUM_MIN"])

BETS_FILE_VOLUME_PATH = "/output/bets.csv"
BETS_FILE_FALLBACK_PATH = "/tmp/bets.csv"


def bets_file_path() -> str:
    configured_path = os.getenv("BETS_FILE")
    if configured_path:
        return configured_path

    if os.path.isdir(os.path.dirname(BETS_FILE_VOLUME_PATH)):
        return BETS_FILE_VOLUME_PATH

    return BETS_FILE_FALLBACK_PATH


def main():
    logger.init()
    s = server.Server(SERVER_HOST, SERVER_PORT, AGENCY_QUORUM_MIN, bets_file_path())
    try:
        s.run()
    except Exception as e:
        logger.error("server-run", logger.LogResult.fail, "err", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
