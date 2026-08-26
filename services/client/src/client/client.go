package client

import (
	"bufio"
	"net"
	"os"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const FINISH_MESSAGE = "FIN DE APUESTAS"
const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()

	err := client.sendBets()
	if err != nil {
		logger.Error("send-bets", logger.Fail)
		return err
	}

	err = client.recvWinners()
	if err != nil {
		logger.Error("recv-winners", logger.Fail)
		return err
	}

	logger.Info("client", logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func (client *Client) sendBets() error {
	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail)
		return err
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	line := 0

	batch := make([]string, 0, client.config.BatchSize)

	for scanner.Scan() {

		record := client.config.AgencyId + "," + scanner.Text()

		if err := scanner.Err(); err != nil {
			logger.Error("read-input", logger.Fail)
			return err
		}

		batch = append(batch, record)

		if len(batch) == client.config.BatchSize {
			logger.Info("send-bet", logger.InProgress)
			if err := safe_socket.SendAll(client.conn, []byte(strings.Join(batch, ";"))); err != nil {
				logger.Error("send-batch", logger.Fail)
				return err
			}
		}

		line++
	}

	if err := safe_socket.SendAll(client.conn, []byte(strings.Join(batch, ";"))); err != nil {
		logger.Error("send-batch", logger.Fail)
		return err
	}

	messageArgs := []any{"agency-id", client.config.AgencyId, FINISH_MESSAGE}
	if err := safe_socket.SendAll(client.conn, []byte(FINISH_MESSAGE)); err != nil {
		logger.Error("send-end-message", logger.Fail, messageArgs)
		return err
	}

	return nil
}

func (client *Client) recvWinners() error {
	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail)
		return err
	}
	defer outputFile.Close()

	messageArgs := []any{"agency-id", client.config.AgencyId}

	for {
		responseBuffer, err := safe_socket.RecvAll(client.conn)
		if err != nil {
			logger.Error("recv-winners", logger.Fail, messageArgs)
			return err
		}

		if string(responseBuffer) == FINISH_MESSAGE {
			logger.Info("recv-winners", logger.Success, messageArgs...)
			break
		}

		outputFile.WriteString(string(responseBuffer) + "\n")
	}

	return nil
}
