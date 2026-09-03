package client

import (
	"bufio"
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const FINISH_MESSAGE = "FIN DE APUESTAS"

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200
const ESTIMATED_BET_SIZE = 65

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn     net.Conn
	protocol *safe_socket.SocketProtocol
	config   ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{
		conn:     conn,
		protocol: safe_socket.NewSocketProtocol(conn),
		config:   config,
	}
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	defer client.protocol.Close()

	err := client.sendBets(ctx)
	if err != nil && (err == context.Canceled || err == context.DeadlineExceeded) {
		logger.Info("send-bets", logger.Fail, "context", "canceled")
		return nil
	}

	if err != nil {
		logger.Error("send-bets", logger.Fail)
		return err
	}

	err = client.recvWinners(ctx)
	if err != nil && (err == context.Canceled || err == context.DeadlineExceeded) {
		logger.Info("recv-winners", logger.Fail, "context", "canceled")
		return nil
	}

	if err != nil {
		logger.Error("recv-winners", logger.Fail)
		return err
	}

	logger.Info("client", logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func (client *Client) sendBets(ctx context.Context) error {
	const action = "send-bets"

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail)
		return err
	}
	defer inputFile.Close()

	stopc, stop := client.setDeadlineOnCancel(ctx)
	scanner := bufio.NewScanner(inputFile)
	batch := newBetBatch(client.config.BatchSize)

	for scanner.Scan() {
		batch.append(client.config.AgencyId, scanner.Bytes())
		if !batch.isFull(client.config.BatchSize) {
			continue
		}
		if err := client.sendBatch(ctx, stop, stopc, action, batch); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action)
		if ctxErr != nil {
			return ctxErr
		}
		logger.Error("read-input", logger.Fail)
		return err
	}

	if batch.hasRecords() {
		if err := client.sendBatch(ctx, stop, stopc, action, batch); err != nil {
			return err
		}
	}

	if err := client.sendFinishMessage(ctx, stop, stopc); err != nil {
		return err
	}

	return checkContext(ctx, stop, stopc, client.conn, action)
}

func (client *Client) sendBatch(ctx context.Context, stop func() bool, stopc <-chan struct{}, action string, batch *betBatch) error {
	if err := checkDone(ctx, stop, action, batch); err != nil {
		return err
	}

	if err := client.protocol.SendMessage(batch.bytes); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action)
		if ctxErr != nil {
			return ctxErr
		}
		logger.Error(action, logger.Fail)
		return err
	}

	batch.reset()
	return nil
}

func (client *Client) sendFinishMessage(ctx context.Context, stop func() bool, stopc <-chan struct{}) error {
	const action = "send-bets"
	messageArgs := []any{"agency-id", client.config.AgencyId, FINISH_MESSAGE}

	if err := client.protocol.SendMessage([]byte(FINISH_MESSAGE)); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action)
		if ctxErr != nil {
			return ctxErr
		}
		logger.Error("send-end-message", logger.Fail, messageArgs)
		return err
	}

	return nil
}

func checkDone(ctx context.Context, stop func() bool, action string, batch *betBatch) error {
	select {
	case <-ctx.Done():
		logger.Info(action, logger.Fail, "context", "done")
		batch.clear()
		stop()
		return ctx.Err()
	default:
		return nil
	}
}

func (client *Client) recvWinners(ctx context.Context) error {
	action := "recv-winners"

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail)
		return err
	}
	defer outputFile.Close()

	stopc, stop := client.setDeadlineOnCancel(ctx)
	messageArgs := []any{"agency-id", client.config.AgencyId}

	for {
		select {
		case <-ctx.Done():
			logger.Info(action, logger.Fail, "context", "done")
			stop()
			return ctx.Err()
		default:
			responseBuffer, err := client.protocol.RecvMessage()
			if err != nil {
				ctxErr := checkContext(ctx, stop, stopc, client.conn, action)
				if ctxErr != nil {
					return ctxErr
				}
				logger.Error(action, logger.Fail, messageArgs)
				return err
			}

			if string(responseBuffer) == FINISH_MESSAGE {
				logger.Info(action, logger.Success, messageArgs...)
				return checkContext(ctx, stop, stopc, client.conn, action)
			}

			outputFile.WriteString(string(responseBuffer) + "\n")
		}
	}
}

func (client *Client) setDeadlineOnCancel(ctx context.Context) (<-chan struct{}, func() bool) {
	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		client.conn.SetDeadline(time.Now())
		close(stopc)
	})
	return stopc, stop
}

func checkContext(ctx context.Context, stop func() bool, stopc <-chan struct{}, socket net.Conn, action string) error {
	if !stop() {
		<-stopc
		socket.SetDeadline(time.Time{})
		logger.Info(action, logger.Fail, "context", "done")
		return ctx.Err()
	}

	return nil
}
