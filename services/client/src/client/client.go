package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const FINISH_MESSAGE = "FIN DE APUESTAS"
const ACK_MESSAGE = "ACK"

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200
const ESTIMATED_BET_SIZE = 65
const BATCH_RATIO_ACK = 1

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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	defer client.conn.Close()

	err := client.sendBets(ctx)
	if err != nil && (err == context.Canceled || err == context.DeadlineExceeded) {
		logger.Info("send-bets", logger.Fail, "context", "canceled")
		client.conn.Close()
		return nil
	}

	if err != nil {
		logger.Error("send-bets", logger.Fail)
		return err
	}

	err = client.recvWinners(ctx)
	if err != nil && (err == context.Canceled || err == context.DeadlineExceeded) {
		logger.Info("recv-winners", logger.Fail, "context", "canceled")
		client.conn.Close()
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
	batchesSent := 0

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail)
		return err
	}
	defer inputFile.Close()

	stopc, stop := client.setDeadlineOnCancel(ctx, true)
	stopcACK, stopACK := client.setDeadlineOnCancel(ctx, false)
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

		batchesSent++
		if batchesSent%BATCH_RATIO_ACK == 0 {
			if err := client.recvAck(ctx, stopACK, stopcACK); err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action, true)
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

		if err := client.recvAck(ctx, stopACK, stopcACK); err != nil {
			return err
		}
	}

	if err := client.sendFinishMessage(ctx, stop, stopc); err != nil {
		return err
	}

	return checkContext(ctx, stop, stopc, client.conn, action, true)
}

func (client *Client) sendBatch(ctx context.Context, stop func() bool, stopc <-chan struct{}, action string, batch *betBatch) error {
	if err := checkDone(ctx, stop, action, batch); err != nil {
		return err
	}

	if err := safe_socket.SendMessage(client.conn, batch.bytes); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action, true)
		if ctxErr != nil {
			return ctxErr
		}
		logger.Error(action, logger.Fail)
		return err
	}

	batch.reset()
	return nil
}

func (client *Client) recvAck(ctx context.Context, stop func() bool, stopc <-chan struct{}) error {
	responseBuffer, err := safe_socket.RecvMessage(client.conn)

	if err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, "recv-ack", false)
		if ctxErr != nil {
			return ctxErr
		}
		logger.Error("recv-ack", logger.Fail)
		return err
	}

	if string(responseBuffer) != ACK_MESSAGE {
		logger.Error("recv-ack", logger.Fail, "expected", ACK_MESSAGE, "got", string(responseBuffer))
		return errors.New("invalid ack message received")
	}

	return nil
}

func (client *Client) sendFinishMessage(ctx context.Context, stop func() bool, stopc <-chan struct{}) error {
	const action = "send-bets"
	messageArgs := []any{"agency-id", client.config.AgencyId, FINISH_MESSAGE}
	message := append([]byte{0, 0, 0}, []byte(FINISH_MESSAGE)...)

	if err := safe_socket.SendMessage(client.conn, message); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action, true)
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

	stopc, stop := client.setDeadlineOnCancel(ctx, false)
	messageArgs := []any{"agency-id", client.config.AgencyId}

	for {
		select {
		case <-ctx.Done():
			logger.Info(action, logger.Fail, "context", "done")
			stop()
			return ctx.Err()
		default:
			responseBuffer, err := safe_socket.RecvMessage(client.conn)
			if err != nil {
				ctxErr := checkContext(ctx, stop, stopc, client.conn, action, false)
				if ctxErr != nil {
					return ctxErr
				}
				logger.Error(action, logger.Fail, messageArgs)
				return err
			}

			if string(responseBuffer) == FINISH_MESSAGE {
				logger.Info(action, logger.Success, messageArgs...)
				return checkContext(ctx, stop, stopc, client.conn, action, false)
			}

			outputFile.WriteString(string(responseBuffer) + "\n")
		}
	}
}

func (client *Client) setDeadlineOnCancel(ctx context.Context, write bool) (<-chan struct{}, func() bool) {
	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		if write {
			client.conn.SetWriteDeadline(time.Now())
		} else {
			client.conn.SetReadDeadline(time.Now())
		}
		close(stopc)
	})
	return stopc, stop
}

func checkContext(ctx context.Context, stop func() bool, stopc <-chan struct{}, socket net.Conn, action string, write bool) error {
	if !stop() {
		<-stopc
		if write {
			socket.SetWriteDeadline(time.Time{})
		} else {
			socket.SetReadDeadline(time.Time{})
		}
		logger.Info(action, logger.Fail, "context", "done")
		return ctx.Err()
	}

	return nil
}
