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
const ESTIMATED_BET_SIZE = 55

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
	action := "send-bets"

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail)
		return err
	}
	defer inputFile.Close()

	// Registro de un AfterFunc que se ejecutará cuando el contexto se cancele o expire.
	// Esta función establece un deadline de escritura en el socket y cierra el canal stopc.
	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		client.conn.SetWriteDeadline(time.Now())
		close(stopc)
	})

	scanner := bufio.NewScanner(inputFile)

	batch := make([]byte, 0, ESTIMATED_BET_SIZE*client.config.BatchSize)
	recordsInBatch := 0

	for scanner.Scan() {
		if recordsInBatch > 0 {
			batch = append(batch, ';')
		}

		batch = append(batch, client.config.AgencyId...)
		batch = append(batch, ',')
		batch = append(batch, scanner.Bytes()...)

		recordsInBatch++

		if recordsInBatch == client.config.BatchSize {
			select {
			case <-ctx.Done():
				logger.Info(action, logger.Fail, "context", "done")
				batch = batch[:0]
				stop()
				return ctx.Err()
			default:
				if err := safe_socket.SendMessage(client.conn, batch); err != nil {
					ctxErr := checkContext(ctx, stop, stopc, client.conn, action, true)
					if ctxErr != nil {
						return ctxErr
					}
					logger.Error(action, logger.Fail)
					return err
				}

				batch = batch[:0]
				recordsInBatch = 0
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

	if recordsInBatch > 0 {
		select {
		case <-ctx.Done():
			logger.Info(action, logger.Fail, "context", "done")
			batch = batch[:0]
			stop()
			return ctx.Err()
		default:
			if err := safe_socket.SendMessage(client.conn, batch); err != nil {
				ctxErr := checkContext(ctx, stop, stopc, client.conn, action, true)
				if ctxErr != nil {
					return ctxErr
				}
				logger.Error(action, logger.Fail)
				return err
			}
			batch = batch[:0]
		}
	}

	messageArgs := []any{"agency-id", client.config.AgencyId, FINISH_MESSAGE}
	if err := safe_socket.SendMessage(client.conn, []byte(FINISH_MESSAGE)); err != nil {
		ctxErr := checkContext(ctx, stop, stopc, client.conn, action, true)
		if ctxErr != nil {
			return ctxErr
		}
		logger.Error("send-end-message", logger.Fail, messageArgs)
		return err
	}

	return checkContext(ctx, stop, stopc, client.conn, action, true)
}

func (client *Client) recvWinners(ctx context.Context) error {
	action := "recv-winners"

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail)
		return err
	}
	defer outputFile.Close()

	// Registro de un AfterFunc que se ejecutará cuando el contexto se cancele o expire.
	// Esta función establece un deadline de lectura en el socket y cierra el canal stopc.
	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		client.conn.SetReadDeadline(time.Now())
		close(stopc)
	})

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

func checkContext(ctx context.Context, stop func() bool, stopc <-chan struct{}, socket net.Conn, action string, write bool) error {
	// Si el contexto se canceló (antes o durante la lectura/escritura),
	// espero a que termine de establecer el deadline, lo restauro y devuelvo el error de cancelación del contexto.
	if !stop() {
		// Comentario de la docu oficial:
		// The AfterFunc was started.
		// Wait for it to complete, and reset the Conn's deadline.
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
