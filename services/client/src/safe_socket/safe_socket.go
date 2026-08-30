package safe_socket

import (
	"errors"
	"io"
)

const HEADER_SIZE = 3
const MAX_MESSAGE_SIZE = 65535 - HEADER_SIZE
const MAX_RETRY = 3

func SendMessage(socket io.Writer, bytes []byte) error {
	// Mientras queden bytes por enviar y
	// El tamaño del mensaje sea mayor a MAX_MESSAGE_SIZE
	// Envío un mensaje de tamaño MAX_MESSAGE_SIZE
	for len(bytes) > MAX_MESSAGE_SIZE {
		err := send(socket, bytes[:MAX_MESSAGE_SIZE], 0)
		if err != nil {
			return err
		}
		bytes = bytes[MAX_MESSAGE_SIZE:]
	}

	// Mando el resto de los bytes que no superan el MAX_MESSAGE_SIZE
	if len(bytes) > 0 {
		err := send(socket, bytes, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

func send(socket io.Writer, bytes []byte, end uint8) error {
	msgSize := uint16(len(bytes))
	header := []byte{byte(msgSize >> 8), byte(msgSize), byte(end)}

	if err := SendAll(socket, header); err != nil {
		return err
	}

	return SendAll(socket, bytes)
}

func SendAll(socket io.Writer, b []byte) error {
	bytesSent := 0
	retryCount := 0

	for bytesSent < len(b) {
		if retryCount >= MAX_RETRY {
			return errors.New("max retry count reached while sending data")
		}

		n, err := socket.Write(b[bytesSent:])

		if n == 0 && err == nil {
			retryCount++
		} else {
			retryCount = 0
		}

		if err != nil {
			return err
		}

		bytesSent += n
	}

	return nil
}

func RecvMessage(socket io.Reader) ([]byte, error) {
	msg, _, err := recv(socket)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func RecvAll(socket io.Reader, n int) ([]byte, error) {
	buff := make([]byte, n)
	bytesRead := 0
	retryCount := 0

	for bytesRead < n {

		if retryCount >= MAX_RETRY {
			return buff, errors.New("max retry count reached while sending data")
		}

		n, err := socket.Read(buff[bytesRead:])

		if n == 0 && err == nil {
			retryCount++
		} else {
			retryCount = 0
		}

		if err != nil {
			return buff, err
		}
		bytesRead += n
	}

	return buff, nil
}

func recv(socket io.Reader) ([]byte, uint8, error) {
	header, err := RecvAll(socket, HEADER_SIZE)
	if err != nil {
		return nil, 1, err
	}
	msgLen := (uint16(header[0]) << 8) | uint16(header[1])
	var end uint8 = header[2]

	msg, err := RecvAll(socket, int(msgLen))
	if err != nil && err != io.EOF {
		return nil, end, err
	}

	for end == 0 {
		nextMsg, nextEnd, err := recv(socket)
		if err != nil {
			return nil, end, err
		}
		msg = append(msg, nextMsg...)
		end = nextEnd
	}

	return msg, end, nil
}
