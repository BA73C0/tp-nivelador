package safe_socket

import (
	"errors"
	"io"
)

const HEADER_SIZE = 3
const MAX_MESSAGE_SIZE = 65535 - HEADER_SIZE
const MAX_RETRY = 3

func SendAll(socket io.Writer, bytes []byte) error {

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
	var size uint16 = uint16(len(bytes) + HEADER_SIZE)
	var msgSize uint16 = uint16(len(bytes))

	msg := make([]byte, 0, size)
	msg = append(msg, byte(msgSize>>8), byte(msgSize), byte(end))
	msg = append(msg, bytes...)

	var bytesSent uint16 = 0
	retryCount := 0

	for bytesSent < size {

		if retryCount >= MAX_RETRY {
			return errors.New("max retry count reached while sending data")
		}

		n, err := socket.Write(msg[bytesSent:])

		if n == 0 && err == nil {
			retryCount++
		} else {
			retryCount = 0
		}

		if err != nil {
			return err
		}
		bytesSent += uint16(n)
	}

	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	msg, _, err := recvAll(socket, size)
	if err != nil {
		return nil, err
	}

	if len(msg) > size {
		return msg[:size], nil
	}
	return msg, nil
}

func recvAll(socket io.Reader, size int) ([]byte, uint8, error) {
	header, err := recv(socket, HEADER_SIZE)
	if err != nil {
		return nil, 1, err
	}
	msgLen := (uint16(header[0]) << 8) | uint16(header[1])
	var end uint8 = header[2]

	msg, err := recv(socket, int(msgLen))
	if err != nil && err != io.EOF {
		return nil, end, err
	}

	for end == 0 {
		newSize := size - int(msgLen)
		if newSize <= 0 {
			return nil, end, nil
		}
		nextMsg, nextEnd, err := recvAll(socket, newSize)
		if err != nil {
			return nil, end, err
		}
		msg = append(msg, nextMsg...)
		end = nextEnd
	}

	return msg, end, nil
}

func recv(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	bytesRead := 0
	retryCount := 0

	for bytesRead < size {

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
