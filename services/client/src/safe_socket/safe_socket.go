package safe_socket

import (
	"io"
)

const HEADER_SIZE = 3
const MAX_MESSAGE_SIZE = 65535

func SendMessage(socket io.Writer, bytes []byte) error {
	// Mientras queden bytes por enviar y
	// El tamaño del mensaje sea mayor a MAX_MESSAGE_SIZE
	// Envío un mensaje de tamaño MAX_MESSAGE_SIZE
	payloadSize := len(bytes) - HEADER_SIZE

	for payloadSize > MAX_MESSAGE_SIZE {
		msgSize := uint16(payloadSize)
		bytes[0] = byte(msgSize >> 8)
		bytes[1] = byte(msgSize)
		bytes[2] = byte(0) // Indica que hay más mensajes por enviar

		err := SendAll(socket, bytes[:MAX_MESSAGE_SIZE])
		if err != nil {
			return err
		}
		bytes = bytes[MAX_MESSAGE_SIZE-HEADER_SIZE:]
	}

	// Mando el resto de los bytes que no superan el MAX_MESSAGE_SIZE
	if len(bytes) > 0 {
		msgSize := uint16(len(bytes) - HEADER_SIZE)
		bytes[0] = byte(msgSize >> 8)
		bytes[1] = byte(msgSize)
		bytes[2] = byte(1) // Indica que es el último mensaje

		err := SendAll(socket, bytes)
		if err != nil {
			return err
		}
	}

	return nil
}

func SendAll(socket io.Writer, b []byte) error {
	bytesSent := 0

	for bytesSent < len(b) {
		n, err := socket.Write(b[bytesSent:])
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

	for bytesRead < n {
		n, err := socket.Read(buff[bytesRead:])
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
