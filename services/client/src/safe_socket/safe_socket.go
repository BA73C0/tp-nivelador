package safe_socket

import (
	"io"
)

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
