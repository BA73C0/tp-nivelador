package safe_socket

import (
	"errors"
	"io"
)

const MAX_MESSAGE_SIZE = 65535

func SendAll(socket io.Writer, bytes []byte) error {
	if len(bytes) > MAX_MESSAGE_SIZE {
		return errors.New("message too long")
	}

	var size uint16 = uint16(len(bytes) + 1)

	msg := []byte{byte(size >> 8), byte(size)}
	msgBytes := make([]byte, size)
	copy(msgBytes, bytes)
	msgBytes[size-1] = '\x00'
	msg = append(msg, msgBytes...)

	err := send(socket, msg, size+uint16(2))
	if err != nil {
		return err
	}

	return nil
}

func send(socket io.Writer, bytes []byte, size uint16) error {
	n, err := socket.Write(bytes)
	if err != nil {
		return err
	}
	if n != int(size) {
		return io.ErrShortWrite
	}
	return nil
}

func RecvAll(socket io.Reader, _ int) ([]byte, error) {
	size, err := recv(socket, 2)
	if err != nil {
		return nil, err
	}

	msgLen := (uint16(size[0]) << 8) | uint16(size[1])

	msg, err := recv(socket, int(msgLen))
	if err != nil {
		return nil, err
	}
	if len(msg) == 0 || msg[len(msg)-1] != '\x00' {
		return nil, io.ErrUnexpectedEOF
	}

	return msg[:len(msg)-1], nil
}

func recv(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	n, err := socket.Read(buff)
	if n < size {
		return nil, errors.New("short read")
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buff, nil
}
