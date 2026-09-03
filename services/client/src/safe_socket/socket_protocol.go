package safe_socket

import (
	"errors"
	"fmt"
	"io"
	"net"
)

const protocolHeaderSize = 5
const protocolBytesHeaderSize = 2
const protocolMaxMessageSize = 1<<(8*protocolBytesHeaderSize) - protocolHeaderSize
const protocolMaxRetries = 3
const protocolMaxMsgInFlight = 1

type SocketPacket struct {
	Data  []byte
	End   byte
	MsgID byte
	IsACK bool
}

type socketReadWriteCloser interface {
	io.Reader
	io.Writer
	io.Closer
}

type SocketProtocol struct {
	socket         socketReadWriteCloser
	inFlight       map[byte][]byte
	lastSentID     byte
	lastReceivedID byte
	hasReceivedID  bool
	retries        int
	maxRetries     int
	maxInFlight    int
	shuttingDown   bool
}

func NewSocketProtocol(socket socketReadWriteCloser) *SocketProtocol {
	return &SocketProtocol{
		socket:      socket,
		inFlight:    make(map[byte][]byte),
		maxRetries:  protocolMaxRetries,
		maxInFlight: protocolMaxMsgInFlight,
	}
}

func (protocol *SocketProtocol) Close() error {
	protocol.shuttingDown = true
	return protocol.socket.Close()
}

func (protocol *SocketProtocol) nextMsgID() byte {
	protocol.lastSentID++
	return protocol.lastSentID
}

func (protocol *SocketProtocol) SendMessage(data []byte) error {
	return protocol.sendMessage(data, protocol.nextMsgID(), true)
}

func (protocol *SocketProtocol) sendMessage(data []byte, msgID byte, waitForACK bool) error {
	bytesSent := 0

	for len(data)-bytesSent > protocolMaxMessageSize {
		if err := protocol.send(data[bytesSent:bytesSent+protocolMaxMessageSize], 0, msgID, false); err != nil {
			return err
		}
		bytesSent += protocolMaxMessageSize
	}

	if bytesSent < len(data) {
		if err := protocol.send(data[bytesSent:], 1, msgID, false); err != nil {
			return err
		}
	}

	protocol.inFlight[msgID] = data

	if waitForACK {
		return protocol.WaitForACK()
	}

	return nil
}

func (protocol *SocketProtocol) WaitForACK() error {
	for len(protocol.inFlight) >= protocol.maxInFlight {
		packet, err := protocol.recv()
		if err != nil {
			if protocol.shuttingDown {
				return errors.New("socket is shutting down, cannot wait for ACK")
			}

			if !isTimeout(err) {
				return err
			}

			protocol.retries++
			if protocol.retries > protocol.maxRetries {
				return errors.New("maximum retries exceeded while waiting for ACK")
			}

			for msgID, data := range protocol.inFlight {
				if err := protocol.sendMessage(data, msgID, false); err != nil {
					return err
				}
			}
			continue
		}

		if !packet.IsACK {
			return fmt.Errorf("received non-ACK message when expecting ACK: %d", packet.MsgID)
		}

		if _, ok := protocol.inFlight[packet.MsgID]; !ok {
			return fmt.Errorf("received ACK for unknown message ID: %d", packet.MsgID)
		}

		delete(protocol.inFlight, packet.MsgID)
	}

	return nil
}

func (protocol *SocketProtocol) send(data []byte, end byte, msgID byte, isACK bool) error {
	isACKByte := byte(0)
	if isACK {
		isACKByte = 1
	}

	msgSize := len(data)
	header := []byte{byte(msgSize >> 8), byte(msgSize), end, msgID, isACKByte}

	if err := SendAll(protocol.socket, header); err != nil {
		return err
	}
	return SendAll(protocol.socket, data)
}

func (protocol *SocketProtocol) RecvMessage() ([]byte, error) {
	duplicateRetries := 0

	for {
		packet, err := protocol.recv()
		if err != nil {
			return nil, err
		}

		if packet.IsACK {
			continue
		}

		isDuplicate := protocol.hasReceivedID && packet.MsgID == protocol.lastReceivedID
		if isDuplicate {
			duplicateRetries++
			if duplicateRetries > protocol.maxRetries {
				return nil, errors.New("too many duplicate messages received, giving up")
			}
		} else {
			protocol.lastReceivedID = packet.MsgID
			protocol.hasReceivedID = true
		}

		if err := protocol.send(nil, 1, packet.MsgID, true); err != nil {
			return nil, err
		}

		if !isDuplicate {
			return packet.Data, nil
		}
	}
}

func (protocol *SocketProtocol) recv() (SocketPacket, error) {
	header, err := RecvAll(protocol.socket, protocolHeaderSize)
	if err != nil {
		return SocketPacket{}, err
	}

	msgLen := (uint16(header[0]) << 8) | uint16(header[1])
	end := header[2]
	msgID := header[3]
	isACK := header[4] != 0

	msg, err := RecvAll(protocol.socket, int(msgLen))
	if err != nil {
		return SocketPacket{}, err
	}

	protocol.retries = 0

	for end == 0 {
		nextPacket, err := protocol.recv()
		if err != nil {
			return SocketPacket{}, err
		}

		if nextPacket.MsgID != msgID {
			return SocketPacket{}, fmt.Errorf("message ID mismatch: expected %d, got %d", msgID, nextPacket.MsgID)
		}

		msg = append(msg, nextPacket.Data...)
		end = nextPacket.End
	}

	return SocketPacket{
		Data:  msg,
		End:   end,
		MsgID: msgID,
		IsACK: isACK,
	}, nil
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
