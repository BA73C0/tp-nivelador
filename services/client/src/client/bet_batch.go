package client

import "github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"

type betBatch struct {
	bytes   []byte
	records int
}

func newBetBatch(batchSize int) *betBatch {
	bytes := make([]byte, 0, (ESTIMATED_BET_SIZE*batchSize)+safe_socket.HEADER_SIZE)
	bytes = append(bytes, []byte{0, 0, 0}...)
	return &betBatch{bytes: bytes}
}

func (batch *betBatch) append(agencyID string, record []byte) {
	if batch.records > 0 {
		batch.bytes = append(batch.bytes, ';')
	}
	batch.bytes = append(batch.bytes, agencyID...)
	batch.bytes = append(batch.bytes, ',')
	batch.bytes = append(batch.bytes, record...)
	batch.records++
}

func (batch *betBatch) isFull(batchSize int) bool {
	return batch.records == batchSize
}

func (batch *betBatch) hasRecords() bool {
	return batch.records > 0
}

func (batch *betBatch) reset() {
	batch.bytes = batch.bytes[:safe_socket.HEADER_SIZE]
	batch.records = 0
}

func (batch *betBatch) clear() {
	batch.bytes = batch.bytes[:0]
	batch.records = 0
}
