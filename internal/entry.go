package internal

import (
	"encoding/binary"

	"github.com/howeyc/crc16"
)

const HEADER_SIZE = 26

type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
}

func SerializeEntry(e *Entry) []byte {
	keySize := len(e.Key)
	valueSize := len(e.Value)

	entrySize := HEADER_SIZE + keySize + valueSize

	buf := make([]byte, entrySize)

	binary.BigEndian.PutUint64(buf[2:10], e.Timestamp)
	binary.BigEndian.PutUint64(buf[10:18], uint64(keySize))
	binary.BigEndian.PutUint64(buf[18:26], uint64(valueSize))

	copy(buf[26:26+keySize], e.Key)
	copy(buf[26+keySize:entrySize], e.Value)

	crc := crc16.ChecksumIBM(buf[2:entrySize])
	binary.BigEndian.PutUint16(buf[:2], crc)

	return buf
}
