package internal

import (
	"errors"
	"time"
)

type Database struct {
	storage *Disk
	keyDir  map[string]ValuePosition
}

type ValuePosition struct {
	FileId    string
	Size      uint64
	Position  uint64
	Timestamp uint64
}

func NewDatabase(directory string) (*Database, error) {
	disk, err := NewDisk(directory)
	if err != nil {
		return nil, err
	}

	keyDir, err := disk.InitKeyDir()
	if err != nil {
		return nil, err
	}

	return &Database{
		storage: disk,
		keyDir:  keyDir,
	}, nil
}

func (db *Database) Get(key string) ([]byte, error) {
	value, ok := db.keyDir[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	return db.storage.Read(value)
}

func (db *Database) Put(key string, value []byte) error {
	entry := &Entry{
		Key:       []byte(key),
		Value:     value,
		Timestamp: uint64(time.Now().Unix()),
	}

	valuePosition, err := db.storage.Write(entry)
	if err != nil {
		return err
	}

	db.keyDir[key] = valuePosition

	return nil
}
