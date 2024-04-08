package internal

import (
	"errors"
	"time"
)

const TOMBSTONE = "BITCASK_TOMBSTONE"

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
	valuePosition, ok := db.keyDir[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	value, err := db.storage.Read(valuePosition)
	if err != nil {
		return nil, err
	}

	if string(value) == TOMBSTONE {
		return nil, errors.New("deleted")
	}

	return value, nil
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

func (db *Database) Delete(key string) error {
	if _, ok := db.keyDir[key]; !ok {
		return errors.New("key not found")
	}

	valuePosition, err := db.storage.Write(&Entry{
		Key:       []byte(key),
		Value:     []byte(TOMBSTONE),
		Timestamp: uint64(time.Now().Unix()),
	})
	if err != nil {
		return err
	}

	db.keyDir[key] = valuePosition

	return nil
}
