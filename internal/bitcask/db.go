package bitcask

import "github.com/varugasu/go-bitcask/internal/storage"

type Database struct {
	storage *storage.Disk
	keyDir  map[string][]KeyDirValue
}

type KeyDirValue struct {
	FileId    string
	Size      int
	Position  int
	Timestamp uint64
}

func NewDatabase(directory string) (*Database, error) {
	disk, err := storage.NewDisk(directory)
	if err != nil {
		return nil, err
	}

	return &Database{
		storage: disk,
		keyDir:  make(map[string][]KeyDirValue),
	}, nil
}
