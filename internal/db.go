package internal

type Database struct {
	storage *Disk
	keyDir  map[string]KeyDirValue
}

type KeyDirValue struct {
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
