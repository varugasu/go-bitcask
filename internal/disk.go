package internal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"
)

type DataFile struct {
	Directory       string
	Filename        string
	File            *os.File
	CurrentPosition int
}

type Disk struct {
	ActiveDataFile *DataFile
}

func NewDisk(directory string) (*Disk, error) {
	filename := strconv.FormatInt(time.Now().Unix(), 10)

	file, err := os.Create(directory + "/" + filename)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		Directory: directory,
		Filename:  filename,
		File:      file,
	}

	return &Disk{ActiveDataFile: dataFile}, nil
}

func (d *Disk) Write(entry *Entry) error {
	serializedEntry := SerializeEntry(entry)

	n, err := d.ActiveDataFile.File.Write(serializedEntry)
	if err != nil {
		return err
	}

	d.ActiveDataFile.CurrentPosition += n
	return nil
}

func (d *Disk) Read(value KeyDirValue) ([]byte, error) {
	f, err := os.Open(value.FileId)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	fmt.Println("Discarding", value.Position)
	reader.Discard(int(value.Position))

	valueBytes := make([]byte, value.Size)
	_, err = reader.Read(valueBytes)
	if err != nil {
		return nil, err
	}

	return valueBytes, nil
}

func (d *Disk) InitKeyDir() (map[string]KeyDirValue, error) {
	keyDir := make(map[string]KeyDirValue)

	// list all files in the directory
	files, err := os.ReadDir(d.ActiveDataFile.Directory)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		filename := file.Name()
		if filename == d.ActiveDataFile.Filename {
			continue
		}

		path := d.ActiveDataFile.Directory + "/" + filename

		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		stat, err := f.Stat()
		if err != nil {
			return nil, err
		}

		reader := bufio.NewReader(f)
		streamSize := stat.Size()
		offset := int64(0)
		for offset < streamSize {
			reader.Discard(2)
			timestampBytes := make([]byte, 8)
			_, err := reader.Read(timestampBytes)
			if err != nil {
				return nil, err
			}

			timestamp := binary.BigEndian.Uint64(timestampBytes)

			keySizeBytes := make([]byte, 8)
			_, err = reader.Read(keySizeBytes)
			if err != nil {
				return nil, err
			}

			keySize := binary.BigEndian.Uint64(keySizeBytes)

			valueSizeBytes := make([]byte, 8)
			_, err = reader.Read(valueSizeBytes)
			if err != nil {
				return nil, err
			}

			valueSize := binary.BigEndian.Uint64(valueSizeBytes)

			keyBytes := make([]byte, keySize)
			_, err = reader.Read(keyBytes)
			if err != nil {
				return nil, err
			}

			valueBytes := make([]byte, valueSize)
			_, err = reader.Read(valueBytes)
			if err != nil {
				return nil, err
			}

			key := string(keyBytes)

			keyDir[key] = KeyDirValue{
				Timestamp: timestamp,
				FileId:    path,
				Size:      valueSize,
				Position: uint64(offset) +
					HEADER_SIZE +
					keySize,
			}

			offset += HEADER_SIZE + int64(keySize) + int64(valueSize)
		}
		f.Close()
	}

	return keyDir, nil
}

func (d *Disk) Close() {
	d.ActiveDataFile.File.Close()
}
