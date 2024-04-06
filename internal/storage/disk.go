package storage

import (
	"os"
	"strconv"
	"time"
)

type DataFile struct {
	Directory string
	Filename  string
	File      *os.File
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

func (d *Disk) Close() {
	d.ActiveDataFile.File.Close()
}
