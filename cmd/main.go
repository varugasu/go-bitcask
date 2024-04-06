package main

import (
	"log"
	"time"

	"github.com/varugasu/go-bitcask/internal"
	"github.com/varugasu/go-bitcask/internal/storage"
)

func main() {
	disk, err := storage.NewDisk("./data")
	if err != nil {
		log.Fatalln(err)
	}

	err = disk.Write(&internal.Entry{
		Key:       []byte("foo"),
		Value:     []byte("bar"),
		Timestamp: uint64(time.Now().Unix()),
	})
	if err != nil {
		log.Fatalln(err)
	}
}
