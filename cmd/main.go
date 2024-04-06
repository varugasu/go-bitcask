package main

import (
	"log"
	"time"

	"github.com/varugasu/go-bitcask/internal"
)

func main() {
	disk, err := internal.NewDisk("./data")
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
