package main

import (
	"log"

	"github.com/varugasu/go-bitcask/internal/storage"
)

func main() {
	_, err := storage.NewDisk("./data")
	if err != nil {
		log.Fatalln(err)
	}
}
