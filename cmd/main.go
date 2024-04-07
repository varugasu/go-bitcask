package main

import (
	"log"

	"github.com/varugasu/go-bitcask/internal"
)

func main() {
	_, err := internal.NewDatabase("./data")
	if err != nil {
		log.Fatalln(err)
	}
}
