package main

import (
	"fmt"
	"log"

	"github.com/varugasu/go-bitcask/internal"
)

func main() {
	db, err := internal.NewDatabase("./data")
	if err != nil {
		log.Fatalln(err)
	}

	db.Put("foo", []byte("bar"))

	value, err := db.Get("foo")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(value))
}
