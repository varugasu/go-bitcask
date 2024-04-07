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

	value, err := db.Get("foo")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(value))
}
