package main

import (
	"github.com/codingbeard/gatabase/storage"
	"log"
)

func main() {
	file, err := storage.GetFile("/go/src/github.com/codingbeard/gatabase/test.txt")

	if err != nil {
		panic(err)
	}

	log.Print(file.CountLines())
}
