package main

import (
	"github.com/codingbeard/gatabase/storage"
)

func main() {
	_, err := storage.GetFile("/go/src/github.com/codingbeard/gatabase/test.txt")

	if err != nil {
		panic(err)
	}


}
