package main

import (
	"github.com/codingbeard/gatabase/storage"
)

func main() {
	_, err := storage.NewImmutableFile("/go/src/github.com/codingbeard/gatabase/data/test.txt")

	if err != nil {
		panic(err)
	}


}
