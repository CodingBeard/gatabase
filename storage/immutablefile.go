package storage

import (
	"io"
	"errors"
	"os"
)

type ImmutableFile struct {
	DataHandle io.ReadWriteSeeker
	IndexHandle io.ReadWriteSeeker
}

func ConstructImmutableFile(path string) (ImmutableFile, error) {
	file := ImmutableFile{}

	if len(path) == 0 {
		return file, errors.New("empty path supplied")
	}

	var fileHandle *os.File
	var err error

	if _, err := os.Stat(path); os.IsNotExist(err) {
		fileHandle, err = os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0666)
	} else {
		fileHandle, err = os.Open(path)
	}

	if err != nil {
		panic(err)
	}

	file.DataHandle = fileHandle

	indexPath := path + ".index"

	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		fileHandle, err = os.OpenFile(indexPath, os.O_RDONLY|os.O_CREATE, 0666)
	} else {
		fileHandle, err = os.Open(indexPath)
	}

	if err != nil {
		panic(err)
	}

	file.DataHandle = fileHandle

	return file, nil
}

func (file *ImmutableFile) Read(p []byte) (n int, err error) {
	return file.DataHandle.Read(p)
}

func (file *ImmutableFile) Write(p []byte) (n int, err error) {
	return file.DataHandle.Write(p)
}

func (file *ImmutableFile) Seek(offset int64, whence int) (int64, error) {
	return file.DataHandle.Seek(offset, whence)
}
