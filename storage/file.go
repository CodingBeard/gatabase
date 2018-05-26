package storage

import (
	"io"
	"os"
	"errors"
)

type File struct {
	Handle io.ReadWriteSeeker
}

func (f *File) Read(p []byte) (n int, err error) {
	return f.Handle.Read(p)
}

func (f *File) Write(p []byte) (n int, err error) {
	return f.Handle.Write(p)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.Handle.Seek(offset, whence)
}

func GetFile(p string) (File, error) {
	file := File{}

	if len(p) == 0 {
		return file, errors.New("empty path supplied")
	}

	fileHandle, err := os.Open(p)

	if err != nil {
		panic(err)
	}

	file.Handle = fileHandle

	return file, nil
}
