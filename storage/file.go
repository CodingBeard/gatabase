package storage

import (
	"io"
	"bytes"
	"os"
	"errors"
)

type File struct {
	Handle io.ReadWriteSeeker
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

func (f File) CountLines() (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := f.Handle.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count + 1, nil

		case err != nil:
			return count, err
		}
	}
}
