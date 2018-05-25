package storage

import (
	"testing"
	"errors"
	"fmt"
	"io"
	"strings"
)

type fakeFileHandle struct {
	data    []byte
	pointer int64
}

func (handle *fakeFileHandle) Read(p []byte) (n int, err error) {
	// Ask for nothing get nothing
	if len(p) == 0 {
		return 0, nil
	}

	// Are we at the end of the file already
	if int64(len(handle.data)) == handle.pointer {
		return 0, io.EOF
	}

	// Has the pointer moved ahead of the end of the file
	if int64(len(handle.data)) < handle.pointer {
		return 0, io.ErrUnexpectedEOF
	}

	// Using runes allows for multi-byte characters
	runes := []rune(string(handle.data))

	// Determine the end of the read based on how much data there is left to read
	var end int64

	if int(int64(len(handle.data))-handle.pointer) < len(p) {
		end = int64(len(handle.data)) - handle.pointer
	} else {
		end = handle.pointer + int64(len(p))
	}

	// Fill the byte slice with the data we want to read
	copy(p, []byte(string(runes[handle.pointer:end])))

	// Count the number of bytes we've read
	n = int(end - handle.pointer)

	// Update the pointer to the end of the read
	handle.pointer += int64(n)

	return n, nil
}

func (handle *fakeFileHandle) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (handle *fakeFileHandle) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// From beginning
		handle.pointer = offset
	case io.SeekCurrent:
		// From current pointer location
		handle.pointer += offset
	case io.SeekEnd:
		// Back from end
		handle.pointer = int64(len(handle.data)) - offset
	default:
		return 0, errors.New(fmt.Sprintf("invalid whence supplied %d", whence))
	}

	return handle.pointer, nil
}

func GetFakeFile(p string, content string) (File, error) {
	file := File{}

	file.Handle = &fakeFileHandle{data: []byte(content)}

	return file, nil
}

func TestRead(t *testing.T) {
	content := "some content to read"
	file, err := GetFakeFile("fake_file", content)

	if err != nil {
		t.Error(err)
	}

	// Test reading 10 bytes
	read := make([]byte, 10)

	bytesRead, err := file.Handle.Read(read)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 10 {
		t.Error("expected to read 10 bytes, read: ", bytesRead, " instead")
	}

	if string(read) != "some conte" {
		t.Error("error when reading 10 bytes, expected 'some conte' got: ", string(read))
	}

	// Test reading 10 bytes after the handle pointer has moved
	read = make([]byte, 10)

	bytesRead, err = file.Handle.Read(read)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 10 {
		t.Error("expected to read 10 bytes, read: ", bytesRead, " instead")
	}

	if string(read) != "nt to read" {
		t.Error("error when reading 10 bytes, expected 'nt to read' got: ", string(read))
	}

	file, err = GetFakeFile("fake_file", content)

	if err != nil {
		t.Error(err)
	}

	// Test reading too many bytes (all bytes + null)
	read = make([]byte, 50)

	bytesRead, err = file.Handle.Read(read)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 20 {
		t.Error("expected to read 20 bytes, read: ", bytesRead, " instead")
	}

	if strings.TrimRight(string(read), "\x00") != content {
		t.Error("error when reading 50 bytes, expected '", content, "' got:", string(read))
	}
}

func TestCountLines(t *testing.T) {
	file, err := GetFakeFile("fake_file", "Single line")

	if err != nil {
		t.Error(err)
	}

	// Test that there is 1 line in the file
	lines, err := file.CountLines()

	if err != nil {
		t.Error(err)
	}

	if lines != 1 {
		t.Error("number of lines in the test file did not match expected 1, got:", lines)
	}

	file, err = GetFakeFile("fake_file", "Some\n"+
		"lines with\n"+
		"words on\n"+
		"them")

	if err != nil {
		t.Error(err)
	}

	// Test that there are 4 lines in the file
	lines, err = file.CountLines()

	if err != nil {
		t.Error(err)
	}

	if lines != 4 {
		t.Error("number of lines in the test file did not match expected 4, got:", lines)
	}
}
