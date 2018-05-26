package storage

import (
	"testing"
	"errors"
	"fmt"
	"io"
	"strings"
)

type FakeFileHandle struct {
	data    []byte
	pointer int64
}

func (handle *FakeFileHandle) Read(p []byte) (n int, err error) {
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

func (handle *FakeFileHandle) Write(p []byte) (n int, err error) {

	// If the capacity of the internal data structure needs to grow to accommodate
	// The new data, increase the capacity
	if handle.pointer + int64(len(p)) > int64(cap(handle.data)) {
		newData := make([]byte, handle.pointer + int64(len(p)))
		copy(newData, handle.data)
		handle.data = newData
	}

	// Write the bytes to the handle's data structure
	for n = 0; n < len(p); n++ {
		handle.data[handle.pointer] = p[n]
		handle.pointer += 1
	}

	return n, nil
}

func (handle *FakeFileHandle) Seek(offset int64, whence int) (int64, error) {
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

func GetFakeFile(content string) (File, error) {
	file := File{}

	file.Handle = &FakeFileHandle{data: []byte(content)}

	return file, nil
}

func TestRead(t *testing.T) {
	content := "some content to read"
	file, err := GetFakeFile(content)

	if err != nil {
		t.Error(err)
	}

	// Test reading 10 bytes
	read := make([]byte, 10)

	bytesRead, err := file.Read(read)

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

	bytesRead, err = file.Read(read)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 10 {
		t.Error("expected to read 10 bytes, read: ", bytesRead, " instead")
	}

	if string(read) != "nt to read" {
		t.Error("error when reading 10 bytes, expected 'nt to read' got: ", string(read))
	}

	file, err = GetFakeFile(content)

	if err != nil {
		t.Error(err)
	}

	// Test reading too many bytes (all bytes + null)
	read = make([]byte, 50)

	bytesRead, err = file.Read(read)

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

func TestSeek(t *testing.T) {
	file, err := GetFakeFile("some content to seek")

	if err != nil {
		t.Error(err)
	}

	// Test seeking to 10 bytes from the start
	pointerPosition, err := file.Seek(10, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	if pointerPosition != 10 {
		t.Error("Did not seek to pointer position 10, got: ", pointerPosition)
	}

	// Test seeking to 50 bytes from the start
	pointerPosition, err = file.Seek(50, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	if pointerPosition != 50 {
		t.Error("Did not seek to pointer position 50, got: ", pointerPosition)
	}

	// Test seeking to 100 bytes from the current position
	pointerPosition, err = file.Seek(50, io.SeekCurrent)

	if err != nil {
		t.Error(err)
	}

	if pointerPosition != 100 {
		t.Error("Did not seek to pointer position 100, got: ", pointerPosition)
	}

	// Test seeking to 10 bytes from the current position
	pointerPosition, err = file.Seek(10, io.SeekEnd)

	if err != nil {
		t.Error(err)
	}

	if pointerPosition != 10 {
		t.Error("Did not seek to pointer position 10, got: ", pointerPosition)
	}

	// Test reading 10 bytes after the handle pointer has moved
	read := make([]byte, 10)

	bytesRead, err := file.Read(read)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 10 {
		t.Error("expected to read 10 bytes, read: ", bytesRead, " instead")
	}

	if string(read) != "nt to seek" {
		t.Error("error when reading 10 bytes, expected 'nt to seek' got: ", string(read))
	}
}

func TestWrite(t *testing.T) {
	file, err := GetFakeFile("")

	if err != nil {
		t.Error(err)
	}

	// Test writing content
	content := "Some written content"
	bytesWritten, err := file.Write([]byte(content))

	if err != nil {
		t.Error(err)
	}

	if bytesWritten != 20 {
		t.Error("did not write expected 20 bytes, wrote:", bytesWritten)
	}

	// Reset the pointer so we can read the written bytes
	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	// Test reading the written content
	readBytes := make([]byte, 20)

	bytesRead, err := file.Read(readBytes)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 20 {
		t.Error("did not read expected 20 bytes, read:", bytesRead)
	}

	if string(readBytes) != content {
		t.Error("read bytes did not match written bytes, expected '", content, "' got: ", string(readBytes))
	}

	// Set the pointer so we can write half way through
	_, err = file.Seek(10, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	// Test writing in the middle of the file
	bytesWritten, err = file.Write([]byte("en words  "))

	if err != nil {
		t.Error(err)
	}

	if bytesWritten != 10 {
		t.Error("did not write expected 10 bytes, wrote:", bytesWritten)
	}

	// Reset the pointer so we can read the written bytes
	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	// Test reading the written content
	readBytes = make([]byte, 20)

	bytesRead, err = file.Read(readBytes)

	if err != nil {
		t.Error(err)
	}

	if bytesRead != 20 {
		t.Error("did not read expected 20 bytes, read:", bytesRead)
	}

	if string(readBytes) != "Some written words  " {
		t.Error("read bytes did not match written bytes, expected 'Some written words  ' got: ", string(readBytes))
	}
}
