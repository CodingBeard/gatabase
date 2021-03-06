package storage

import (
	"testing"
	"io"
	"strings"
)

func TestMemoryFileHandle_Read(t *testing.T) {
	content := "some content to read"
	file := NewMemoryFileHandle([]byte(content))

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

	file = NewMemoryFileHandle([]byte(content))

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

func TestMemoryFileHandle_Seek(t *testing.T) {
	file := NewMemoryFileHandle([]byte("some content to seek"))

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

func TestMemoryFileHandle_Write(t *testing.T) {
	file := NewMemoryFileHandle([]byte(""))

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
