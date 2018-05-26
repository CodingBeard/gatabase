package storage

import (
	"io"
	"errors"
	"fmt"
)

type MemoryFileHandle struct {
	data    []byte
	pointer int64
}

func ConstructMemoryImmutableFile(content string) (ImmutableFile, error) {
	file := ImmutableFile{}

	file.DataHandle = &MemoryFileHandle{data: []byte(content)}
	file.IndexHandle = &MemoryFileHandle{data: []byte(content)}

	return file, nil
}

func (handle *MemoryFileHandle) Read(p []byte) (n int, err error) {
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

func (handle *MemoryFileHandle) Write(p []byte) (n int, err error) {

	// If the capacity of the internal data structure needs to grow to
	// accommodate the new data, increase the capacity
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

func (handle *MemoryFileHandle) Seek(offset int64, whence int) (int64, error) {
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
