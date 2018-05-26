package storage

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"fmt"
	"io"
)

const (
	// Value used for ParentId when there is no parent
	btreeNodeParentIdNoValue = int32(-1)
	// The padding applied to byte lengths when serialising
	btreeNodeLengthPadLength = 20
	// Deletion flags used prior to the length of the node in bytes
	btreeNodeNotDeleted = "0"
	btreeNodeDeleted = "1"
)

// A btree node containing elements
type BTreeNode struct {
	ParentId int32
	Path     []int32
	Elements []BTreeElement
}

// Construct a new BTreeNode
func NewBTreeNode(parentId int32, elements []BTreeElement, path []int32) (BTreeNode) {
	return BTreeNode{
		ParentId:parentId,
		Path:path,
		Elements:elements,
	}
}

// Deserialise the node at the current pointer of the passed in ReadSeeker
func DeserialiseBTreeNode(serialisedNode io.ReadSeeker) (BTreeNode) {
	// Check to see if the node is deleted
	deleted := make([]byte, 1)
	_, err := serialisedNode.Read(deleted)

	if err != nil {
		panic(err)
	}

	if string(deleted) == btreeNodeDeleted {
		// seek to the new location and read that node
		newNodeLocationString := make([]byte, 20)
		_, err = serialisedNode.Read(newNodeLocationString)

		if err != nil {
			panic(err)
		}

		newNodeLocation, err := strconv.ParseInt(string(newNodeLocationString), 10, 64)

		if err != nil {
			panic(err)
		}

		serialisedNode.Seek(newNodeLocation, io.SeekStart)

		return DeserialiseBTreeNode(serialisedNode)
	}

	// Get the length of the serialised node
	lengthString := make([]byte, 20)
	_, err = serialisedNode.Read(lengthString)

	if err != nil {
		panic(err)
	}

	length, err := strconv.ParseInt(string(lengthString), 10, 64)

	// Read the serialised node
	serialisedBytes := make([]byte, length)
	_, err = serialisedNode.Read(serialisedBytes)

	if err != nil {
		panic(err)
	}

	// Decode the node
	element := BTreeNode{}
	buffer := bytes.Buffer{}
	buffer.Write(serialisedBytes)
	decoder := gob.NewDecoder(&buffer)
	err = decoder.Decode(&element)

	if err != nil {
		panic(err)
	}

	return element
}


// Serialise the node and return the byte slice representing it
// Along with a deletion flag and the length of the serialised node
func (element BTreeNode) Serialize() ([]byte) {
	// Serialise the node
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(element)

	if err != nil {
		panic(err)
	}

	// Generate the length
	encodedBytes := buffer.Bytes()
	length := len(encodedBytes)
	lengthString := strconv.Itoa(length)
	lengthString = fmt.Sprintf("%0" + strconv.Itoa(btreeNodeLengthPadLength) + "s", lengthString)

	// Construct the full response
	serialised := []byte(btreeNodeNotDeleted)
	serialised = append(serialised, []byte(lengthString)...)
	serialised = append(serialised, encodedBytes...)

	return serialised
}
