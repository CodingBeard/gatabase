package storage

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"fmt"
	"io"
	"sort"
	"math/big"
	"time"
	"errors"
)

const (
	// Value used for ParentId when there is no parent
	btreeNodeParentIdNoValue = int32(-1)
	// The padding applied to byte lengths when serialising
	btreeNodeLengthPadLength = 20
	// Deletion flags used prior to the length of the node in bytes
	btreeNodeNotDeleted = "0"
	btreeNodeMoved      = "1"
	btreeNodeDeleted    = "2"
)

// A btree node containing elements
type BTreeNode struct {
	Deleted  bool
	ParentId int32
	Id       int32
	Path     []int32
	Elements []BTreeElement
}

// Construct a new BTreeNode
func NewBTreeNode(deleted bool, parentId int32, id int32, elements []BTreeElement, path []int32) (BTreeNode) {
	return BTreeNode{
		Deleted:  deleted,
		ParentId: parentId,
		Id:       id,
		Path:     path,
		Elements: elements,
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

	if string(deleted) == btreeNodeMoved {
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
	} else if string(deleted) == btreeNodeDeleted {
		return BTreeNode{Deleted: true}
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

// Sort the elements by key depending on type
func (node *BTreeNode) Sort() {
	if len(node.Elements) == 0 {
		return
	}

	if node.Elements[0].IsIntType() {
		sort.Slice(node.Elements, func(i, j int) bool {
			return node.Elements[i].KeyInt < node.Elements[j].KeyInt
		})
	} else if node.Elements[0].IsStringType() {
		sort.Slice(node.Elements, func(i, j int) bool {
			zero, _ := new(big.Int).SetString("0", 10)

			return node.Elements[i].GetDistanceFromStringKey(node.Elements[j].KeyString).Cmp(zero) == 1
		})
	} else if node.Elements[0].IsDateType() {
		sort.Slice(node.Elements, func(i, j int) bool {
			return node.Elements[i].KeyDate.Unix() < node.Elements[j].KeyDate.Unix()
		})
	}
}

// Add a new element and then sort
func (node *BTreeNode) AddElement(element BTreeElement) {
	if len(node.Elements) != 0 {
		if node.Elements[0].KeyType != element.KeyType {
			panic("attempting to add an element of differing key type to node")
		}
	}

	node.Elements = append(node.Elements, element)
	node.Sort()
}

// Remove an element by key
func (node *BTreeNode) RemoveElement(key interface{}) {
	if len(node.Elements) == 0 {
		return
	}

	_, isInt := key.(int64)
	_, isString := key.(string)
	_, isDate := key.(time.Time)

	if node.Elements[0].KeyType == btreeElementTypeInt && isInt {
		for i, element := range node.Elements {
			if element.KeyInt == key {
				node.Elements[i] = node.Elements[len(node.Elements)-1]
				node.Elements = node.Elements[:len(node.Elements)-1]
			}
		}
	} else if node.Elements[0].KeyType == btreeElementTypeString && isString {
		for i, element := range node.Elements {
			if element.KeyString == key {
				node.Elements[i] = node.Elements[len(node.Elements)-1]
				node.Elements = node.Elements[:len(node.Elements)-1]
			}
		}
	} else if node.Elements[0].KeyType == btreeElementTypeDate && isDate {
		for i, element := range node.Elements {
			if element.KeyDate == key {
				node.Elements[i] = node.Elements[len(node.Elements)-1]
				node.Elements = node.Elements[:len(node.Elements)-1]
			}
		}
	}
}

// Get an element by its key
func (node *BTreeNode) GetElementByKey(key interface{}) (*BTreeElement, error) {
	if len(node.Elements) == 0 {
		return &BTreeElement{}, errors.New("no elements")
	}

	keyInt, isInt := key.(int64)
	keyString, isString := key.(string)
	keyDate, isDate := key.(time.Time)

	if node.Elements[0].KeyType == btreeElementTypeInt && isInt {
		for i := range node.Elements {
			if node.Elements[i].KeyInt == keyInt {
				return &node.Elements[i], nil
			}
		}
	} else if node.Elements[0].KeyType == btreeElementTypeString && isString {
		for i := range node.Elements {
			if node.Elements[i].KeyString == keyString {
				return &node.Elements[i], nil
			}
		}
	} else if node.Elements[0].KeyType == btreeElementTypeDate && isDate {
		for i := range node.Elements {
			if node.Elements[i].KeyDate.Unix() == keyDate.Unix() {
				return &node.Elements[i], nil
			}
		}
	}

	return &BTreeElement{}, errors.New("element not found by key")
}

// Serialise the node and return the byte slice representing it
// Along with a deletion flag and the length of the serialised node
func (node BTreeNode) Serialize() ([]byte) {
	// Serialise the node
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(node)

	if err != nil {
		panic(err)
	}

	// Generate the length
	encodedBytes := buffer.Bytes()
	length := len(encodedBytes)
	lengthString := strconv.Itoa(length)
	lengthString = fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthPadLength)+"s", lengthString)

	// Construct the full response
	serialised := []byte(btreeNodeNotDeleted)
	serialised = append(serialised, []byte(lengthString)...)
	serialised = append(serialised, encodedBytes...)

	return serialised
}
