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
	"math"
	"github.com/codingbeard/gatabase/gataerrors"
)

const (
	// Value used for ParentId when there is no parent
	btreeNodeParentIdNoValue = int32(-1)
	// When we create a new node and it has no location in the index yet
	btreeNodeNoLocationValue = int64(-1)
	// The padding applied to byte lengths/locations when serialising
	btreeNodeLengthLocationPadLength = 20
	// Deletion flags used prior to the length of the node in bytes
	btreeNodeNotDeleted = "0"
	btreeNodeMoved      = "1"
	btreeNodeDeleted    = "2"
)

var (
	SerialiseNodeError = gataerrors.NewGataError("unable to serialise node")
	DeserialiseNodeReadDeletedError          = gataerrors.NewGataError("unable to read deleted flag from ReadSeeker")
	DeserialiseNodeReadMovedLocationError    = gataerrors.NewGataError("unable to read moved to location from ReadSeeker")
	DeserialiseNodeInvalidMovedLocationError = gataerrors.NewGataError("unable to parse new location of node from ReadSeeker")
	DeserialiseNodeReadLengthError = gataerrors.NewGataError("unable to read length of the node from ReadSeeker")
	DeserialiseNodeReadNodeError = gataerrors.NewGataError("unable to read the node from ReadSeaker")
	DeserialiseNodeDeserialiseBytesError = gataerrors.NewGataError("unable to deserialise the binary node")
	ElementNotFoundByKeyError = gataerrors.NewGataError("element not found by key")
	NoNearestNodeFoundByKeyError = gataerrors.NewGataError("no nearest node found by key")
)

// A btree node containing elements
type BTreeNode struct {
	Deleted  bool
	Location int64
	ParentId int32
	Id       int32
	Path     []int32
	Elements []BTreeElement
}

// Construct a new BTreeNode
func NewBTreeNode(deleted bool, parentId int32, id int32, elements []BTreeElement, path []int32) (BTreeNode) {
	return BTreeNode{
		Deleted:  deleted,
		Location: int64(btreeNodeNoLocationValue),
		ParentId: parentId,
		Id:       id,
		Path:     path,
		Elements: elements,
	}
}

// Deserialise the node at the current pointer of the passed in ReadSeeker
func DeserialiseBTreeNode(serialisedNode io.ReadSeeker, location int64) (BTreeNode, error) {
	// Check to see if the node is deleted
	deleted := make([]byte, 1)
	_, err := serialisedNode.Read(deleted)

	if err != nil {
		return BTreeNode{}, DeserialiseNodeReadDeletedError.SetUnderlying(err)
	}

	if string(deleted) == btreeNodeMoved {
		// seek to the new location and read that node
		newNodeLocationString := make([]byte, 20)
		_, err = serialisedNode.Read(newNodeLocationString)

		if err != nil {
			return BTreeNode{}, DeserialiseNodeReadMovedLocationError.SetUnderlying(err)
		}

		newNodeLocation, err := strconv.ParseInt(string(newNodeLocationString), 10, 64)

		if err != nil {
			return BTreeNode{}, DeserialiseNodeInvalidMovedLocationError.SetUnderlying(err)
		}

		serialisedNode.Seek(newNodeLocation, io.SeekStart)

		return DeserialiseBTreeNode(serialisedNode, location)
	} else if string(deleted) == btreeNodeDeleted {
		return BTreeNode{Deleted: true}, nil
	}

	// Get the length of the serialised node
	lengthString := make([]byte, 20)
	_, err = serialisedNode.Read(lengthString)

	if err != nil {
		return BTreeNode{}, DeserialiseNodeReadLengthError.SetUnderlying(err)
	}

	length, err := strconv.ParseInt(string(lengthString), 10, 64)

	// Read the serialised node
	serialisedBytes := make([]byte, length)
	_, err = serialisedNode.Read(serialisedBytes)

	if err != nil {
		return BTreeNode{}, DeserialiseNodeReadNodeError.SetUnderlying(err)
	}

	// Decode the node
	node := BTreeNode{}
	buffer := bytes.Buffer{}
	buffer.Write(serialisedBytes)
	decoder := gob.NewDecoder(&buffer)
	err = decoder.Decode(&node)

	if err != nil {
		return BTreeNode{}, DeserialiseNodeDeserialiseBytesError.SetUnderlying(err)
	}

	node.Location = location

	return node, nil
}

// Get the key type from the first element
// You are unable to add different key types to the same node so we can use the
// First element without worrying
func (node *BTreeNode) GetKeyType() (int8) {
	if len(node.Elements) == 0 {
		return btreeElementTypeUnset
	}

	return node.Elements[0].KeyType
}

// Sort the elements by key depending on type
func (node *BTreeNode) Sort() {
	if len(node.Elements) == 0 {
		return
	}

	if node.GetKeyType() == btreeElementTypeInt {
		sort.Slice(node.Elements, func(i, j int) bool {
			return node.Elements[i].KeyInt < node.Elements[j].KeyInt
		})
	} else if node.GetKeyType() == btreeElementTypeString {
		sort.Slice(node.Elements, func(i, j int) bool {
			zero, _ := new(big.Int).SetString("0", 10)

			return node.Elements[i].GetDistanceFromStringKey(node.Elements[j].KeyString).Cmp(zero) == 1
		})
	} else if node.GetKeyType() == btreeElementTypeDate {
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
		return &BTreeElement{}, ElementNotFoundByKeyError
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

	return &BTreeElement{}, ElementNotFoundByKeyError
}

// Get the location (in bytes) of the next node to check if GetElementByKey
// Didn't have the key we were looking for
func (node *BTreeNode) GetNearestNodeLocationByKey(key interface{}) (int64, error) {
	if len(node.Elements) == 0 {
		return 0, NoNearestNodeFoundByKeyError
	}

	keyInt, isInt := key.(int64)
	keyString, isString := key.(string)
	keyDate, isDate := key.(time.Time)

	closestKeySet := false
	var closestElement BTreeElement

	if node.Elements[0].KeyType == btreeElementTypeInt && isInt {
		var closestKey int64
		for _, element := range node.Elements {
			distanceInt := int64(
				math.Abs(
					float64(
						element.GetDistanceFromIntKey(keyInt),
					),
				),
			)

			if !closestKeySet {
				closestKey = distanceInt
				closestKeySet = true
				closestElement = element
			}

			if distanceInt < closestKey {
				closestKey = distanceInt
				closestElement = element
			}
		}

		if closestElement.GetDistanceFromIntKey(keyInt) < 0 && closestElement.LessLocation != btreeElementNoChildValue {
			return closestElement.LessLocation, nil
		} else if closestElement.MoreLocation != btreeElementNoChildValue {
			return closestElement.MoreLocation, nil
		}
	} else if node.Elements[0].KeyType == btreeElementTypeString && isString {
		var closestKey *big.Int
		for _, element := range node.Elements {
			distanceBigInt := new(big.Int).Abs(
				element.GetDistanceFromStringKey(keyString),
			)

			if !closestKeySet {
				closestKey = distanceBigInt
				closestKeySet = true
				closestElement = element
			}

			if distanceBigInt.Cmp(closestKey) == -1 {
				closestKey = distanceBigInt
				closestElement = element
			}
		}

		zero, _ := new(big.Int).SetString("0", 10)

		if closestElement.GetDistanceFromStringKey(keyString).Cmp(zero) == -1 && closestElement.LessLocation != btreeElementNoChildValue {
			return closestElement.LessLocation, nil
		} else if closestElement.MoreLocation != btreeElementNoChildValue {
			return closestElement.MoreLocation, nil
		}
	} else if node.Elements[0].KeyType == btreeElementTypeDate && isDate {
		var closestKey int64
		for _, element := range node.Elements {
			distanceInt := int64(
				math.Abs(
					float64(
						element.GetDistanceFromDateKey(keyDate),
					),
				),
			)

			if !closestKeySet {
				closestKey = distanceInt
				closestKeySet = true
				closestElement = element
			}

			if distanceInt < closestKey {
				closestKey = distanceInt
				closestElement = element
			}
		}

		if closestElement.GetDistanceFromDateKey(keyDate) < 0 && closestElement.LessLocation != btreeElementNoChildValue {
			return closestElement.LessLocation, nil
		} else if closestElement.MoreLocation != btreeElementNoChildValue {
			return closestElement.MoreLocation, nil
		}
	}

	return 0, NoNearestNodeFoundByKeyError
}

// Serialise the node and return the byte slice representing it
// Along with a deletion flag and the length of the serialised node
func (node BTreeNode) Serialise() ([]byte, error) {
	// Serialise the node
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(node)

	if err != nil {
		return make([]byte, 0), SerialiseNodeError.SetUnderlying(err)
	}

	// Generate the length
	encodedBytes := buffer.Bytes()
	length := len(encodedBytes)
	lengthString := strconv.Itoa(length)
	lengthString = fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s", lengthString)

	// Construct the full response
	serialised := []byte(btreeNodeNotDeleted)
	serialised = append(serialised, []byte(lengthString)...)
	serialised = append(serialised, encodedBytes...)

	return serialised, nil
}
