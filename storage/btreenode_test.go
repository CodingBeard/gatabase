package storage

import (
	"testing"
	"reflect"
	"time"
)

func TestNewBTreeNode(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Time{}
	keyString := ""
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	elements[0] = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	if node.ParentId != btreeNodeParentIdNoValue {
		t.Error("ParentId  not injected correctly")
	}
	if !reflect.DeepEqual(node.Elements, elements) {
		t.Error("Elements  not injected correctly")
	}
	if !reflect.DeepEqual(node.Path, make([]int32, 0)) {
		t.Error("Path  not injected correctly")
	}
}


func TestBTreeNode_Serialize(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Time{}
	keyString := ""
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	elements[0] = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	elements[1] = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	elements[2] = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	elements[3] = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	serialised := node.Serialize()

	buffer, err := NewMemoryImmutableFile(serialised, make([]byte, 0))

	if err != nil {
		panic(err)
	}

	unserialisedNode := DeserialiseBTreeNode(&buffer)

	if reflect.DeepEqual(unserialisedNode, node) {
		t.Error("deserialised node does not match original")
	}
}
