package storage

import (
	"testing"
	"reflect"
	"io"
	"strconv"
)

func TestNewBTree(t *testing.T) {
	index := &MemoryFileHandle{}

	btree := NewBTree(index, 4, true)

	if !reflect.DeepEqual(index, btree.Index) {
		t.Error("Index injected incorrectly")
	}

	if btree.MaxElementsPerNode != 4 {
		t.Error("MaxElementsPerNode injected incorrectly. Expected 4, got:", btree.MaxElementsPerNode)
	}

	if btree.Unique != true {
		t.Error("Unique injected incorrectly. Expected true, got:", btree.Unique)
	}
}

func TestBtree_writeNode(t *testing.T) {
	// New btree with a memory index
	index := &MemoryFileHandle{data:[]byte("123")}
	btree := NewBTree(index, 4, true)

	// Create a node
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(123),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(123),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(123),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(123),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	// Serialise the node so we can compare to it later
	serialised, err := node.Serialise()

	if err != nil {
		t.Error(err)
	}

	// Write the node
	location, err := btree.writeNode(node)

	if err != nil {
		t.Error(err)
	}

	if location != 3 {
		t.Error("expected to write at end of file (3 bytes long), wrote at: ", location)
	}

	_, err = btree.Index.Seek(3, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	read := make([]byte, len(serialised))

	_, err = btree.Index.Read(read)

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(serialised, read) {
		t.Error("did not read serialised node from index")
	}
}

func TestBtree_writeRoot(t *testing.T) {
	// New btree with a memory index with initialised data
	index := &MemoryFileHandle{data:[]byte("123")}
	btree := NewBTree(index, 4, true)

	// Create the root
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(4),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	// Write the root
	location, err := btree.writeRoot(node)

	if err != nil {
		t.Error(err)
	}

	_, err = btree.Index.Seek(0, io.SeekStart)
	if err != nil {
		t.Error(err)
	}

	rootLocationString := make([]byte, 20)
	_, err = btree.Index.Read(rootLocationString)
	if err != nil {
		t.Error(err)
	}

	rootLocation, err := strconv.ParseInt(string(rootLocationString), 10, 64)

	if location != rootLocation {
		t.Error("mismatched root locations")
	}
}

func TestBTree_getRoot(t *testing.T) {
	// Create a btree with an memory index
	index := &MemoryFileHandle{}
	btree := NewBTree(index, 4, true)

	root, err := btree.getRoot()

	if err != bTreeNoRootError {
		t.Error("did not get expected error when getting a non-existent root")
	}

	if !reflect.DeepEqual(root, NewBTreeNode(
		false,
		0,
		0,
		make([]BTreeElement, 0),
		make([]int32, 0),
	)) {
		t.Error("did not get expected blank node when no root found")
	}

	btree.Index.Write([]byte("0000000000000010000"))

	root, err = btree.getRoot()

	if !BtreeRootUnableToDeserialise.IsSame(err) {
		t.Error("did not get expected error when unable to seek to root")
	}

	// Create the root
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(4),
		int64(0),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	_, err = btree.writeRoot(node)

	root, err = btree.getRoot()

	nodeBytes, err := node.Serialise()
	if err != nil {
		t.Error(err)
	}
	rootBytes, err := root.Serialise()
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(nodeBytes, rootBytes) {
		t.Error("gotten root does not match writen node")
	}
}

func TestBTree_Find(t *testing.T) {
	index := &MemoryFileHandle{}

	btree := NewBTree(index, 4, true)

	location, err := btree.Find(int64(1))

	if location != 0 {
		t.Error("unexpected location returned when finding non-existent key")
	}

	if err != BTreeKeyNotFoundError {
		t.Error("did not get expected error when unable to find key")
	}
}

func TestBTree_Insert(t *testing.T) {

}
