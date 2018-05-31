package storage

import (
	"testing"
	"reflect"
	"io"
	"strconv"
)

func nodesEqual(a, b BTreeNode) (bool, error) {
	aSerialised, err := a.Serialise()
	if err != nil {
		return false, err
	}
	bSerialised, err := b.Serialise()
	if err != nil {
		return false, err
	}

	return reflect.DeepEqual(aSerialised, bSerialised), nil
}

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

	if location != btreeNodeLengthLocationPadLength {
		t.Error("expected to write after the root location, wrote at: ", location)
	}

	_, err = btree.Index.Seek(btreeNodeLengthLocationPadLength, io.SeekStart)

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

func TestBtree_readNode(t *testing.T) {
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

	if location != btreeNodeLengthLocationPadLength {
		t.Error("expected to write after the root location, wrote at: ", location)
	}

	read, err := btree.readNode(btreeNodeLengthLocationPadLength)

	if err != nil {
		t.Error(err)
	}

	seralisedRead, err := read.Serialise()

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(serialised, seralisedRead) {
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

func TestBtree_findNodeByKey(t *testing.T) {
	// Create a btree with an memory index
	index := &MemoryFileHandle{}
	btree := NewBTree(index, 4, true)

	// Create node one
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(1),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	nodeOne := NewBTreeNode(false, parentId, 1, elements, path)

	nodeOneLocation, err := btree.writeNode(nodeOne)
	if err != nil {
		t.Error(err)
	}

	// Create node two
	parentId = btreeNodeParentIdNoValue
	path = make([]int32, 0)
	elements = make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(3),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	nodeTwo := NewBTreeNode(false, parentId, 1, elements, path)

	nodeTwoLocation, err := btree.writeNode(nodeTwo)
	if err != nil {
		t.Error(err)
	}

	// Create the root
	parentId = btreeNodeParentIdNoValue
	path = make([]int32, 0)
	elements = make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(0),
		nodeOneLocation,
		nodeTwoLocation,
	)

	root := NewBTreeNode(false, parentId, 1, elements, path)

	_, err = btree.writeRoot(root)
	if err != nil {
		t.Error(err)
	}

	findRoot, err := btree.findNodeByKey(0, int64(2))

	nodesSame, err := nodesEqual(findRoot, root)
	if err != nil {
		t.Error(err)
	}

	if !nodesSame {
		t.Error("found root node not equal to created root node")
	}

	findOne, err := btree.findNodeByKey(0, int64(1))

	nodesSame, err = nodesEqual(findOne, nodeOne)
	if err != nil {
		t.Error(err)
	}

	if !nodesSame {
		t.Error("found node one not equal to created node one")
	}

	findTwo, err := btree.findNodeByKey(0, int64(3))

	nodesSame, err = nodesEqual(findTwo, nodeTwo)
	if err != nil {
		t.Error(err)
	}

	if !nodesSame {
		t.Error("found node two not equal to created node two")
	}
}

func TestBTree_Insert(t *testing.T) {
	// initialise an empty btree with a memory index
	index := &MemoryFileHandle{}
	btree := NewBTree(index, 4, true)

	// insert a key of 1 with a location of 10
	err := btree.Insert(int64(1), int64(10))

	if err != nil {
		t.Error(err)
	}
}

func TestBTree_Find(t *testing.T) {
	index := &MemoryFileHandle{}

	btree := NewBTree(index, 4, true)

	location, err := btree.Find(int64(1))

	if location != 0 {
		t.Error("unexpected location returned when finding non-existent key")
	}

	if !BTreeKeyNotFoundError.IsSame(err) {
		t.Error("did not get expected error when unable to find key")
	}

	// Create node one
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(10),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	nodeOne := NewBTreeNode(false, parentId, 1, elements, path)

	nodeOneLocation, err := btree.writeNode(nodeOne)
	if err != nil {
		t.Error(err)
	}

	// Create node two
	parentId = btreeNodeParentIdNoValue
	path = make([]int32, 0)
	elements = make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(30),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	nodeTwo := NewBTreeNode(false, parentId, 1, elements, path)

	nodeTwoLocation, err := btree.writeNode(nodeTwo)
	if err != nil {
		t.Error(err)
	}

	// Create the root
	parentId = btreeNodeParentIdNoValue
	path = make([]int32, 0)
	elements = make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(20),
		nodeOneLocation,
		nodeTwoLocation,
	)

	root := NewBTreeNode(false, parentId, 1, elements, path)

	_, err = btree.writeRoot(root)
	if err != nil {
		t.Error(err)
	}

	twoLocation, err := btree.Find(int64(2))
	if err != nil {
		t.Error(err)
	}

	if twoLocation != 20 {
		t.Error("did not get expected location of 20 back, got:", twoLocation)
	}

	oneLocation, err := btree.Find(int64(1))
	if err != nil {
		t.Error(err)
	}

	if oneLocation != 10 {
		t.Error("did not get expected location of 10 back, got:", oneLocation)
	}

	threeLocation, err := btree.Find(int64(3))
	if err != nil {
		t.Error(err)
	}

	if threeLocation != 30 {
		t.Error("did not get expected location of 30 back, got:", threeLocation)
	}
}
