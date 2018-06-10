package storage

import (
	"testing"
	"reflect"
	"io"
	"strconv"
)

func nodesEqual(a, b BTreeNode, ignoreLocation bool) (bool, error) {
	if ignoreLocation {
		a.Location = -1
		b.Location = -1
	}

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

	tree := NewBTree(index, 4, true)

	if !reflect.DeepEqual(index, tree.Index) {
		t.Error("Index injected incorrectly")
	}

	if tree.MaxElementsPerNode != 4 {
		t.Error("MaxElementsPerNode injected incorrectly. Expected 4, got:", tree.MaxElementsPerNode)
	}

	if tree.Unique != true {
		t.Error("Unique injected incorrectly. Expected true, got:", tree.Unique)
	}
}

func TestBtree_writeNode(t *testing.T) {
	// New btree with a memory index
	index := &MemoryFileHandle{data:[]byte("123")}
	tree := NewBTree(index, 4, true)

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
	location, err := tree.writeNode(node)

	if err != nil {
		t.Error(err)
	}

	if location != btreeNodeLengthLocationPadLength {
		t.Error("expected to write after the root location, wrote at: ", location)
	}

	_, err = tree.Index.Seek(btreeNodeLengthLocationPadLength, io.SeekStart)

	if err != nil {
		t.Error(err)
	}

	read := make([]byte, len(serialised))

	_, err = tree.Index.Read(read)

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(serialised, read) {
		t.Error("did not read serialised node from index")
	}

	// test writing seeks to the end
	elements = make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(123),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	endOfFile, err := tree.Index.Seek(0, io.SeekEnd)
	if err != nil {
		t.Error(err)
	}

	_, err = tree.Index.Seek(0, io.SeekStart)
	if err != nil {
		t.Error(err)
	}

	location, err = tree.writeNode(node)
	if err != nil {
		t.Error(err)
	}

	if location != endOfFile {
		t.Error("did not write at expected end of file (", endOfFile, ") wrote at:", location)
	}
}

func TestBtree_readNode(t *testing.T) {
	// New btree with a memory index
	index := &MemoryFileHandle{data:[]byte("123")}
	tree := NewBTree(index, 4, true)

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

	// Write the node
	location, err := tree.writeNode(node)

	if err != nil {
		t.Error(err)
	}

	if location != btreeNodeLengthLocationPadLength {
		t.Error("expected to write after the root location, wrote at: ", location)
	}

	read, err := tree.readNode(btreeNodeLengthLocationPadLength)

	if err != nil {
		t.Error(err)
	}

	seralisedRead, err := read.Serialise()

	if err != nil {
		t.Error(err)
	}

	node.Location = 20

	// Serialise the node so we can compare to it later
	serialised, err := node.Serialise()

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(serialised, seralisedRead) {
		t.Error("did not read serialised node from index")
	}
}

func TestBtree_writeReadNode(t *testing.T) {
	// New btree with a memory index
	index := &MemoryFileHandle{data:[]byte("123")}
	tree := NewBTree(index, 4, true)

	// Create a node
	parentId := int32(1)
	path := make([]int32, 0)
	elements := make([]BTreeElement, 1)

	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(10),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	// Write the node
	location, err := tree.writeNode(node)
	if err != nil {
		t.Error(err)
	}

	readNode, err := tree.readNode(location)
	if err != nil {
		t.Error(err)
	}

	if readNode.Location != location {
		t.Error("read node's location does not match actual location")
	}

	readNode.Elements = append(
		readNode.Elements,
		NewBTreeElement(
			btreeElementTypeInt,
			int64(2),
			int64(20),
			btreeElementNoChildValue,
			btreeElementNoChildValue,
		),
	)

	// Write the node
	secondLocation, err := tree.writeNode(readNode)
	if err != nil {
		t.Error(err)
	}

	if location != secondLocation {
		t.Error("did not get back expected same location for written node expected:", location, "got:", secondLocation)
	}

	deletionFlag := make([]byte, 1)

	_, err = tree.Index.Seek(location, io.SeekStart)
	if err != nil {
		t.Error(err)
	}

	_, err = tree.Index.Read(deletionFlag)
	if err != nil {
		t.Error(err)
	}

	if string(deletionFlag) != btreeNodeMoved {
		t.Error("did not find expected moved flag in location of initial node write, found:", string(deletionFlag))
	}
}

func TestBtree_writeRoot(t *testing.T) {
	// New btree with a memory index with initialised data
	index := &MemoryFileHandle{data:[]byte("123")}
	tree := NewBTree(index, 4, true)

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
	location, err := tree.writeRoot(node)

	if err != nil {
		t.Error(err)
	}

	_, err = tree.Index.Seek(0, io.SeekStart)
	if err != nil {
		t.Error(err)
	}

	rootLocationString := make([]byte, 20)
	_, err = tree.Index.Read(rootLocationString)
	if err != nil {
		t.Error(err)
	}

	rootLocation, err := strconv.ParseInt(string(rootLocationString), 10, 64)

	if location != rootLocation {
		t.Error("mismatched root locations")
	}

	readRoot, err := tree.readNode(rootLocation)
	if err != nil {
		t.Error(err)
	}

	equal, err := nodesEqual(node, readRoot, true)
	if err != nil {
		t.Error(err)
	}

	if !equal {
		t.Error("did not find matching root when reading what was written")
	}
}

func TestBTree_getRoot(t *testing.T) {
	// Create a btree with an memory index
	index := &MemoryFileHandle{}
	tree := NewBTree(index, 4, true)

	root, err := tree.getRoot()

	if err != bTreeNoRootError {
		t.Error("did not get expected error when getting a non-existent root")
	}

	if !reflect.DeepEqual(root, NewBTreeNode(
		false,
		btreeNodeParentIdNoValue,
		0,
		make([]BTreeElement, 0),
		make([]int32, 0),
	)) {
		t.Error("did not get expected blank node when no root found")
	}

	tree.Index.Write([]byte("0000000000000010000"))

	root, err = tree.getRoot()

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

	_, err = tree.writeRoot(node)

	root, err = tree.getRoot()

	node.Location = root.Location

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
	tree := NewBTree(index, 4, true)

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

	nodeOneLocation, err := tree.writeNode(nodeOne)
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

	nodeTwoLocation, err := tree.writeNode(nodeTwo)
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

	_, err = tree.writeRoot(root)
	if err != nil {
		t.Error(err)
	}

	findRoot, err := tree.findNodeByKey(0, int64(2))

	nodesSame, err := nodesEqual(findRoot, root, true)
	if err != nil {
		t.Error(err)
	}

	if !nodesSame {
		t.Error("found root node not equal to created root node")
	}

	findOne, err := tree.findNodeByKey(0, int64(1))

	nodesSame, err = nodesEqual(findOne, nodeOne, true)
	if err != nil {
		t.Error(err)
	}

	if !nodesSame {
		t.Error("found node one not equal to created node one")
	}

	findTwo, err := tree.findNodeByKey(0, int64(3))

	nodesSame, err = nodesEqual(findTwo, nodeTwo, true)
	if err != nil {
		t.Error(err)
	}

	if !nodesSame {
		t.Error("found node two not equal to created node two")
	}
}

func TestBTree_Find(t *testing.T) {
	index := &MemoryFileHandle{}

	tree := NewBTree(index, 4, true)

	location, err := tree.Find(int64(1))

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

	nodeOneLocation, err := tree.writeNode(nodeOne)
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

	nodeTwoLocation, err := tree.writeNode(nodeTwo)
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

	_, err = tree.writeRoot(root)
	if err != nil {
		t.Error(err)
	}

	twoLocation, err := tree.Find(int64(2))
	if err != nil {
		t.Error(err)
	}

	if twoLocation != 20 {
		t.Error("did not get expected location of 20 back, got:", twoLocation)
	}

	oneLocation, err := tree.Find(int64(1))
	if err != nil {
		t.Error(err)
	}

	if oneLocation != 10 {
		t.Error("did not get expected location of 10 back, got:", oneLocation)
	}

	threeLocation, err := tree.Find(int64(3))
	if err != nil {
		t.Error(err)
	}

	if threeLocation != 30 {
		t.Error("did not get expected location of 30 back, got:", threeLocation)
	}
}

func TestBTree_Insert(t *testing.T) {
	// initialise an empty tree with a memory index
	index := &MemoryFileHandle{}
	tree := NewBTree(index, 4, true)

	// insert a key of 1 with a location of 10
	err := tree.Insert(int64(1), int64(10))
	if err != nil {
		t.Error(err)
	}

	// Ensure we wrote the key/location to the root
	root, err := tree.getRoot()
	if err != nil {
		t.Error(err)
	}

	if len(root.Elements) != 1 {
		t.Error("did not find expected 1 element in root, found:", len(root.Elements))
	}

	if root.Elements[0].KeyInt != int64(1) || root.Elements[0].Location != int64(10) {
		t.Error("did not find expected key value (1) and location (10) in element, found key:", root.Elements[0].KeyInt, "and location:", root.Elements[0].Location)
	}

	// Fill the root
	err = tree.Insert(int64(2), int64(20))
	if err != nil {
		t.Error(err)
	}
	err = tree.Insert(int64(3), int64(30))
	if err != nil {
		t.Error(err)
	}
	err = tree.Insert(int64(4), int64(40))
	if err != nil {
		t.Error(err)
	}

	// Ensure we wrote the key/locations to the root
	root, err = tree.getRoot()
	if err != nil {
		t.Error(err)
	}

	if len(root.Elements) != 4 {
		t.Error("did not find expected 4 elements in root, found:", len(root.Elements))
	}

	if root.Elements[0].KeyInt != int64(1) || root.Elements[0].Location != int64(10) {
		t.Error("did not find expected key value (1) and location (10) in element, found key:", root.Elements[0].KeyInt, "and location:", root.Elements[0].Location)
	}
}
