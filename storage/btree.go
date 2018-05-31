package storage

import (
	"io"
	"strconv"
	"github.com/codingbeard/gatabase/gataerrors"
	"fmt"
)

const (
	btreeRootCacheIndex = int64(-1)
)

var (
	BTreeDuplicateKeyError       = gataerrors.NewGataError("duplicate key used in insert when btree is in unique mode")
	BTreeKeyNotFoundError        = gataerrors.NewGataError("unable to find key in btree index")
	bTreeNoRootError             = gataerrors.NewGataError("no root node exists")
	BtreeRootUnableToReadNodeLocation = gataerrors.NewGataError("unable to read the location of the node")
	BtreeRootUnableToDeserialise = gataerrors.NewGataError("expected to find root at location but could not seek to it")
	btreeWriteNodeSeekToEndError = gataerrors.NewGataError("unable to seek to end of index readWriteSeeker to write node")
	btreeWriteNodeWriteError = gataerrors.NewGataError("unable to write to index readWriteSeeker")
	btreeWriteRootWriteNodeError = gataerrors.NewGataError("unable to write root node to end of ReadWriteSeeker")
	btreeWriteRootSeekStartIndexError = gataerrors.NewGataError("unable to seek to the start of the ReadWriteSeeker")
	btreeWriteRootWriteRootLocationError = gataerrors.NewGataError("unable to write the location of the root in the ReadWriteSeeker")
)

// BTree index
type BTree struct {
	Index io.ReadWriteSeeker
	cache map[int64]BTreeNode
	MaxElementsPerNode int8
	Unique bool
}

// Construct a new btree index
func NewBTree(index io.ReadWriteSeeker, maxElementCount int8, unique bool) (BTree) {
	return BTree{
		Index:index,
		MaxElementsPerNode:maxElementCount,
		Unique:unique,
	}
}

// Insert a new key-location pair to the index
func (btree *BTree) Insert(key interface{}, location int64) (error) {

	return nil
}

// Find the location of a key
func (btree *BTree) Find(key interface{}) (int64, error) {

	return 0, BTreeKeyNotFoundError
}

// Write a node to the index, return the location it wrote at
func (btree *BTree) writeNode(node BTreeNode) (int64, error) {
	location, err := btree.Index.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, btreeWriteNodeSeekToEndError.SetUnderlying(err)
	}

	serialised, err := node.Serialise()

	if err != nil {
		return 0, err
	}

	_, err = btree.Index.Write(serialised)

	if err != nil {
		return 0, btreeWriteNodeWriteError.SetUnderlying(err)
	}

	return location, nil
}

// Write the root node and update the root reference
func (btree *BTree) writeRoot(node BTreeNode) (int64, error) {
	location, err := btree.writeNode(node)

	if err != nil {
		return 0, btreeWriteRootWriteNodeError.SetUnderlying(err)
	}

	locationString := strconv.FormatInt(location, 10)
	locationString = fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s", locationString)

	_, err = btree.Index.Seek(0, io.SeekStart)

	if err != nil {
		return 0, btreeWriteRootSeekStartIndexError.SetUnderlying(err)
	}

	_, err = btree.Index.Write([]byte(locationString))

	if err != nil {
		return 0, btreeWriteRootWriteRootLocationError.SetUnderlying(err)
	}

	return location, nil
}

// Seek to and unserialise the root
func (btree *BTree) getRoot() (BTreeNode, error) {
	if root, ok := btree.cache[btreeRootCacheIndex]; ok {
		return root, nil
	}

	// Get the location of the root node
	_, err := btree.Index.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}

	rootLocationString := make([]byte, 20)
	_, err = btree.Index.Read(rootLocationString)

	// If it is an empty index
	if err == io.EOF {
		return NewBTreeNode(
			false,
			0,
			0,
			make([]BTreeElement, 0),
			make([]int32, 0),
		),
			bTreeNoRootError.SetUnderlying(err)
	}

	if err != nil {
		BtreeRootUnableToReadNodeLocation.SetUnderlying(err)
	}

	// Parse the root location
	rootLocation, err := strconv.ParseInt(string(rootLocationString), 10, 64)

	_, err = btree.Index.Seek(rootLocation, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	// Deserialise the root
	root, err := DeserialiseBTreeNode(btree.Index)

	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	return root, nil
}
