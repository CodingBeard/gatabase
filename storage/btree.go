package storage

import (
	"io"
	"errors"
	"strconv"
)

const (
	btreeRootCacheIndex = int64(-1)
)

var (
	BTreeDuplicateKeyError = errors.New("duplicate key used in insert when btree is in unique mode")
	BTreeKeyNotFoundError = errors.New("unable to find key in btree index")
	bTreeNoRootError = errors.New("no root node exists")
	BTreeRootNotFoundError = errors.New("expected to find root at location but could not seek to it")
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

	if err == io.EOF {
		return NewBTreeNode(
			false,
			0,
			0,
			make([]BTreeElement, 0),
			make([]int32, 0),
		),
			bTreeNoRootError
	}

	if err != nil {
		panic(err)
	}

	rootLocation, err := strconv.ParseInt(string(rootLocationString), 10, 64)

	_, err = btree.Index.Seek(rootLocation, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BTreeRootNotFoundError
	}

	root := DeserialiseBTreeNode(btree.Index)

	return root, nil
}
