package storage

import (
	"io"
	"errors"
	"strconv"
	"github.com/codingbeard/gatabase/gataerrors"
)

const (
	btreeRootCacheIndex = int64(-1)
)

var (
	BTreeDuplicateKeyError       = gataerrors.NewGataError("duplicate key used in insert when btree is in unique mode", errors.New(""))
	BTreeKeyNotFoundError        = gataerrors.NewGataError("unable to find key in btree index", errors.New(""))
	bTreeNoRootError             = gataerrors.NewGataError("no root node exists", errors.New(""))
	BtreeRootUnableToReadNodeLocation = gataerrors.NewGataError("unable to read the location of the node", errors.New(""))
	BtreeRootUnableToDeserialise = gataerrors.NewGataError("expected to find root at location but could not seek to it", errors.New(""))
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
			bTreeNoRootError.SetUnderlying(err)
	}

	if err != nil {
		BtreeRootUnableToReadNodeLocation.SetUnderlying(err)
	}

	rootLocation, err := strconv.ParseInt(string(rootLocationString), 10, 64)

	_, err = btree.Index.Seek(rootLocation, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	root, err := DeserialiseBTreeNode(btree.Index)

	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	return root, nil
}
