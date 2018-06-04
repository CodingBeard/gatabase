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
	btreeWriteError = gataerrors.NewGataError("unable to write to index readWriteSeeker")
	btreeWriteRootWriteNodeError = gataerrors.NewGataError("unable to write root node to end of ReadWriteSeeker")
	btreeWriteRootSeekStartIndexError = gataerrors.NewGataError("unable to seek to the start of the ReadWriteSeeker")
	btreeWriteRootWriteRootLocationError = gataerrors.NewGataError("unable to write the location of the root in the ReadWriteSeeker")
	BtreeFindGetRootError = gataerrors.NewGataError("error when attempting to find a key, the root could not be found")
	btreeFindNodeByKeyNearestNodeFoundError = gataerrors.NewGataError("did not find the node containing the key but did find the nearest node")
	BtreeIndexSeekError = gataerrors.NewGataError("error when seeking in index")
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

	_, isInt := key.(int64)
	_, isString := key.(int64)
	_, isDate := key.(int64)

	var keyType int8

	if isInt {
		keyType = btreeElementTypeInt
	} else if isString {
		keyType = btreeElementTypeString
	} else if isDate {
		keyType = btreeElementTypeDate
	}

	node, err := btree.findNodeByKey(0, key)
	if err == nil {
		return BTreeDuplicateKeyError
	}

	if int8(len(node.Elements)) < btree.MaxElementsPerNode {
		node.AddElement(NewBTreeElement(keyType, key, location, btreeElementNoChildValue, btreeElementNoChildValue))
	}

	return nil
}

// Find the location of a key
func (btree *BTree) Find(key interface{}) (int64, error) {
	node, err := btree.findNodeByKey(0, key)

	if err != nil && btreeFindNodeByKeyNearestNodeFoundError.IsSame(err) {
		return 0, BTreeKeyNotFoundError
	}

	element, err := node.GetElementByKey(key)

	if err != nil {
		return 0, BTreeKeyNotFoundError
	}

	return element.Location, nil
}

// Find the node a key belongs to or the nearest node to it
func (btree *BTree) findNodeByKey(location int64, key interface{}) (BTreeNode, error) {
	// If we're starting from the root
	if location == 0 {
		node, err := btree.getRoot()

		if err != nil && !bTreeNoRootError.IsSame(err) {
			return node, BtreeFindGetRootError.SetUnderlying(err)
		}

		// If the key is in the root, return that
		if _, err = node.GetElementByKey(key); err == nil {
			return node, nil
		}

		nearestNodeLocation, err := node.GetNearestNodeLocationByKey(key)

		if err != nil && NoNearestNodeFoundByKeyError.IsSame(err) {
			return node, btreeFindNodeByKeyNearestNodeFoundError
		}

		return btree.findNodeByKey(nearestNodeLocation, key)
	}

	// We're now recursing
	node, err := btree.readNode(location)

	if err != nil {
		return BTreeNode{}, err
	}

	// If the key is in the node, return that
	if _, err = node.GetElementByKey(key); err == nil {
		return node, nil
	}

	nearestNodeLocation, err := node.GetNearestNodeLocationByKey(key)

	if NoNearestNodeFoundByKeyError.IsSame(err) {
		return node, btreeFindNodeByKeyNearestNodeFoundError
	}

	return btree.findNodeByKey(nearestNodeLocation, key)
}

// Write a node to the index, return the location it wrote at
func (btree *BTree) writeNode(node BTreeNode) (int64, error) {

	// Seek to the end of the file
	location, err := btree.Index.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, btreeWriteNodeSeekToEndError.SetUnderlying(err)
	}

	// If this is a new index file that does not yet have a root location
	if location < btreeNodeLengthLocationPadLength {
		_, err = btree.Index.Write([]byte(fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s")))

		if err != nil {
			return 0, btreeWriteError.SetUnderlying(err)
		}

		location, err = btree.Index.Seek(btreeNodeLengthLocationPadLength, io.SeekStart)

		if err != nil {
			return 0, btreeWriteNodeSeekToEndError.SetUnderlying(err)
		}
	}

	serialised, err := node.Serialise()

	if err != nil {
		return 0, err
	}

	_, err = btree.Index.Write(serialised)

	if err != nil {
		return 0, btreeWriteError.SetUnderlying(err)
	}

	// If the node we're writing was read from elsewhere in the index update
	// The original location to point to the new one
	if node.Location != btreeNodeNoLocationValue {
		_, err = btree.Index.Seek(node.Location, io.SeekStart)

		if err != nil {
			return 0, BtreeIndexSeekError.SetUnderlying(err)
		}

		_, err = btree.Index.Write([]byte(btreeNodeMoved))

		if err != nil {
			return 0, btreeWriteError.SetUnderlying(err)
		}

		btree.Index.Write([]byte(fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s", strconv.FormatInt(location, 10))))

		if err != nil {
			return 0, btreeWriteError.SetUnderlying(err)
		}

		return node.Location, nil
	} else {
		return location, nil
	}
}

// Read the node at a specific location in the index
func (btree *BTree) readNode(location int64) (BTreeNode, error) {
	_, err := btree.Index.Seek(location, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BtreeIndexSeekError.SetUnderlying(err)
	}

	node, err := DeserialiseBTreeNode(btree.Index, location)
	if err != nil {
		return BTreeNode{}, err
	}

	return node, nil
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
		return BTreeNode{}, BtreeIndexSeekError.SetUnderlying(err)
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
	root, err := DeserialiseBTreeNode(btree.Index, 0)

	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	return root, nil
}
