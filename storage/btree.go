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
func (tree *BTree) Insert(key interface{}, location int64) (error) {

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

	node, err := tree.findNodeByKey(0, key)
	if err == nil {
		return BTreeDuplicateKeyError
	}

	if int8(len(node.Elements)) < tree.MaxElementsPerNode {
		node.AddElement(NewBTreeElement(keyType, key, location, btreeElementNoChildValue, btreeElementNoChildValue))

		if node.ParentId == btreeNodeParentIdNoValue {
			tree.writeRoot(node)
		} else {
			tree.writeNode(node)
		}
	}

	return nil
}

// Find the location of a key
func (tree *BTree) Find(key interface{}) (int64, error) {
	node, err := tree.findNodeByKey(0, key)

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
func (tree *BTree) findNodeByKey(location int64, key interface{}) (BTreeNode, error) {
	// If we're starting from the root
	if location == 0 {
		node, err := tree.getRoot()

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

		return tree.findNodeByKey(nearestNodeLocation, key)
	}

	// We're now recursing
	node, err := tree.readNode(location)

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

	return tree.findNodeByKey(nearestNodeLocation, key)
}

// Write a node to the index, return the location it wrote at
func (tree *BTree) writeNode(node BTreeNode) (int64, error) {

	// Seek to the end of the file
	location, err := tree.Index.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, btreeWriteNodeSeekToEndError.SetUnderlying(err)
	}

	// If this is a new index file that does not yet have a root location
	if location < btreeNodeLengthLocationPadLength {
		_, err = tree.Index.Write([]byte(fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s")))

		if err != nil {
			return 0, btreeWriteError.SetUnderlying(err)
		}

		location, err = tree.Index.Seek(btreeNodeLengthLocationPadLength, io.SeekStart)

		if err != nil {
			return 0, btreeWriteNodeSeekToEndError.SetUnderlying(err)
		}
	}

	serialised, err := node.Serialise()

	if err != nil {
		return 0, err
	}

	_, err = tree.Index.Write(serialised)

	if err != nil {
		return 0, btreeWriteError.SetUnderlying(err)
	}

	// If the node we're writing was read from elsewhere in the index update
	// The original location to point to the new one
	if node.Location != btreeNodeNoLocationValue && node.ParentId != btreeNodeParentIdNoValue {
		_, err = tree.Index.Seek(node.Location, io.SeekStart)

		if err != nil {
			return 0, BtreeIndexSeekError.SetUnderlying(err)
		}

		_, err = tree.Index.Write([]byte(btreeNodeMoved))

		if err != nil {
			return 0, btreeWriteError.SetUnderlying(err)
		}

		tree.Index.Write([]byte(fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s", strconv.FormatInt(location, 10))))

		if err != nil {
			return 0, btreeWriteError.SetUnderlying(err)
		}

		return node.Location, nil
	} else {
		return location, nil
	}
}

// Read the node at a specific location in the index
func (tree *BTree) readNode(location int64) (BTreeNode, error) {
	_, err := tree.Index.Seek(location, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BtreeIndexSeekError.SetUnderlying(err)
	}

	node, err := DeserialiseBTreeNode(tree.Index, location)
	if err != nil {
		return BTreeNode{}, err
	}

	return node, nil
}

// Write the root node and update the root reference
func (tree *BTree) writeRoot(node BTreeNode) (int64, error) {
	location, err := tree.writeNode(node)

	if err != nil {
		return 0, btreeWriteRootWriteNodeError.SetUnderlying(err)
	}

	locationString := strconv.FormatInt(location, 10)
	locationString = fmt.Sprintf("%0"+strconv.Itoa(btreeNodeLengthLocationPadLength)+"s", locationString)

	_, err = tree.Index.Seek(0, io.SeekStart)

	if err != nil {
		return 0, btreeWriteRootSeekStartIndexError.SetUnderlying(err)
	}

	_, err = tree.Index.Write([]byte(locationString))

	if err != nil {
		return 0, btreeWriteRootWriteRootLocationError.SetUnderlying(err)
	}

	return location, nil
}

// Seek to and unserialise the root
func (tree *BTree) getRoot() (BTreeNode, error) {
	if root, ok := tree.cache[btreeRootCacheIndex]; ok {
		return root, nil
	}

	// Get the location of the root node
	_, err := tree.Index.Seek(0, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BtreeIndexSeekError.SetUnderlying(err)
	}

	rootLocationString := make([]byte, 20)
	_, err = tree.Index.Read(rootLocationString)

	// If it is an empty index
	if err == io.EOF {
		return NewBTreeNode(
			false,
			btreeNodeParentIdNoValue,
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

	_, err = tree.Index.Seek(rootLocation, io.SeekStart)
	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	// Deserialise the root
	root, err := DeserialiseBTreeNode(tree.Index, 0)

	if err != nil {
		return BTreeNode{}, BtreeRootUnableToDeserialise.SetUnderlying(err)
	}

	return root, nil
}
