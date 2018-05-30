package storage

import (
	"testing"
	"reflect"
	"github.com/codingbeard/gatabase/gataerrors"
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

func TestBTree_getRoot(t *testing.T) {
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

	if !gataerrors.IsSameError(err, BtreeRootUnableToDeserialise) {
		t.Error("did not get expected error when unable to seek to root")
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
