package storage

import (
	"testing"
	"reflect"
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
