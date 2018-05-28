package storage

import "io"

type BTree struct {
	Index io.ReadWriteSeeker
	MaxElementsPerNode int8
	Unique bool
}

func NewBTree(index io.ReadWriteSeeker, maxElementCount int8, unique bool) (BTree) {
	return BTree{
		Index:index,
		MaxElementsPerNode:maxElementCount,
		Unique:unique,
	}
}
