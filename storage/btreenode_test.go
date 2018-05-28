package storage

import (
	"testing"
	"reflect"
	"time"
)

func TestNewBTreeNode(t *testing.T) {
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

	node := NewBTreeNode(false, parentId, 1, elements, path)

	if node.ParentId != btreeNodeParentIdNoValue {
		t.Error("ParentId  not injected correctly")
	}
	if !reflect.DeepEqual(node.Elements, elements) {
		t.Error("Elements  not injected correctly")
	}
	if !reflect.DeepEqual(node.Path, make([]int32, 0)) {
		t.Error("Path  not injected correctly")
	}
}


func TestBTreeNode_Serialize(t *testing.T) {
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

	serialised := node.Serialize()

	buffer, err := NewMemoryImmutableFile(serialised, make([]byte, 0))

	if err != nil {
		panic(err)
	}

	unserialisedNode := DeserialiseBTreeNode(&buffer)

	if reflect.DeepEqual(unserialisedNode, node) {
		t.Error("deserialised node does not match original")
	}
}

func TestBTreeNode_Sort(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	// Test int sorting
	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(4),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	node.Sort()

	if node.Elements[0].KeyInt != 1 {
		t.Error("sort did not put 1 first")
	}
	if node.Elements[1].KeyInt != 2 {
		t.Error("sort did not put 2 second")
	}
	if node.Elements[2].KeyInt != 3 {
		t.Error("sort did not put 3 third")
	}
	if node.Elements[3].KeyInt != 4 {
		t.Error("sort did not put 4 fourth")
	}

	// Test string sorting
	elements[0] = NewBTreeElement(
		btreeElementTypeString,
		"d",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeString,
		"c",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeString,
		"b",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeString,
		"a",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	node.Sort()

	if node.Elements[0].KeyString != "a" {
		t.Error("sort did not put a first, got: ", node.Elements[0].KeyString)
	}
	if node.Elements[1].KeyString != "b" {
		t.Error("sort did not put b second, got: ", node.Elements[1].KeyString)
	}
	if node.Elements[2].KeyString != "c" {
		t.Error("sort did not put c third, got: ", node.Elements[2].KeyString)
	}
	if node.Elements[3].KeyString != "d" {
		t.Error("sort did not put d fourth, got: ", node.Elements[3].KeyString)
	}

	// Test date sorting
	elements[0] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			4,
			0,
			&time.Location{}),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			3,
			0,
			&time.Location{}),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			2,
			0,
			&time.Location{}),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			1,
			0,
			&time.Location{}),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	node.Sort()

	if node.Elements[0].KeyDate.Second() != 1 {
		t.Error("sort did not put 1 first")
	}
	if node.Elements[1].KeyDate.Second() != 2 {
		t.Error("sort did not put 2 second")
	}
	if node.Elements[2].KeyDate.Second() != 3 {
		t.Error("sort did not put 3 third")
	}
	if node.Elements[3].KeyDate.Second() != 4 {
		t.Error("sort did not put 4 fourth")
	}
}

func TestBTreeNode_AddElement(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 0)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	// Test adding the first element
	element1 := NewBTreeElement(
		btreeElementTypeInt,
		int64(4),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node.AddElement(element1)

	if len(node.Elements) != 1 {
		t.Error("first element was not added")
	}

	if node.Elements[0].KeyInt != element1.KeyInt {
		t.Error("first element was not added correctly")
	}

	// Test adding an element which will sort into the front
	element2 := NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node.AddElement(element2)

	if len(node.Elements) != 2 {
		t.Error("second element was not added")
	}

	if node.Elements[0].KeyInt != element2.KeyInt {
		t.Error("second element was not added correctly or sorted incorrectly (0)")
	}

	if node.Elements[1].KeyInt != element1.KeyInt {
		t.Error("second element was not added correctly or sorted incorrectly (1)")
	}

	// Test adding an element which will sort into the middle
	element3 := NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node.AddElement(element3)

	if len(node.Elements) != 3 {
		t.Error("second element was not added")
	}

	if node.Elements[0].KeyInt != element2.KeyInt {
		t.Error("third element was not added correctly or sorted incorrectly (0)")
	}

	if node.Elements[1].KeyInt != element3.KeyInt {
		t.Error("third element was not added correctly or sorted incorrectly (1)")
	}

	if node.Elements[2].KeyInt != element1.KeyInt {
		t.Error("third element was not added correctly or sorted incorrectly (2)")
	}
}

func TestBTreeNode_RemoveElement(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	// Test int sorting
	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(4),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	node.RemoveElement(int64(1))

	if len(node.Elements) != 3 {
		t.Error("failed to remove an element expected 3, got: ", len(node.Elements))
	}
}

func TestBTreeNode_GetElementByKey(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	// Test int get
	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(2),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(4),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	element, err := node.GetElementByKey(int64(3))

	if err != nil {
		t.Error("did not find element when we expected to")
	}

	if element.KeyInt != int64(3) {
		t.Error("did not find correct element, expecting: 3, got:", element.KeyInt)
	}

	// Test string get
	elements[0] = NewBTreeElement(
		btreeElementTypeString,
		"c",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeString,
		"b",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeString,
		"d",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeString,
		"a",
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	element, err = node.GetElementByKey("c")

	if err != nil {
		t.Error("did not find element when we expected to")
	}

	if element.KeyString != "c" {
		t.Error("did not find correct element, expecting: c, got:", element.KeyString)
	}

	// Test date get
	findDateKey := time.Date(
		2018,
		05,
		27,
		10,
		20,
		3,
		0,
		&time.Location{},
	)

	elements[0] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			1,
			0,
			&time.Location{},
		),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			2,
			0,
			&time.Location{},
		),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeDate,
		findDateKey,
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			4,
			0,
			&time.Location{},
		),
		int64(345),
		btreeElementNoChildValue,
		btreeElementNoChildValue,
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	element, err = node.GetElementByKey(findDateKey)

	if err != nil {
		t.Error("did not find element when we expected to")
	}
	if element.KeyDate.Unix() != findDateKey.Unix() {
		t.Error("did not find correct element, expecting:", findDateKey.Unix(), ", got:", element.KeyDate.Unix())
	}

	_, err = node.GetElementByKey(int64(123))

	if err == nil {
		t.Error("found element when we didn't expect to")
	}
}

func TestBTreeNode_GetNearestNodeLocationByKey(t *testing.T) {
	parentId := btreeNodeParentIdNoValue
	path := make([]int32, 0)
	elements := make([]BTreeElement, 4)

	// Test int get 1, 3, 7, 10
	elements[0] = NewBTreeElement(
		btreeElementTypeInt,
		int64(1),
		int64(345),
		int64(1),
		int64(2),
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeInt,
		int64(3),
		int64(345),
		int64(2),
		int64(3),
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeInt,
		int64(7),
		int64(345),
		int64(3),
		int64(4),
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeInt,
		int64(10),
		int64(345),
		int64(4),
		int64(5),
	)

	node := NewBTreeNode(false, parentId, 1, elements, path)

	nearestNodeLocation, err := node.GetNearestNodeLocationByKey(int64(4))

	if err != nil {
		t.Error(err)
	}

	if nearestNodeLocation != int64(3) {
		t.Error("did not get more location (3) of nearest element to key, got:", nearestNodeLocation)
	}

	nearestNodeLocation, err = node.GetNearestNodeLocationByKey(int64(15))

	if err != nil {
		t.Error(err)
	}

	if nearestNodeLocation != int64(5) {
		t.Error("did not get more location (5) of nearest element to key, got:", nearestNodeLocation)
	}

	// Test string get a, c, g, j
	elements[0] = NewBTreeElement(
		btreeElementTypeString,
		"a",
		int64(345),
		int64(1),
		int64(2),
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeString,
		"c",
		int64(345),
		int64(2),
		int64(3),
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeString,
		"g",
		int64(345),
		int64(3),
		int64(4),
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeString,
		"j",
		int64(345),
		int64(4),
		int64(5),
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	nearestNodeLocation, err = node.GetNearestNodeLocationByKey("d")

	if err != nil {
		t.Error(err)
	}

	if nearestNodeLocation != int64(3) {
		t.Error("did not get more location (3) of nearest element to key, got:", nearestNodeLocation)
	}

	nearestNodeLocation, err = node.GetNearestNodeLocationByKey("o")

	if err != nil {
		t.Error(err)
	}

	if nearestNodeLocation != int64(5) {
		t.Error("did not get more location (5) of nearest element to key, got:", nearestNodeLocation)
	}

	// Test date get 1, 3, 7, 10 seconds
	elements[0] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			1,
			0,
			&time.Location{},
		),
		int64(345),
		int64(1),
		int64(2),
	)

	elements[1] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			3,
			0,
			&time.Location{},
		),
		int64(345),
		int64(2),
		int64(3),
	)

	elements[2] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			7,
			0,
			&time.Location{},
		),
		int64(345),
		int64(3),
		int64(4),
	)

	elements[3] = NewBTreeElement(
		btreeElementTypeDate,
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			10,
			0,
			&time.Location{},
		),
		int64(345),
		int64(4),
		int64(5),
	)

	node = NewBTreeNode(false, parentId, 1, elements, path)

	nearestNodeLocation, err = node.GetNearestNodeLocationByKey(
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			4,
			0,
			&time.Location{},
		),
	)

	if err != nil {
		t.Error(err)
	}

	if nearestNodeLocation != int64(3) {
		t.Error("did not get more location (3) of nearest element to key, got:", nearestNodeLocation)
	}

	nearestNodeLocation, err = node.GetNearestNodeLocationByKey(
		time.Date(
			2018,
			05,
			27,
			10,
			20,
			15,
			0,
			&time.Location{},
		),
	)

	if err != nil {
		t.Error(err)
	}

	if nearestNodeLocation != int64(5) {
		t.Error("did not get more location (5) of nearest element to key, got:", nearestNodeLocation)
	}
}
