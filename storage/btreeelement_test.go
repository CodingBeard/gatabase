package storage

import (
	"testing"
	"time"
	"math/big"
)

func TestNewBTreeElement(t *testing.T) {
	// Test constructor injects all variables correctly
	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := int64(678)
	moreLocation := int64(91011)

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if element.KeyType != keyType {
		t.Error("KeyType not injected correctly")
	}
	if element.KeyInt != keyInt {
		t.Error("KeyInt not injected correctly")
	}
	if element.KeyString != keyString {
		t.Error("KeyString not injected correctly")
	}
	if element.KeyDate != keyDate {
		t.Error("KeyDate not injected correctly")
	}
	if element.Location != location {
		t.Error("Location not injected correctly")
	}
	if element.LessLocation != lessLocation {
		t.Error("LessLocation not injected correctly")
	}
	if element.MoreLocation != moreLocation {
		t.Error("MoreLocation not injected correctly")
	}
}

func TestBTreeElement_HasChildren(t *testing.T) {
	// Test element without children
	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if element.HasChildren() {
		t.Error("HasChildren returning true when constructed with storage.btreeElementNoChildValue")
	}

	// Test element with less location
	lessLocation = 123
	moreLocation = btreeElementNoChildValue

	element = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if !element.HasChildren() {
		t.Error("HasChildren returning false when constructed with a lessLocation value")
	}

	// Test element with more location
	lessLocation = btreeElementNoChildValue
	moreLocation = 123

	element = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if !element.HasChildren() {
		t.Error("HasChildren returning false when constructed with a moreLocation value")
	}

	// Test element with more and less locations
	lessLocation = 123
	moreLocation = 123

	element = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if !element.HasChildren() {
		t.Error("HasChildren returning false when constructed with a moreLocation and lessLocation value")
	}
}

func TestBTreeElement_IsIntType(t *testing.T) {
	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if !element.IsIntType() {
		t.Error("element is not int type")
	}

	keyType = btreeElementTypeDate
	element = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if element.IsIntType() {
		t.Error("element of type date claiming to be int")
	}
}

func TestBTreeElement_IsStringType(t *testing.T) {
	keyType := btreeElementTypeString
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if !element.IsStringType() {
		t.Error("element is not string type")
	}

	keyType = btreeElementTypeDate
	element = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if element.IsStringType() {
		t.Error("element of type date claiming to be string")
	}
}

func TestBTreeElement_IsDateType(t *testing.T) {
	keyType := btreeElementTypeDate
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if !element.IsDateType() {
		t.Error("element is not date type")
	}

	keyType = btreeElementTypeInt
	element = NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if element.IsDateType() {
		t.Error("element of type int claiming to be date")
	}
}

func TestBTreeElement_GetDistanceFromIntKey(t *testing.T) {
	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	if element.GetDistanceFromIntKey(124) != 1 {
		t.Error(
			"distance between keys is not expected 1, got: ",
			element.GetDistanceFromIntKey(124))
	}
}

func TestBTreeElement_GetDistanceFromStringKey(t *testing.T) {
	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	one, _ := new(big.Int).SetString("1", 10)

	if element.GetDistanceFromStringKey("aaaaab").Cmp(one) != 0 {
		t.Error(
			"distance between keys is not expected",
			one,
			"got:",
			element.GetDistanceFromStringKey("aaaaab"))
	}
}

func TestBTreeElement_GetDistanceFromDateKey(t *testing.T) {
	keyType := btreeElementTypeInt
	keyInt := int64(123)
	keyDate := time.Date(
		2018,
		5,
		26,
		15,
		31,
		17,
		0,
		&time.Location{})
	keyString := "aaaaaa"
	location := int64(345)
	lessLocation := btreeElementNoChildValue
	moreLocation := btreeElementNoChildValue

	element := NewBTreeElement(
		keyType,
		keyInt,
		keyString,
		keyDate,
		location,
		lessLocation,
		moreLocation)

	compare := time.Date(
		2018,
		5,
		26,
		15,
		31,
		18,
		0,
		&time.Location{})

	if element.GetDistanceFromDateKey(compare) != 1 {
		t.Error(
			"distance between keys is not expected 1, got: ",
			element.GetDistanceFromDateKey(compare))
	}
}

