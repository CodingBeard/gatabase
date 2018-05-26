package storage

import (
	"time"
	"math/big"
)

const (
	// Key type int
	btreeElementTypeInt      = int8(0)
	// Key type string
	btreeElementTypeString   = int8(1)
	// Key type date
	btreeElementTypeDate     = int8(2)
	// Value which means the element has no children when used for the
	// LessLocation or MoreLocation
	btreeElementNoChildValue = int64(-1)
)

// A btree element which lives inside a btree node
// Contains a key of variable type, byte/key location of the data attached to
// The key, and byte/key locations of the node with keys more or less than this
type BTreeElement struct {
	KeyType      int8
	KeyInt       int64
	KeyString    string
	KeyDate      time.Time
	Location     int64
	LessLocation int64
	MoreLocation int64
}

// Construct a new BTreeElement
func NewBTreeElement(keyType int8, keyInt int64, keyString string, keyDate time.Time, location int64, lessLocation int64, moreLocation int64) (BTreeElement) {
	return BTreeElement{
		KeyType:      keyType,
		KeyInt:       keyInt,
		KeyString:    keyString,
		KeyDate:      keyDate,
		Location:     location,
		LessLocation: lessLocation,
		MoreLocation: moreLocation,
	}
}

// Whether the current element has children with keys larger or smaller than it
func (element *BTreeElement) HasChildren() (bool) {
	if element.LessLocation != btreeElementNoChildValue || element.MoreLocation != btreeElementNoChildValue {
		return true
	}
	return false
}

// Does the element contain an int key
func (element *BTreeElement) IsIntType() (bool) {
	return element.KeyType == btreeElementTypeInt
}

// Does the element contain an string key
func (element *BTreeElement) IsStringType() (bool) {
	return element.KeyType == btreeElementTypeString
}

// Does the element contain an date key
func (element *BTreeElement) IsDateType() (bool) {
	return element.KeyType == btreeElementTypeDate
}

// Return the distance between the supplied key and the element's key
func (element *BTreeElement) GetDistanceFromIntKey(key int64) (int64) {
	return key - element.KeyInt
}

// Return the distance between the supplied key and the element's key
func (element *BTreeElement) GetDistanceFromStringKey(key string) (*big.Int) {
	return new(big.Int).Sub(
		new(big.Int).SetBytes([]byte(key)),
		new(big.Int).SetBytes([]byte(element.KeyString)))
}

// Return the distance between the supplied key and the element's key
func (element *BTreeElement) GetDistanceFromDateKey(key time.Time) (int64) {
	return key.Unix() - element.KeyDate.Unix()
}
