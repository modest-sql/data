package data

import "encoding/binary"

const (
	maxAttributeNameLength = 60
	maxCharLength          = 2000
)

type (
	Integer  int64
	Float    float64
	Datetime int64
	Boolean  bool
	Char     string
)

func (i Integer) bytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(i))
	return bytes
}

func (i Integer) size() int {
	return binary.Size(i)
}

func (f Float) bytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(f))
	return bytes
}

func (f Float) size() int {
	return binary.Size(f)
}

func (d Datetime) bytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(d))
	return bytes
}

func (d Datetime) size() int {
	return binary.Size(d)
}

func (b Boolean) bytes() []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func (b Boolean) size() int {
	return binary.Size(b)
}

func (c Char) bytes() []byte {
	return []byte(c)
}

func (c Char) size() int {
	return len(c)
}

func newChar(length int) Char {
	return Char(make([]byte, length))
}

func newTableRecord() record {
	return newRecord(tuple{newChar(maxAttributeNameLength), Integer(0), Integer(0)})
}
