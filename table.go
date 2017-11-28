package data

import (
	"encoding/binary"
	"fmt"
)

const (
	maxAttributeNameLength = 60
	maxCharLength          = 2000
)

const (
	IntegerSize  = 8
	FloatSize    = 8
	DatetimeSize = 8
	BooleanSize  = 1
)

type (
	Integer  int64
	Float    float64
	Datetime int64
	Boolean  bool
	Char     []byte
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

func (c Char) String() string {
	return string(c)
}

func (c Char) bytes() []byte {
	return c
}

func (c Char) size() int {
	return len(c)
}

func newChar(str string, length int) Char {
	if len(str) > length {
		panic("String is greater than char size")
	}

	b := make([]byte, length)
	copy(b, str)
	return b
}

func newTableTuple(name string, columns address, records address) tuple {
	if len(name) > maxAttributeNameLength {
		panic(fmt.Sprintf("Table name can't be greater than %d bytes", maxAttributeNameLength))
	}

	return tuple{
		tupleElement{maxAttributeNameLength, false, newChar(name, maxAttributeNameLength)},
		tupleElement{IntegerSize, false, Integer(columns)},
		tupleElement{IntegerSize, false, Integer(records)},
	}
}
