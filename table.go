package data

import (
	"encoding/binary"
	"fmt"

	"github.com/modest-sql/common"
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

const (
	defaultFlag = iota
	autoincrementFlag
	nullableFlag
	primaryKeyFlag
	foreignKeyFlag
)

type (
	Integer  int64
	Float    float64
	Datetime int64
	Boolean  bool
	Char     []byte
)

type columnBlock struct {
	block
	columns []column
}

type column struct {
	columnSize   uint16
	name         [maxAttributeNameLength]byte
	dataType     uint16
	dataSize     uint16
	counter      uint32
	constraints  bitmap
	defaultValue storable
}

func (c column) IsNullable() bool {
	return c.constraints.At(nullableFlag)
}

func (c column) Autoincrementable() bool {
	return c.constraints.At(autoincrementFlag)
}

func (c column) HasDefaultValue() bool {
	return c.constraints.At(defaultFlag)
}

func (c column) IsPrimaryKey() bool {
	return c.constraints.At(primaryKeyFlag)
}

func (c column) IsForeignKey() bool {
	return c.constraints.At(foreignKeyFlag)
}

func (cb columnBlock) bytes() (bytes []byte) {
	bytes = cb.block.bytes()

	for _, column := range cb.columns {
		bytes = append(bytes, column.bytes()...)
	}

	return bytes
}

func (cb columnBlock) size() (size int) {
	size = cb.block.size()

	for _, column := range cb.columns {
		size += column.size()
	}

	return size
}

func (c column) bytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.LittleEndian.PutUint16(bytes[:4], c.columnSize)

	bytes = append(bytes, c.name[:]...)

	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint16(tmp[:2], c.dataType)
	binary.LittleEndian.PutUint16(tmp[2:4], c.dataSize)
	binary.LittleEndian.PutUint32(tmp[4:], c.counter)
	bytes = append(bytes, tmp...)

	bytes = append(bytes, c.constraints.bytes()...)
	bytes = append(bytes, c.defaultValue.bytes()...)
	return bytes
}

func (c column) size() int {
	return 3*2 + 4 + maxAttributeNameLength + c.constraints.size() + c.defaultValue.size()
}

func (i Integer) bytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(i))
	return bytes
}

func (i Integer) size() int {
	return IntegerSize
}

func (f Float) bytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(f))
	return bytes
}

func (f Float) size() int {
	return FloatSize
}

func (d Datetime) bytes() (bytes []byte) {
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(d))
	return bytes
}

func (d Datetime) size() int {
	return DatetimeSize
}

func (b Boolean) bytes() []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func (b Boolean) size() int {
	return BooleanSize
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

func buildColumn(definition common.TableColumnDefiner) column {
	c := column{}

}

func newChar(str string, length int) Char {
	if len(str) > length {
		panic("String length is greater than char size")
	}

	b := make([]byte, length)
	copy(b, str)
	return b
}

func newTuple(columns []common.TableColumnDefiner) tuple {
	return nil
}

func newTableTuple(name string, columns address, records address) tuple {
	if len(name) > maxAttributeNameLength {
		panic(fmt.Sprintf("Table name length can't be greater than %d bytes", maxAttributeNameLength))
	}

	return tuple{
		tupleElement{maxAttributeNameLength, false, newChar(name, maxAttributeNameLength)},
		tupleElement{IntegerSize, false, Integer(columns)},
		tupleElement{IntegerSize, false, Integer(records)},
	}
}
