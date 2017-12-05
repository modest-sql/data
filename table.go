package data

import (
	"encoding/binary"
	"fmt"

	"github.com/modest-sql/common"
)

const (
	maxAttributeNameLength           = 60
	maxCharLength                    = 2000
	constraintsCount                 = 5
	columnBlockSignature   signature = 0xdbfe9f24
)

const (
	IntegerSize  = 8
	FloatSize    = 8
	DatetimeSize = 8
	BooleanSize  = 1
)

const (
	integerType uint16 = iota
	floatType
	datetimeType
	booleanType
	charType
)

const (
	defaultFlag uint = iota
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

var dataTypeSizes = map[uint16]int{
	integerType:  IntegerSize,
	floatType:    FloatSize,
	datetimeType: DatetimeSize,
	booleanType:  BooleanSize,
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
	return 3*2 + 4 + maxAttributeNameLength + c.constraints.size() + int(c.dataSize)
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

func dataTypeInfo(cd common.TableColumnDefiner) (dataType uint16, dataSize uint16, defaultValue storable) {
	definerDefaultValue := cd.DefaultValue()
	hasDefaultValue := definerDefaultValue != nil

	switch v := cd.(type) {
	case common.IntegerTableColumn:
		if hasDefaultValue {
			defaultValue = Integer(definerDefaultValue.(int64))
		} else {
			defaultValue = Integer(0)
		}
		return integerType, IntegerSize, defaultValue
	case common.FloatTableColumn:
		if hasDefaultValue {
			defaultValue = Float(definerDefaultValue.(int64))
		} else {
			defaultValue = Float(0)
		}
		return floatType, FloatSize, defaultValue
	case common.DatetimeTableColumn:
		if hasDefaultValue {
			defaultValue = Datetime(definerDefaultValue.(int64))
		} else {
			defaultValue = Datetime(0)
		}
		return datetimeType, DatetimeSize, defaultValue
	case common.BooleanTableColumn:
		if hasDefaultValue {
			defaultValue = Boolean(definerDefaultValue.(bool))
		} else {
			defaultValue = Boolean(false)
		}
		return booleanType, BooleanSize, defaultValue
	case common.CharTableColumn:
		if hasDefaultValue {
			defaultValue = newChar(definerDefaultValue.(string), int(v.Size()))
		} else {
			defaultValue = newChar("", int(v.Size()))
		}
		return charType, uint16(v.Size()), defaultValue
	}

	return dataType, dataSize, defaultValue
}

func newChar(str string, length int) Char {
	if len(str) > length {
		panic("String length is greater than char size")
	}

	if length == 0 {
		panic("Char length must be greater than 0")
	} else if length > maxCharLength {
		panic(fmt.Sprintf("Char length can't be greater than %d bytes", maxCharLength))
	}

	b := make([]byte, length)
	copy(b, str)
	return b
}

func buildColumn(definition common.TableColumnDefiner) column {
	if len(definition.ColumnName()) == 0 {
		panic("Column name can't be empty")
	} else if len(definition.ColumnName()) > maxAttributeNameLength {
		panic(fmt.Sprintf("Column name length can't be greater than %d bytes", maxAttributeNameLength))
	}

	c := column{constraints: newBitmap(constraintsCount)}
	c.dataType, c.dataSize, c.defaultValue = dataTypeInfo(definition)
	copy(c.name[:], definition.ColumnName())

	if definition.Autoincrementable() {
		c.constraints.Set(autoincrementFlag)
	}

	if definition.DefaultValue() != nil {
		c.constraints.Set(defaultFlag)
	}

	if definition.ForeignKey() {
		c.constraints.Set(foreignKeyFlag)
	}

	if definition.Nullable() {
		c.constraints.Set(nullableFlag)
	}

	if definition.PrimaryKey() {
		c.constraints.Set(primaryKeyFlag)
	}

	c.columnSize = uint16(c.size())

	return c
}

func (db Database) newColumnBlock(columns []column) *columnBlock {
	return &columnBlock{
		block: block{
			Signature: columnBlockSignature,
		},
		columns: columns,
	}
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
