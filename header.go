package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/modest-sql/common"
)

type dataType uint16
type tableColumns [maxTableColumns]tableColumn

const (
	maxColumnNameLength         = 60
	maxTableColumns             = 61
	tableHeaderBlockPaddingSize = 54
)

const (
	integer dataType = iota
	float
	boolean
	datetime
	char
	invalid
)

var dataTypeNames = map[dataType]string{
	integer:  "INTEGER",
	float:    "FLOAT",
	boolean:  "BOOLEAN",
	datetime: "DATETIME",
	char:     "CHAR",
	invalid:  "INVALID",
}

var dataTypeSizes = map[dataType]int{
	integer:  8,
	float:    8,
	boolean:  1,
	datetime: 8,
}

func dataTypeOf(column interface{}) dataType {
	switch column.(type) {
	case common.IntegerTableColumn:
		return integer
	case common.FloatTableColumn:
		return float
	case common.BooleanTableColumn:
		return boolean
	case common.DatetimeTableColumn:
		return datetime
	case common.CharTableColumn:
		return char
	}

	return invalid
}

type tableHeaderBlock struct {
	Signature         blockSignature
	NextHeaderBlock   Address
	FirstRecordBlock  Address
	ColumnCount       uint32
	TableColumnsArray tableColumns
	Padding           [tableHeaderBlockPaddingSize]byte
}

func (h *tableHeaderBlock) AddTableColumn(column tableColumn) {
	h.TableColumnsArray[h.ColumnCount] = column
	h.ColumnCount++
}

func (h tableHeaderBlock) TableColumns() []tableColumn {
	return h.TableColumnsArray[:h.ColumnCount]
}

func (h tableHeaderBlock) Table(tableName string) *Table {
	tableColumns := []TableColumn{}

	for _, c := range h.TableColumns() {
		tableColumns = append(tableColumns, TableColumn{
			ColumnName: c.ColumnName(),
			ColumnType: c.DataType,
			ColumnSize: c.Size,
		})
	}

	return &Table{
		TableName:    tableName,
		TableColumns: tableColumns,
	}
}

type recordReader func(record) interface{}

func (h tableHeaderBlock) recordReaders() (size int, readers map[string]recordReader) {
	readers = map[string]recordReader{}
	size = freeFlagSize + nullBitmapSize

	for _, column := range h.TableColumns() {
		columnName := column.ColumnName()
		offset := size

		if column.DataType == char {
			size += int(column.Size)
		} else {
			size += dataTypeSizes[column.DataType]
		}

		switch column.DataType {
		case datetime:
			fallthrough
		case integer:
			readers[columnName] = func(r record) interface{} {
				return int64(binary.LittleEndian.Uint64(r[offset:size]))
			}
		case float:
			readers[columnName] = func(r record) interface{} {
				return float64(binary.LittleEndian.Uint64(r[offset:size]))
			}
		case boolean:
			readers[columnName] = func(r record) interface{} {
				return r[offset] != 0
			}
		case char:
			readers[columnName] = func(r record) interface{} {
				return string(bytes.TrimRight(r[offset:size], "\x00"))
			}
		}

	}

	return size, readers
}

type tableColumn struct {
	DataType        dataType
	Size            uint16
	ConstraintFlags uint16
	ColumnNameArray [maxColumnNameLength]byte
}

func (c *tableColumn) SetDefaultValue() {
	c.ConstraintFlags |= 1
}

func (c *tableColumn) SetAutoincrementable() {
	c.ConstraintFlags |= (1 << 1)
}

func (c *tableColumn) SetNullable() {
	c.ConstraintFlags |= (1 << 2)
}

func (c *tableColumn) SetPrimaryKey() {
	c.ConstraintFlags |= (1 << 3)
}

func (c *tableColumn) SetForeignKey() {
	c.ConstraintFlags |= (1 << 4)
}

func (c tableColumn) HasDefaultValue() bool {
	return (c.ConstraintFlags & 1) != 0
}

func (c tableColumn) IsAutoincrementable() bool {
	return (c.ConstraintFlags & (1 << 1)) != 0
}

func (c tableColumn) IsNullable() bool {
	return (c.ConstraintFlags & (1 << 2)) != 0
}

func (c tableColumn) IsPrimaryKey() bool {
	return (c.ConstraintFlags & (1 << 3)) != 0
}

func (c tableColumn) IsForeignKey() bool {
	return (c.ConstraintFlags & (1 << 4)) != 0
}

func (c tableColumn) ColumnName() string {
	return string(bytes.TrimRight(c.ColumnNameArray[:], "\x00"))
}

func (c *tableColumn) SetColumnName(columnName string) {
	copy(c.ColumnNameArray[:], strings.ToUpper(columnName))
}

func (c tableColumn) ColumnSize() int {
	if c.DataType != char {
		return dataTypeSizes[c.DataType]
	}
	return int(c.Size)
}

func (db Database) readHeaderBlock(blockAddr Address) (*tableHeaderBlock, error) {
	block, err := db.readBlock(blockAddr)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	tableHeaderBlock := &tableHeaderBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, tableHeaderBlock); err != nil {
		return nil, err
	}

	if tableHeaderBlock.Signature != tableHeaderBlockSignature {
		return nil, fmt.Errorf("Block %d is not a TableHeaderBlock", blockAddr)
	}

	return tableHeaderBlock, nil
}

func (db Database) writeTableHeaderBlock(blockAddr Address, tableHeaderBlock *tableHeaderBlock) error {
	buffer := bytes.NewBuffer(nil)

	tableHeaderBlock.Signature = tableHeaderBlockSignature
	if err := binary.Write(buffer, binary.LittleEndian, tableHeaderBlock); err != nil {
		return err
	}

	block := block{}
	copy(block[:], buffer.Bytes())

	return db.writeBlock(blockAddr, block)
}

func (db Database) findHeaderBlock(tableName string) (*tableHeaderBlock, error) {
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	return db.readHeaderBlock(tableEntry.HeaderBlock)
}
