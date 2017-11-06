package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type dataType uint16
type tableColumns [maxTableColumns]tableColumn

const (
	maxColumnNameLength         = 60
	maxTableColumns             = 63
	tableHeaderBlockPaddingSize = 48
)

const (
	integer dataType = iota
	float
	boolean
	char
	datetime
)

var dataTypeSizes = map[dataType]int{
	integer:  4,
	float:    4,
	boolean:  1,
	datetime: 4,
}

type tableHeaderBlock struct {
	Signature         blockSignature
	NextHeaderBlock   Address
	FirstRecordBlock  Address
	ColumnCount       uint32
	TableColumnsArray tableColumns
	Padding           [tableHeaderBlockPaddingSize]byte
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
	size = freeFlagSize

	for _, column := range h.TableColumns() {
		columnName := column.ColumnName()
		offset := size

		if column.DataType == char {
			size += int(column.Size)
		} else {
			size += dataTypeSizes[column.DataType]
		}

		switch column.DataType {
		case integer:
			readers[columnName] = func(r record) interface{} {
				return int32(binary.LittleEndian.Uint32(r[offset : offset+4]))
			}
		case float:
			readers[columnName] = func(r record) interface{} {
				return float32(binary.LittleEndian.Uint32(r[offset : offset+4]))
			}
		case datetime:
			readers[columnName] = func(r record) interface{} {
				return int32(binary.LittleEndian.Uint32(r[offset : offset+4]))
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
	ColumnNameArray [maxColumnNameLength]byte
}

func (c tableColumn) ColumnName() string {
	return string(bytes.TrimRight(c.ColumnNameArray[:], "\x00"))
}

func (c tableColumn) SetColumnName(columnName string) {
	copy(c.ColumnNameArray[:], columnName)
}

func (db Database) readHeaderBlock(blockNo Address) (*tableHeaderBlock, error) {
	block, err := db.readBlock(blockNo)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	tableHeaderBlock := &tableHeaderBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, tableHeaderBlock); err != nil {
		return nil, err
	}

	if tableHeaderBlock.Signature != tableHeaderBlockSignature {
		return nil, fmt.Errorf("Block %d is not a TableHeaderBlock", blockNo)
	}

	return tableHeaderBlock, nil
}

func (db Database) findHeaderBlock(tableName string) (*tableHeaderBlock, error) {
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	return db.readHeaderBlock(tableEntry.HeaderBlock)
}
