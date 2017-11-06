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
