package data

import (
	"bytes"
	"errors"
)

type dataType uint16
type tableColumns [maxTableColumns]tableColumn

const (
	maxColumnNameLength         = 60
	maxTableColumns             = 63
	tableHeaderBlockPaddingSize = 51
)

const (
	integer dataType = iota
	float
	boolean
	char
	datetime
)

type tableHeaderBlock struct {
	Signature        blockSignature
	NextHeaderBlock  Address
	FirstRecordBlock Address
	TableColumns     tableColumns
	Padding          [tableHeaderBlockPaddingSize]byte
	_                byte
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

func (e tableEntry) header() (*tableHeaderBlock, error) {
	return nil, errors.New("Not implemented")
}
