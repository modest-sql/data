package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type tableValues map[string]interface{}

const (
	maxRecordDataLength = 4084
	freeFlagSize        = 4
	freeFlag            = 0x99887766
	fullFlag            = 0x10ccff01
)

type recordData [maxRecordDataLength]byte

func (rd recordData) split(recordSize int) (records []record) {
	recordsPerBlock := maxRecordDataLength / recordSize

	for i := 0; i < recordsPerBlock; i++ {
		startOffset := recordSize * i
		endOffset := startOffset + recordSize
		records = append(records, rd[startOffset:endOffset])
	}

	return records
}

type record []byte

func (r record) isFree() bool {
	return binary.LittleEndian.Uint32(r[:4]) == freeFlag
}

type recordBlock struct {
	Signature       blockSignature
	NextRecordBlock Address
	FullFlag        uint32
	Data            recordData
}

func (db Database) readRecordBlock(blockAddr Address) (*recordBlock, error) {
	block, err := db.readBlock(blockAddr)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	recordBlock := &recordBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, recordBlock); err != nil {
		return nil, err
	}

	if recordBlock.Signature != recordBlockSignature {

		return nil, fmt.Errorf("Block %d is not a RecordBlock", blockAddr)
	}

	return recordBlock, nil
}

func (db Database) writeRecordBlock(blockAddr Address, recordBlock *recordBlock) error {
	buffer := bytes.NewBuffer(nil)

	recordBlock.Signature = recordBlockSignature
	if err := binary.Write(buffer, binary.LittleEndian, recordBlock); err != nil {
		return err
	}

	block := block{}
	copy(block[:], buffer.Bytes())

	return db.writeBlock(blockAddr, block)
}

func (db Database) updateRecords(tableName string, values map[string]interface{}) error {
	return errors.New("updateRecords not implemented")
}

func (db Database) deleteRecords(tableName string) (int, error) {
	return 0, errors.New("deleteRecords not implemented")
}

func (v tableValues) record(columns []tableColumn) (record record) {
	record = append(record, make([]byte, 4)...)

	for _, column := range columns {
		value := v[column.ColumnName()]

		switch column.DataType {
		case integer:
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(value.(uint32)))
			record = append(record, buffer...)
		case float:
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(value.(float32)))
			record = append(record, buffer...)
		case boolean:
			record = append(record, value.(byte))
		case datetime:
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, value.(uint32))
			record = append(record, buffer...)
		case char:
			str := make([]byte, column.Size)
			copy(str, value.(string))
			record = append(record, str...)
		}
	}

	return record
}

func (rb *recordBlock) insertRecord(tableColumns []tableColumn, values tableValues) bool {
	if rb.FullFlag == fullFlag {
		return false
	}

	newRecord := values.record(tableColumns)
	recordSize := len(newRecord)

	for i, record := range rb.Data.split(recordSize) {
		if !record.isFree() {
			continue
		}

		startOffset := i * recordSize
		endOffset := startOffset + recordSize

		copy(rb.Data[startOffset:endOffset], newRecord)
		break
	}

	return true
}

func (db *Database) Insert(tableName string, values tableValues) error {
	return errors.New("Insert not implemented")
}
