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
	nullBitmapSize      = 8
	maxCharLength       = maxRecordDataLength - freeFlagSize - nullBitmapSize
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

func (r *record) setFree() {
	binary.LittleEndian.PutUint32((*r)[:4], freeFlag)
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
	record = append(record, make([]byte, freeFlagSize+nullBitmapSize)...)

	for _, column := range columns {
		value := v[column.ColumnName()]

		switch column.DataType {
		case datetime:
			fallthrough
		case integer:
			buffer := make([]byte, dataTypeSizes[column.DataType])
			binary.LittleEndian.PutUint64(buffer, uint64(value.(int64)))
			record = append(record, buffer...)
		case float:
			buffer := make([]byte, dataTypeSizes[column.DataType])
			binary.LittleEndian.PutUint64(buffer, uint64(value.(float64)))
			record = append(record, buffer...)
		case boolean:
			record = append(record, value.(byte))
		case char:
			str := make([]byte, column.Size)
			copy(str, value.(string))
			record = append(record, str...)
		}
	}

	return record
}

func (rb *recordBlock) init(recordSize int) {
	records := rb.Data.split(recordSize)

	for i, record := range records {
		record.setFree()

		startOffset := i * recordSize
		endOffset := startOffset + recordSize

		copy(rb.Data[startOffset:endOffset], record)
	}
}

func (rb *recordBlock) insertRecord(newRecord record) bool {
	if rb.FullFlag == fullFlag {
		return false
	}

	recordSize := len(newRecord)
	recordsPerBlock := maxRecordDataLength / recordSize

	for i, record := range rb.Data.split(recordSize) {
		if !record.isFree() {
			continue
		}

		startOffset := i * recordSize
		endOffset := startOffset + recordSize

		copy(rb.Data[startOffset:endOffset], newRecord)

		if i == recordsPerBlock-1 {
			rb.FullFlag = fullFlag
		}

		return true
	}

	return false
}

func (db *Database) checkAutoincrement(tableEntry *tableEntry, tableHeaderBlock *tableHeaderBlock, values tableValues) error {
	return nil
}

func (db *Database) checkDefaultValue(tableEntry *tableEntry, tableHeaderBlock *tableHeaderBlock, values tableValues) error {
	return nil
}

func (db *Database) checkNullable(tableEntry *tableEntry, tableHeaderBlock *tableHeaderBlock, values tableValues) error {
	return nil
}

func (db *Database) checkPrimaryKey(tableEntry *tableEntry, tableHeaderBlock *tableHeaderBlock, values tableValues) error {
	return nil
}

func (db *Database) checkForeignKey(tableEntry *tableEntry, tableHeaderBlock *tableHeaderBlock, values tableValues) error {
	return nil
}

func (db *Database) checkConstraints(tableEntry *tableEntry, tableHeaderBlock *tableHeaderBlock, values tableValues) error {
	if err := db.checkAutoincrement(tableEntry, tableHeaderBlock, values); err != nil {
		return err
	}

	if err := db.checkDefaultValue(tableEntry, tableHeaderBlock, values); err != nil {
		return err
	}

	if err := db.checkNullable(tableEntry, tableHeaderBlock, values); err != nil {
		return err
	}

	if err := db.checkPrimaryKey(tableEntry, tableHeaderBlock, values); err != nil {
		return err
	}

	return db.checkForeignKey(tableEntry, tableHeaderBlock, values)
}

func (db *Database) Insert(tableName string, values tableValues) error {
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return err
	}

	tableHeaderBlock, err := db.readHeaderBlock(tableEntry.HeaderBlock)
	if err != nil {
		return err
	}

	record := values.record(tableHeaderBlock.TableColumns())

	if err := db.checkConstraints(tableEntry, tableHeaderBlock, values); err != nil {
		return err
	}

	var lastRecordBlockAddr Address
	var lastRecordBlock *recordBlock

	for recordBlockAddr := tableHeaderBlock.FirstRecordBlock; recordBlockAddr != nullBlockAddr; {
		recordBlock, err := db.readRecordBlock(recordBlockAddr)
		if err != nil {
			return err
		}

		if recordBlock.insertRecord(record) {
			return db.writeRecordBlock(recordBlockAddr, recordBlock)
		}

		lastRecordBlock, lastRecordBlockAddr = recordBlock, recordBlockAddr
		recordBlockAddr = recordBlock.NextRecordBlock
	}

	newRecordBlockAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	if tableHeaderBlock.FirstRecordBlock == nullBlockAddr {
		tableHeaderBlock.FirstRecordBlock = newRecordBlockAddr

		if err := db.writeTableHeaderBlock(tableEntry.HeaderBlock, tableHeaderBlock); err != nil {
			return err
		}
	} else {
		lastRecordBlock.NextRecordBlock = newRecordBlockAddr

		if err := db.writeRecordBlock(lastRecordBlockAddr, lastRecordBlock); err != nil {
			return err
		}
	}

	newRecordBlock := &recordBlock{}
	newRecordBlock.init(len(record))
	newRecordBlock.insertRecord(record)

	return db.writeRecordBlock(newRecordBlockAddr, newRecordBlock)
}
