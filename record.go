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

func (r recordBlock) hasFreeSpace() bool {
	return binary.LittleEndian.Uint32(r[:4]) == fullFlag
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

func (rb *recordBlock) insertRecord(values tableValues) bool {
	if rb.FullFlag == fullFlag {

	}

	return false
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

	recordSize, _ := tableHeaderBlock.recordReaders()

	for recordBlockAddr := tableHeaderBlock.FirstRecordBlock; recordBlockAddr != nullBlockAddr; {
		recordBlock, err := db.readRecordBlock(recordBlockAddr)
		if err != nil {
			return err
		}

		for _, record := range recordBlock.Data.split(recordSize) {
			if !record.isFree() {
				continue
			}
			recordBlock.insertRecord(values)
		}
		recordBlockAddr = recordBlock.NextRecordBlock
	}
	return nil
}
