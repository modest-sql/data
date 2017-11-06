package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const maxRecordDataLength = 4088

type recordData [maxRecordDataLength]byte

type recordBlock struct {
	Signature       blockSignature
	NextRecordBlock Address
	Data            recordData
}

func (db Database) readRecordBlock(blockNo Address) (*recordBlock, error) {
	block, err := db.readBlock(blockNo)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	recordBlock := &recordBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, recordBlock); err != nil {
		return nil, err
	}

	if recordBlock.Signature != recordBlockSignature {

		return nil, fmt.Errorf("Block %d is not a RecordBlock", blockNo)
	}

	return recordBlock, nil
}
