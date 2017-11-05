package data

import (
	"encoding/binary"
)

type blockSignature uint32

const (
	blockSize                               = 4096
	tableEntryBlockSignature blockSignature = 0xff77ff77
)

type block [blockSize]byte

func (b block) signature() blockSignature {
	return blockSignature(binary.LittleEndian.Uint32(b[:4]))
}

func (db Database) readBlock(blockNo Address) (b block, err error) {
	if _, err = db.file.ReadAt(b[:], blockNo.offset()); err != nil {
		return b, err
	}

	return b, nil
}
