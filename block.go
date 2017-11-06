package data

import (
	"encoding/binary"
)

type blockSignature uint32

const (
	blockSize                                = 4096
	nullBlockNo               Address        = 0
	tableEntryBlockSignature  blockSignature = 0xff77ff77
	tableHeaderBlockSignature blockSignature = 0xee11ee11
	recordBlockSignature      blockSignature = 0xaa88aa88
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

func (db Database) writeBlock(blockNo Address, block block) (err error) {
	_, err = db.file.WriteAt(block[:], blockNo.offset())
	return err
}
