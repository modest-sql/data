package data

import (
	"errors"
)

type blockSignature uint32

const (
	blockSize                                = 4096
	rawBlockPadding                          = blockSize - 4*2
	nullBlockAddr             Address        = 0
	tableEntryBlockSignature  blockSignature = 0xff77ff77
	tableHeaderBlockSignature blockSignature = 0xee11ee11
	recordBlockSignature      blockSignature = 0xaa88aa88
)

type block [blockSize]byte

type rawBlock struct {
	Signature blockSignature
	NextBlock Address
	Padding   [rawBlockPadding]byte
}

func (db Database) readBlock(blockAddr Address) (b block, err error) {
	if _, err = db.file.ReadAt(b[:], blockAddr.offset()); err != nil {
		return b, err
	}
	return b, nil
}

func (db Database) writeBlock(blockAddr Address, block block) (err error) {
	_, err = db.file.WriteAt(block[:], blockAddr.offset())
	return err
}

func (db *Database) allocBlock() (Address, error) {
	return 0, errors.New("allocBlock not implemented")
}

func (db *Database) freeBlock(blockAddr Address) error {
	return errors.New("freeBlock not implemented")
}
