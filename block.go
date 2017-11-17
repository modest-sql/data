package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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

func (db Database) readRawBlock(blockAddr Address) (*rawBlock, error) {
	block, err := db.readBlock(blockAddr)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	rawBlock := &rawBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, rawBlock); err != nil {
		return nil, err
	}

	return rawBlock, nil
}

func (db *Database) allocBlock() (newAddr Address, err error) {
	if db.FirstFreeBlock == 0 {
		db.BlockCount++
		newAddr = Address(db.BlockCount)

		if _, err := db.file.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}

		if err := binary.Write(db.file, binary.LittleEndian, block{}); err != nil {
			return 0, err
		}
	} else {
		newAddr = db.FirstFreeBlock

		rawBlock, err := db.readRawBlock(db.FirstFreeBlock)
		if err != nil {
			return 0, err
		}

		if db.LastFreeBlock == db.FirstFreeBlock {
			db.LastFreeBlock = rawBlock.NextBlock
		}

		db.FirstFreeBlock = rawBlock.NextBlock
	}

	if err := db.writeMetadata(); err != nil {
		return 0, err
	}

	return newAddr, nil
}

func (db *Database) freeBlock(blockAddr Address) error {
	return errors.New("freeBlock not implemented")
}
