package data

import (
	"errors"
	"database"
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
	mdata := database.readMetadata()	

	if err != nil{
		return nil, err
	}

	nblockAddr := mdata.LastFreeBlock.nullBlockAddr
	newBlock := new(rawBlock)
	newBlock.blockSignature = mdata.LastFreeBlock.Signature + 1
	newBlock.NextBlock = 0
	newBlock.nullBlockAddr = nblockAddr + 1

	if (mdata.FirstFreeBlock == nil) || (mdata.FirstFreeBlock == 0){
		mdata.FirstFreeBlock = newBlock.nullBlockAddr
	}

	lastBlock := mdata.LastFreeBlock
	lastBlock.NextBlock = newBlock.nullBlockAddr
	mdata.LastFreeBlock = newBlock.nullBlockAddr
	mdata.BlockCount = mdata.BlockCount + 1

	if err := database.writeMetadata() {
		return nil, err
	}

	return newBlock.nullBlockAddr
}

func (db *Database) freeBlock(blockAddr Address) error {
	return errors.New("freeBlock not implemented")
}
