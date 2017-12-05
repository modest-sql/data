package data

import (
	"encoding/binary"
	"io"
)

const (
	nullBlock address = 0
)

type signature uint32

type storable interface {
	bytes() []byte
	size() int
}

type block struct {
	Signature signature
	NextBlock address
}

func (s signature) bytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(s))
	return bytes
}

func (s signature) size() int {
	return binary.Size(s)
}

func (b block) bytes() []byte {
	return append(b.Signature.bytes(), b.NextBlock.bytes()...)
}

func (b block) size() (size int) {
	return b.Signature.size() + b.NextBlock.size()
}

func (db Database) readRawBlockAt(addr address) (*block, error) {
	b, err := db.readAt(addr)
	if err != nil {
		return nil, err
	}

	return &block{
		Signature: signature(binary.LittleEndian.Uint32(b[:4])),
		NextBlock: address(binary.LittleEndian.Uint32(b[4:8])),
	}, nil
}

func (db *Database) allocBlock() (newAddr address, err error) {
	if db.databaseInfo.FirstFreeBlock == 0 {
		db.databaseInfo.Blocks++
		newAddr = address(db.databaseInfo.Blocks)

		if _, err := db.file.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}

		if err := binary.Write(db.file, binary.LittleEndian, make([]byte, db.databaseInfo.BlockSize)); err != nil {
			return 0, err
		}
	} else {
		newAddr = db.databaseInfo.FirstFreeBlock

		rawBlock, err := db.readRawBlockAt(db.databaseInfo.FirstFreeBlock)
		if err != nil {
			return 0, err
		}

		if db.databaseInfo.LastFreeBlock == db.databaseInfo.FirstFreeBlock {
			db.databaseInfo.LastFreeBlock = rawBlock.NextBlock
		}

		db.databaseInfo.FirstFreeBlock = rawBlock.NextBlock
	}

	if err := db.writeAt(db.databaseInfo, MetadataAddress); err != nil {
		return 0, err
	}

	return newAddr, nil
}
