package data

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

const (metadataBlockSize = 128
	   blockSize 		 = 4096
)

type Database struct {
	file *os.File
	DatabaseMetadata
}

type DatabaseMetadata struct {
	FirstEntryBlock uint32
	LastEntryBlock  uint32
	BlocksQuantity	uint32
	FirstFreeBlock  uint32
	LastFreeBlock   uint32
	Padding         [metadataBlockSize - 4*metadataFields]byte
}

type TableEntry struct{
	TableName 	[60] byte 
	HeaderBlock uint32	 
}

type BlockHeader struct{		
	NextEntryBlock	uint32
	BlockId 		[8]byte 
	ListTableEntry	[]TableEntry 
	Padding			[blockSize - 4*metadataBlockSize]byte
}

type Block struct {
	Metadata BlockHeader
	Size 	 [blockSize]byte	
} 

func (db *Database) newBlock() (int64, error) {
	bk := new(Block)	
	bk.NextEntryBlock = 1
	bk.BlockId, error = BlocksQuantity + 1
	bk.ListTableEntry := new(TableEntry)

	if error != nil {
		if FirstFreeBlock == 0 {
			FirstFreeBlock = bk.BlockId
			LastFreeBlock = bk.BlockId
		} else{
			LastFreeBlock = bk.BlockId
		}
		return bk.BlockId, error
	}
	return nil, error
}