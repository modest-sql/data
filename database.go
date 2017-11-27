package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

const (
	//MagicBytes is used to identify a Modest SQL database file.
	MagicBytes uint32 = 0x8709f625
	//MinBlockSize defines the minimum block size a Modest SQL database can have.
	MinBlockSize uint32 = 512
	//MaxBlockSize defines the maximum block size a Modest SQL database can have.
	MaxBlockSize uint32 = 1048576
)

type address uint32

//Database represents a Modest SQL database file.
type Database struct {
	references      uint32
	referencesMutex *sync.Mutex
	rwMutex         *sync.RWMutex
	file            *os.File
	databaseInfo    DatabaseInfo
}

//DatabaseInfo is used to store metadata information about the database.
type DatabaseInfo struct {
	MagicBytes     uint32
	BlockSize      uint32
	Blocks         uint32
	FreeBlocks     uint32
	FirstFreeBlock address
	LastFreeBlock  address
	MetaTable      address
}

var databases sync.Map

/*NewDatabase creates a new database file specified by the path received as parameter.
The file will be initialized with the binary structure of a Modest SQL database.
*/
func NewDatabase(path string, blockSize uint32) (db *Database, err error) {
	if blockSize < MinBlockSize {
		return nil, fmt.Errorf("Block size must be at least %d bytes", MinBlockSize)
	} else if blockSize > MaxBlockSize {
		return nil, fmt.Errorf("Block size can't be greater than %d bytes", MaxBlockSize)
	}

	if !((blockSize != 0) && ((blockSize & (blockSize - 1)) == 0)) {
		return nil, errors.New("Block size must be power of 2")
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	db = &Database{
		references:      1,
		referencesMutex: &sync.Mutex{},
		rwMutex:         &sync.RWMutex{},
		file:            file,
		databaseInfo: DatabaseInfo{
			MagicBytes: MagicBytes,
			BlockSize:  blockSize,
			Blocks:     1,
		},
	}

	if err := db.writeAt(0, db.databaseInfo); err != nil {
		return nil, err
	}

	databases.Store(file.Name(), db)

	return db, nil
}

//LoadDatabase opens a valid Modest SQL database file.
func LoadDatabase(path string) (*Database, error) {
	return nil, errors.New("LoadDatabase not implemented")
}

//Close frees the database instance from memory if there are no more references to it.
func (db Database) Close() {
	db.referencesMutex.Lock()
	defer db.referencesMutex.Unlock()

	db.references--
	if db.references == 0 {
		databases.Delete(db.file.Name())
	}
}

//DatabaseInfo returns the metadata information about the database.
func (db Database) DatabaseInfo() DatabaseInfo {
	return db.databaseInfo
}

func (addr address) fileOffset(blockSize int) int64 {
	return int64(addr) * int64(blockSize)
}

func (db Database) writeAt(addr address, data interface{}) error {
	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	dataSize := binary.Size(data)
	blockSize := int(db.databaseInfo.BlockSize)

	if dataSize > blockSize {
		return errors.New("Data exceeds block size")
	}

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.LittleEndian, data); err != nil {
		return err
	}
	buffer.Write(make([]byte, blockSize-dataSize))

	if _, err := db.file.WriteAt(buffer.Bytes(), addr.fileOffset(blockSize)); err != nil {
		return err
	}

	return nil
}
