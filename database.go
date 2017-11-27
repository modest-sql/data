package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	//MagicBytes is used to identify a Modest SQL database file.
	MagicBytes uint32 = 0x8709f625
	//MinBlockSize defines the minimum block size a Modest SQL database can have.
	MinBlockSize uint32 = 512
	//MaxBlockSize defines the maximum block size a Modest SQL database can have.
	MaxBlockSize uint32 = 1048576
	//MetadataAddress defines the address in which the database metadata is located.
	MetadataAddress = 1
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

var databasesMutex sync.Mutex
var databases sync.Map

/*NewDatabase creates a new database file specified by the path received as parameter.
The file will be initialized with the binary structure of a Modest SQL database.
*/
func NewDatabase(path string, blockSize uint32) (db *Database, err error) {
	databasesMutex.Lock()
	defer databasesMutex.Unlock()

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

	if err := db.writeAt(db.databaseInfo, MetadataAddress); err != nil {
		return nil, err
	}

	databases.Store(file.Name(), db)

	return db, nil
}

//LoadDatabase opens a valid Modest SQL database file.
func LoadDatabase(path string) (db *Database, err error) {
	databasesMutex.Lock()
	defer databasesMutex.Unlock()

	if val, ok := databases.Load(filepath.Base(path)); ok {
		db = val.(*Database)

		db.referencesMutex.Lock()
		defer db.referencesMutex.Unlock()

		db.references++

		return db, nil
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	db = &Database{
		references:      1,
		referencesMutex: &sync.Mutex{},
		rwMutex:         &sync.RWMutex{},
		file:            file,
	}

	b, err := db.readAt(MetadataAddress)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewBuffer(b)
	if err := binary.Read(reader, binary.LittleEndian, &db.databaseInfo); err != nil {
		return nil, err
	}

	if db.databaseInfo.MagicBytes != MagicBytes {
		return nil, errors.New("Invalid database file")
	}

	databases.Store(file.Name(), db)

	return db, nil
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
	if addr <= 0 {
		panic("Addresses must be greater than 0")
	}

	return (int64(addr) - 1) * int64(blockSize)
}

func (db Database) writeAt(data interface{}, addr address) error {
	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	dataSize := binary.Size(data)
	blockSize := int(db.databaseInfo.BlockSize)

	if dataSize > blockSize {
		return errors.New("Failed to write data because it exceeds block size")
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

func (db Database) readAt(addr address) (b []byte, err error) {
	db.rwMutex.RLock()
	defer db.rwMutex.RUnlock()

	blockSize := int(db.databaseInfo.BlockSize)

	b = make([]byte, blockSize)
	if _, err := db.file.ReadAt(b, addr.fileOffset(blockSize)); err != nil {
		return nil, err
	}

	return b, nil
}
