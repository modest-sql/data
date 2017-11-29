package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/modest-sql/common"
)

const (
	//MagicBytes is used to identify a Modest SQL database file.
	MagicBytes uint32 = 0x8709f625
	//MinBlockSize defines the minimum block size a Modest SQL database can have.
	MinBlockSize uint32 = 4096
	//MaxBlockSize defines the maximum block size a Modest SQL database can have.
	MaxBlockSize uint32 = 1048576
	//MetadataAddress defines the address in which the database metadata is located.
	MetadataAddress address = 1
	//MetaTableAddress defines the address in which the database metatable is located.
	MetaTableAddress address = 2
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
			Blocks:     2,
			MetaTable:  MetaTableAddress,
		},
	}

	if err := db.init(); err != nil {
		if err := os.Remove(path); err != nil {
			return nil, err
		}
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

func (i DatabaseInfo) bytes() (bytes []byte) {
	bytes = make([]byte, 16)
	binary.LittleEndian.PutUint32(bytes[:4], i.MagicBytes)
	binary.LittleEndian.PutUint32(bytes[4:8], i.BlockSize)
	binary.LittleEndian.PutUint32(bytes[8:12], i.Blocks)
	binary.LittleEndian.PutUint32(bytes[12:16], i.FreeBlocks)
	bytes = append(bytes, i.FirstFreeBlock.bytes()...)
	bytes = append(bytes, i.LastFreeBlock.bytes()...)
	bytes = append(bytes, i.MetaTable.bytes()...)
	return bytes
}

func (i DatabaseInfo) size() int {
	return 7 * 4
}

func (addr address) bytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(addr))
	return bytes
}

func (addr address) size() int {
	return binary.Size(addr)
}

func (db Database) writeAt(data storable, addr address) error {
	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	dataSize := data.size()
	blockSize := int(db.databaseInfo.BlockSize)

	if dataSize > blockSize {
		return errors.New("Failed to write data because it exceeds block size")
	}

	bytes := append(data.bytes(), make([]byte, blockSize-dataSize)...)
	if _, err := db.file.WriteAt(bytes, addr.fileOffset(blockSize)); err != nil {
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

func (db Database) init() error {
	if err := db.writeAt(db.databaseInfo, MetadataAddress); err != nil {
		return err
	}

	metatables, err := db.newRecordBlock(newTableTuple("", 0, 0))
	if err != nil {
		return err
	}

	return db.writeAt(metatables, db.databaseInfo.MetaTable)
}

func (db Database) NewTable(name string, columns []common.TableColumnDefiner) (err error) {
	columnsBlockAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	recordsBlockAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	tableTuple := newTableTuple(name, columnsBlockAddr, recordsBlockAddr)

	defer func() {
		if err != nil {
			return
		}

		var columnBlock *columnBlock
		var recordBlock *recordBlock

		if columnBlock, err = db.newColumnBlock(columns); err != nil {
			return
		}

		if recordBlock, err = db.newRecordBlock(newTuple(columns)); err != nil {
			return
		}

		if err = db.writeAt(columnBlock, columnsBlockAddr); err != nil {
			return
		}

		err = db.writeAt(recordBlock, recordsBlockAddr)
	}()

	for recordBlockAddr := db.databaseInfo.MetaTable; recordBlockAddr != nullBlock; {
		b, err := db.readAt(recordBlockAddr)
		if err != nil {
			return err
		}

		recordBlock := &recordBlock{}
		fill(recordBlock, b)

		if ok := recordBlock.insert(tableTuple); ok {
			if err := db.writeAt(recordBlock, recordBlockAddr); err != nil {
				return err
			}
			return nil
		}

		recordBlockAddr = recordBlock.NextBlock
	}

	newRecordBlockAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	newRecordBlock, err := db.newRecordBlock(tableTuple)
	if err != nil {
		return nil
	}

	if err := db.writeAt(newRecordBlock, newRecordBlockAddr); err != nil {
		return err
	}

	return nil
}

func (db Database) Insert(tableName string, values map[string]storable) error {
	return errors.New("Not implemented")
}

func fill(data interface{}, b []byte) interface{} {
	buffer := bytes.NewBuffer(b)
	binary.Read(buffer, binary.LittleEndian, data)
	return data
}
