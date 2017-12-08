package data

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type dbInfo struct {
	blockSize            int64
	blocks               int64
	availableBlocks      int64
	availableBlocksFront int64
	availableBlocksBack  int64
}

type database struct {
	dbInfo
	dbTableIDs map[string]dbInteger
	dbTables   []dbTable
	dbFile     *os.File
}

func newDatabase(dbInfo dbInfo, dbTables []dbTable, dbFile *os.File) database {
	dbTableIDs := map[string]dbInteger{}
	for i := range dbTables {
		dbTableIDs[dbTables[i].name()] = dbTables[i].dbTableID
	}

	return database{
		dbInfo:     dbInfo,
		dbTableIDs: dbTableIDs,
		dbTables:   dbTables,
		dbFile:     dbFile,
	}
}

func (db database) name() string {
	filename := filepath.Base(db.dbFile.Name())

	n := strings.LastIndexByte(filename, '.')
	if n >= 0 {
		return filename[:n]
	}

	return filename
}

func (db database) table(name string) (*dbTable, error) {
	dbTableID, ok := db.dbTableIDs[name]
	if !ok {
		return nil, fmt.Errorf("Table `%s' does not exist in database `%s'", name, db.name())
	}

	for i := range db.dbTables {
		if db.dbTables[i].dbTableID == dbTableID {
			return &db.dbTables[i], nil
		}
	}

	return nil, fmt.Errorf("Database `%s' does not contain table with ID %d", db.name(), dbTableID)
}

func (db *database) addTable(dbTable dbTable) error {
	if dbTable, _ := db.table(dbTable.name()); dbTable != nil {
		return fmt.Errorf("Duplicate table `%s' in database `%s'", dbTable.name(), db.name())
	}

	db.dbTableIDs[dbTable.name()] = dbTable.dbTableID
	db.dbTables = append(db.dbTables, dbTable)
	return nil
}

func (db *database) deleteTable(name string) error {
	dbTableID, ok := db.dbTableIDs[name]
	if !ok {
		return fmt.Errorf("Table `%s' does not exist in database `%s'", name, db.name())
	}

	for i := range db.dbTables {
		if db.dbTables[i].dbTableID == dbTableID {
			delete(db.dbTableIDs, name)
			db.dbTables[i] = db.dbTables[len(db.dbTables)-1]
			db.dbTables = db.dbTables[:len(db.dbTables)-1]
			return nil
		}
	}

	return fmt.Errorf("Database `%s' does not contain table with ID %d", db.name(), dbTableID)
}

func (db database) blockOffset(addr int64) (int64, error) {
	if addr <= 0 {
		return 0, errors.New("Address must be greater than 0")
	}

	return db.dbInfo.blockSize * (addr - 1), nil
}

func (db database) writeAt(b []byte, addr int64) error {
	blockPaddingLen := db.dbInfo.blockSize - int64(len(b))
	if blockPaddingLen < 0 {
		return errors.New("Byte slice is greater than block size")
	}

	blockOffset, err := db.blockOffset(addr)
	if err != nil {
		return err
	}

	_, err = db.dbFile.WriteAt(append(b, make([]byte, blockPaddingLen)...), blockOffset)
	return err
}

func (db database) readAt(addr int64) ([]byte, error) {
	blockOffset, err := db.blockOffset(addr)
	if err != nil {
		return nil, err
	}

	b := make([]byte, db.dbInfo.blockSize)
	if _, err := db.dbFile.ReadAt(b, blockOffset); err != nil {
		return nil, err
	}

	return b, nil
}
