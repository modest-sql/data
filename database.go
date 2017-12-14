package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type dbInfo struct {
	blockSize            int64
	blocks               int64
	availableBlocks      int64
	availableBlocksFront int64
}

type database struct {
	dbInfo
	dbTableIDs  map[string]dbInteger
	dbTables    []dbTable
	dbSysTables []dbTable
	dbFile      *os.File
}

func NewDatabase(name string, blockSize int64) (*database, error) {
	sysBlockSize := systemBlockSize()
	if blockSize <= 0 {
		return nil, errors.New("Block size must be greater than 0")
	}

	if blockSize%sysBlockSize != 0 {
		return nil, fmt.Errorf("Block size must be multiple of disk block size (%d)", sysBlockSize)
	}

	dbFile, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	dbInfo := dbInfo{blockSize: blockSize, blocks: 1}

	if err := binary.Write(dbFile, binary.LittleEndian, dbInfo); err != nil {
		return nil, err
	}

	if _, err := dbFile.Write(make([]byte, int(blockSize)-binary.Size(dbInfo))); err != nil {
		return nil, err
	}

	db := newDatabase(dbInfo, dbFile)

	for _, sysTable := range db.dbSysTables {
		sysTableAddr, err := db.allocBlock()
		if err != nil {
			return nil, err
		}

		sysTableRecordBlock, err := sysTable.newDBRecordBlock(db.blockSize)
		if err != nil {
			return nil, err
		}

		if err := db.writeAt(sysTable.recordBlockBytes(sysTableRecordBlock), sysTableAddr); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func newDatabase(dbInfo dbInfo, dbFile *os.File) *database {
	return &database{
		dbInfo:      dbInfo,
		dbTableIDs:  map[string]dbInteger{},
		dbSysTables: newSysTables(),
		dbFile:      dbFile,
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

func (db database) readAt(addr int64) (dbBlock, error) {
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

func (db *database) allocBlock() (int64, error) {
	if db.dbInfo.availableBlocks == 0 {
		addr := db.dbInfo.blocks + 1

		if err := db.writeAt([]byte{}, addr); err != nil {
			return 0, err
		}

		db.dbInfo.blocks++
		return addr, nil
	}

	addr := db.dbInfo.availableBlocksFront
	block, err := db.readAt(addr)
	if err != nil {
		return 0, err
	}

	db.dbInfo.availableBlocksFront = block.nextBlock()
	db.dbInfo.availableBlocks--
	return addr, nil
}

func (db *database) freeBlock(addr int64) error {
	block, err := db.readAt(addr)
	if err != nil {
		return err
	}

	block.putNextBlock(db.dbInfo.availableBlocksFront)
	if err := db.writeAt(block, addr); err != nil {
		return err
	}

	db.dbInfo.availableBlocksFront = addr
	db.dbInfo.availableBlocks++
	return nil
}

func (db database) tableSet(table dbTable) (set dbSet, err error) {
	for i := int64(table.firstRecordBlockAddr); i != nullBlockAddr; {
		block, err := db.readAt(i)
		if err != nil {
			return nil, err
		}

		recordBlock := table.loadRecordBlockBytes(block)
		records := recordBlock.dbRecords
		for i := range records {
			if !records[i].isFree() {
				set = append(set, records[i].dbTuple)
			}
		}

		i = block.nextBlock()
	}

	return set, nil
}

func (db database) Delete(table dbTable) error {
	for blockAddr := int64(table.firstRecordBlockAddr); blockAddr != nullBlockAddr; {
		block, err := db.readAt(blockAddr)
		if err != nil {
			return err
		}
		recordBlock := table.loadRecordBlockBytes(block)

		// Free all records
		for index := range recordBlock.dbRecords {
			// Set freeFlag on tuple
			recordBlock.dbRecords[index].freeFlag = freeFlag
		}
		// Serialize record block
		freeBlock := table.recordBlockBytes(recordBlock)

		// Write modified block
		if err := db.writeAt(freeBlock, blockAddr); err != nil {
			return err
		}
		blockAddr = recordBlock.nextRecordBlock
	}
	return nil
}

func (db database) loadTables() error {
	tablesSet, err := db.tableSet(db.dbSysTables[0])
	if err != nil {
		return err
	}

	columnsSet, err := db.tableSet(db.dbSysTables[1])
	if err != nil {
		return err
	}

	result := db.joinByAttribute(tablesSet, columnsSet, operatorEquals, "SYS_TABLES.TABLE_ID", "SYS_COLUMNS.TABLE_ID")

	tablesMap := map[string][]dbColumn{}
	tablesRecordBlocks := map[string]dbInteger{}

	for i := range result {
		tableName := string(result[i]["SYS_TABLES.TABLE_NAME"].(dbChar))

		column := dbColumn{
			dbColumnID:                 result[i]["SYS_COLUMNS.COLUMN_ID"].(dbInteger),
			dbTableID:                  result[i]["SYS_COLUMNS.TABLE_ID"].(dbInteger),
			dbColumnName:               result[i]["SYS_COLUMNS.COLUMN_NAME"].(dbChar),
			dbColumnPosition:           result[i]["SYS_COLUMNS.COLUMN_POSITION"].(dbInteger),
			dbTypeID:                   dbTypeID(result[i]["SYS_COLUMNS.COLUMN_TYPE"].(dbInteger)),
			dbTypeSize:                 result[i]["SYS_COLUMNS.COLUMN_SIZE"].(dbInteger),
			dbAutoincrementCounter:     result[i]["SYS_COLUMNS.COLUMN_COUNTER"].(dbInteger),
			dbConstraints:              dbConstraintType(result[i]["SYS_COLUMNS.COLUMN_CONSTRAINTS"].(dbInteger)),
			dbDefaultValueConstraintID: result[i]["SYS_COLUMNS.DEFAULT_CONSTRAINT_ID"].(dbInteger),
		}

		if _, ok := tablesMap[tableName]; !ok {
			tablesMap[tableName] = []dbColumn{}
			tablesRecordBlocks[tableName] = result[i]["SYS_TABLES.FIRST_RECORD_BLOCK"].(dbInteger)
			db.dbTableIDs[tableName] = result[i]["SYS_TABLES.TABLE_ID"].(dbInteger)
		}

		tablesMap[tableName] = append(tablesMap[tableName], column)
	}

	for tableName, columns := range tablesMap {
		table := newDBTable(db.dbTableIDs[tableName], dbChar(tableName), columns, tablesRecordBlocks[tableName])
		db.dbTables = append(db.dbTables, table)
	}

	return nil
}

func systemBlockSize() int64 {
	var stat syscall.Stat_t
	syscall.Stat(os.DevNull, &stat)
	return stat.Blksize
}
