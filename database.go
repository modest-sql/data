package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"github.com/modest-sql/common"
)

type dbInfo struct {
	blockSize            int64
	blocks               int64
	availableBlocks      int64
	availableBlocksFront int64
	tables               int64
	columns              int64
	defaultNumerics      int64
	defaultChars         int64
}

type database struct {
	dbInfo
	dbTableIDs  map[string]dbInteger
	dbTables    []dbTable
	dbSysTables []dbTable
	dbFile      *os.File
}

func NewDatabase(path string, blockSize int64) (*database, error) {
	sysBlockSize := systemBlockSize()
	if blockSize <= 0 {
		return nil, errors.New("Block size must be greater than 0")
	}

	if blockSize%sysBlockSize != 0 {
		return nil, fmt.Errorf("Block size must be multiple of disk block size (%d)", sysBlockSize)
	}

	dbFile, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	dbInfo := dbInfo{blockSize: blockSize, blocks: 1}
	db := newDatabase(dbInfo, dbFile)

	if err := db.writeDbInfo(); err != nil {
		return nil, err
	}

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

func LoadDatabase(path string) (*database, error) {
	dbFile, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	db := newDatabase(dbInfo{}, dbFile)

	if err := db.readDbInfo(); err != nil {
		return nil, err
	}

	if err := db.loadTables(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *database) NewTable(name string, columnDefiners []common.TableColumnDefiner) error {
	tableName := make(dbChar, maxNameLength)
	copy(tableName, name)

	firstRecordBlockAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	tableID := dbInteger(db.tables + 1)

	values := map[string]dbType{
		"TABLE_ID":           tableID,
		"FIRST_RECORD_BLOCK": dbInteger(firstRecordBlockAddr),
		"TABLE_NAME":         tableName,
	}

	table := newDBTable(tableID, tableName, []dbColumn{}, dbInteger(firstRecordBlockAddr))

	for pos, definition := range columnDefiners {
		column, err := db.newDBColumn(table, definition, pos)
		if err != nil {
			return err
		}

		if err := table.addColumn(column); err != nil {
			return err
		}
	}

	if err := db.addTable(table); err != nil {
		return err
	}

	db.tables++
	if err := db.writeDbInfo(); err != nil {
		return err
	}

	if err := db.insert(db.sysTables(), values); err != nil {
		return err
	}

	for _, column := range table.dbColumns {
		values := map[string]dbType{
			"COLUMN_ID":             column.dbColumnID,
			"TABLE_ID":              column.dbTableID,
			"COLUMN_POSITION":       column.dbColumnPosition,
			"COLUMN_TYPE":           dbInteger(column.dbTypeID),
			"COLUMN_SIZE":           column.dbTypeSize,
			"COLUMN_COUNTER":        column.dbAutoincrementCounter,
			"COLUMN_CONSTRAINTS":    dbInteger(column.dbConstraints),
			"DEFAULT_CONSTRAINT_ID": column.dbDefaultValueConstraintID,
			"COLUMN_NAME":           column.dbColumnName,
		}

		if err := db.insert(db.sysColumns(), values); err != nil {
			return err
		}
	}

	rb, err := table.newDBRecordBlock(db.blockSize)
	if err != nil {
		return err
	}

	if err := db.writeAt(table.recordBlockBytes(rb), firstRecordBlockAddr); err != nil {
		return err
	}

	return nil
}

func (db *database) Insert(name string, values map[string]interface{}) error {
	table, err := db.table(name)
	if err != nil {
		return err
	}

	dbValues, err := convertValuesMap(*table, values)
	if err != nil {
		return err
	}

	return db.insert(*table, dbValues)
}

func (db *database) insert(table dbTable, values map[string]dbType) error {
	record, err := table.buildDBRecord(values)
	if err != nil {
		return err
	}

	lastAddr := nullBlockAddr
	for addr := int64(table.firstRecordBlockAddr); addr != nullBlockAddr; {
		block, err := db.readAt(addr)
		if err != nil {
			return err
		}

		rb := table.loadRecordBlockBytes(block)
		if rb.insertRecord(record) {
			return db.writeAt(table.recordBlockBytes(rb), addr)
		}

		lastAddr = addr
		addr = block.nextBlock()
	}

	newAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	lastBlock, err := db.readAt(lastAddr)
	if err != nil {
		return err
	}

	lastBlock.putNextBlock(newAddr)
	if err := db.writeAt(lastBlock, lastAddr); err != nil {
		return err
	}

	rb, err := table.newDBRecordBlock(db.blockSize)
	if err != nil {
		return err
	}

	rb.insertRecord(record)
	return db.writeAt(table.recordBlockBytes(rb), newAddr)
}

func (db *database) Delete(name string) error {
	table, err := db.table(name)
	if err != nil {
		return err
	}

	return db.delete(*table)
}

func (db *database) delete(table dbTable) error {
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

func (db *database) Update(name string, values map[string]interface{}) error {
	return errors.New("Update not implemented")
}

func (db *database) Drop(name string) error {
	table, err := db.table(name)
	if err != nil {
		return err
	}

	//TODO: Add where condition
	if err := db.delete(db.sysTables()); err != nil {
		return err
	}

	// TODO: Add where condition
	if err := db.delete(db.sysColumns()); err != nil {
		return err
	}

	// Delete all records block
	for blockAddr := int64(table.firstRecordBlockAddr); blockAddr != nullBlockAddr; {
		// Read block
		block, err := db.readAt(blockAddr)
		if err != nil {
			return err
		}

		// Free the record block
		if err := db.freeBlock(blockAddr); err != nil {
			return err
		}
		// Point to next record block
		blockAddr = block.nextBlock()
	}

	return db.deleteTable(table.name())
}

func (db *database) Select(name string) (*ResultSet, error) {
	return nil, errors.New("Selected not implemented")
}

func newDatabase(dbInfo dbInfo, dbFile *os.File) *database {
	return &database{
		dbInfo:      dbInfo,
		dbTableIDs:  map[string]dbInteger{},
		dbSysTables: newSysTables(),
		dbFile:      dbFile,
	}
}

func (db database) sysTables() dbTable {
	return db.dbSysTables[0]
}

func (db database) sysColumns() dbTable {
	return db.dbSysTables[1]
}

func (db database) sysNumerics() dbTable {
	return db.dbSysTables[2]
}

func (db database) sysChars() dbTable {
	return db.dbSysTables[3]
}

func (db database) name() string {
	filename := filepath.Base(db.dbFile.Name())

	n := strings.LastIndexByte(filename, '.')
	if n >= 0 {
		return filename[:n]
	}

	return filename
}

func (db *database) readDbInfo() error {
	blockSizeB := make([]byte, 8)
	if _, err := db.dbFile.ReadAt(blockSizeB, 0); err != nil {
		return err
	}

	b := make([]byte, binary.LittleEndian.Uint64(blockSizeB))
	if _, err := db.dbFile.ReadAt(b, 0); err != nil {
		return err
	}

	db.dbInfo = dbInfo{
		blockSize:            int64(binary.LittleEndian.Uint64(b[:8])),
		blocks:               int64(binary.LittleEndian.Uint64(b[8:16])),
		availableBlocks:      int64(binary.LittleEndian.Uint64(b[16:24])),
		availableBlocksFront: int64(binary.LittleEndian.Uint64(b[24:32])),
		tables:               int64(binary.LittleEndian.Uint64(b[32:40])),
		columns:              int64(binary.LittleEndian.Uint64(b[40:48])),
		defaultNumerics:      int64(binary.LittleEndian.Uint64(b[48:56])),
		defaultChars:         int64(binary.LittleEndian.Uint64(b[56:64])),
	}

	return nil
}

func (db database) writeDbInfo() error {
	if _, err := db.dbFile.Seek(0, 0); err != nil {
		return err
	}

	if err := binary.Write(db.dbFile, binary.LittleEndian, db.dbInfo); err != nil {
		return err
	}

	if _, err := db.dbFile.Write(make([]byte, int(db.blockSize)-binary.Size(db.dbInfo))); err != nil {
		return err
	}

	return nil
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

func (db *database) loadTables() error {
	tablesSet, err := db.tableSet(db.sysTables())
	if err != nil {
		return err
	}

	columnsSet, err := db.tableSet(db.sysColumns())
	if err != nil {
		return err
	}

	result := joinByAttribute(tablesSet, columnsSet, operatorEquals, "SYS_TABLES.TABLE_ID", "SYS_COLUMNS.TABLE_ID")
	tablesSet, columnsSet = nil, nil

	tablesMap := map[string][]dbColumn{}
	tablesRecordBlocks := map[string]dbInteger{}

	for i := range result {
		tableName := trimName(result[i]["SYS_TABLES.TABLE_NAME"].(dbChar))

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

	result = nil
	for tableName, columns := range tablesMap {
		table := newDBTable(db.dbTableIDs[tableName], dbChar(tableName), []dbColumn{}, tablesRecordBlocks[tableName])
		for i := range columns {
			if err := table.addColumn(columns[i]); err != nil {
				return err
			}
		}
		db.dbTables = append(db.dbTables, table)
	}

	return nil
}

/*
CommandFactory creates instances of common.Command according to command object received
as parameter. Once the command is run, execution is moved to the callback function received as parameter.
*/
func (db *database) CommandFactory(cmd interface{}, cb func(interface{}, error)) (command common.Command) {
	switch cmd := cmd.(type) {
	case *common.CreateTableCommand:
		command = common.NewCommand(
			cmd,
			common.Create,
			func() {
				cb(nil, db.NewTable(cmd.TableName(), cmd.TableColumnDefiners()))
			},
		)
	case *common.InsertCommand:
		command = common.NewCommand(
			cmd,
			common.Insert,
			func() {
				cb(nil, db.Insert(cmd.TableName(), cmd.Values()))
			},
		)
	case *common.UpdateTableCommand:
		command = common.NewCommand(
			cmd,
			common.Update,
			func() {
				cb(nil, db.Update(cmd.TableName(), map[string]interface{}{}))
			},
		)
	case *common.DeleteCommand:
		command = common.NewCommand(
			cmd,
			common.Delete,
			func() {
				cb(nil, db.Delete(cmd.TableName()))
			},
		)
	case *common.DropCommand:
		command = common.NewCommand(
			cmd,
			common.Drop,
			func() {
				cb(nil, db.Drop(cmd.TableName()))
			},
		)
	case *common.SelectTableCommand:
		command = common.NewCommand(
			cmd,
			common.Select,
			func() {
				cb(db.Select(cmd.TableName()))
			},
		)
	default:
		cb(nil, fmt.Errorf("Unrecognized command type %v", reflect.TypeOf(cmd)))
	}

	return command
}

func systemBlockSize() int64 {
	var stat syscall.Stat_t
	syscall.Stat(os.DevNull, &stat)
	return stat.Blksize
}

func splitIdentifier(identifier string) (tableName string, columnName string) {
	names := strings.Split(identifier, ".")
	if len(names) != 2 {
		return "", names[0]
	}
	return names[0], names[1]
}

func qualifiedIdentifier(table dbTable, identifier string) string {
	t, c := splitIdentifier(identifier)
	if t == "" {
		return concatTable(table.name(), c)
	}
	return identifier
}

func concatTable(tableName string, columnName string) string {
	return fmt.Sprintf("%s.%s", tableName, columnName)
}

func trimName(name []byte) string {
	return string(bytes.TrimRight(name, "\x00"))
}
