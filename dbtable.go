package data

import "fmt"

type dbTable struct {
	dbTableID                     dbInteger
	dbTableName                   dbChar
	dbColumnIDs                   map[string]dbInteger
	dbColumns                     []dbColumn
	firstRecordBlockAddr          dbInteger
	firstAvailableRecordBlockAddr dbInteger
}

func newDBTable(dbTableID dbInteger, dbTableName dbChar, dbColumns []dbColumn) dbTable {
	dbColumnIDs := map[string]dbInteger{}
	for i := range dbColumns {
		dbColumnIDs[dbColumns[i].name()] = dbColumns[i].dbColumnID
	}

	return dbTable{
		dbTableID:   dbTableID,
		dbTableName: dbTableName,
		dbColumnIDs: dbColumnIDs,
		dbColumns:   dbColumns,
	}
}

func (t dbTable) name() string {
	return string(t.dbTableName)
}

func (t dbTable) column(name string) (*dbColumn, error) {
	dbColumnID, ok := t.dbColumnIDs[name]
	if !ok {
		return nil, fmt.Errorf("Column `%s' does not exist in table `%s'", name, t.name())
	}

	for i := range t.dbColumns {
		if t.dbColumns[i].dbColumnID == dbColumnID {
			return &t.dbColumns[i], nil
		}
	}

	return nil, fmt.Errorf("Table `%s' does not contain column with ID %d", t.name(), dbColumnID)
}

func (t *dbTable) addColumn(dbColumn dbColumn) error {
	if dbColumn, _ := t.column(dbColumn.name()); dbColumn != nil {
		return fmt.Errorf("Duplicate column `%s' in table `%s'", dbColumn.name(), t.name())
	}

	t.dbColumnIDs[dbColumn.name()] = dbColumn.dbColumnID
	t.dbColumns = append(t.dbColumns, dbColumn)
	return nil
}

func (t *dbTable) deleteColumn(name string) error {
	dbColumnID, ok := t.dbColumnIDs[name]
	if !ok {
		return fmt.Errorf("Column `%s' does not exist in table `%s'", name, t.name())
	}

	for i := range t.dbColumns {
		if t.dbColumns[i].dbColumnID == dbColumnID {
			delete(t.dbColumnIDs, name)
			t.dbColumns[i] = t.dbColumns[len(t.dbColumns)-1]
			t.dbColumns = t.dbColumns[:len(t.dbColumns)-1]
			return nil
		}
	}

	return fmt.Errorf("Table `%s' does not contain column with ID %d", t.name(), dbColumnID)
}

func (t dbTable) newDBTuple() (tuple dbTuple) {
	tuple = dbTuple{}

	for i := range t.dbColumns {
		tuple[t.dbColumns[i].name()] = nil
	}

	return tuple
}

func (t dbTable) newDBRecord() (record dbRecord) {
	nulls := newBitmap(len(t.dbColumns))
	for i := range nulls {
		nulls[i] = ^byte(0)
	}

	return dbRecord{
		freeFlag: freeFlag,
		nulls:    nulls,
		dbTuple:  t.newDBTuple(),
	}
}

func (t dbTable) dbRecordSize() (size int) {
	for i := range t.dbColumns {
		size += int(t.dbColumns[i].dbTypeSize)
	}
	return size
}

func (t dbTable) newDBRecordBlock(dbBlockSize int64) dbRecordBlock {
	usableRecordBlockSpace := int(dbBlockSize) - 8
	recordsPerBlock := usableRecordBlockSpace / t.dbRecordSize()
	dbRecords := []dbRecord{}

	for i := 0; i < recordsPerBlock; i++ {
		dbRecords = append(dbRecords, t.newDBRecord())
	}

	return dbRecordBlock{
		dbRecords: dbRecords,
	}
}
