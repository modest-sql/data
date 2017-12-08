package data

type dbTable struct {
	dbTableID            dbInteger
	dbTableName          dbChar
	dbColumns            []dbColumn
	dbColumnIDs          map[string]dbInteger
	firstRecordBlockAddr dbInteger
}

func newDBTable(dbTableID dbInteger, dbTableName dbChar, dbColumns []dbColumn) dbTable {
	dbColumnIDs := map[string]dbInteger{}
	for i := range dbColumns {
		dbColumnIDs[string(dbColumns[i].dbColumnName)] = dbColumns[i].dbColumnID
	}

	return dbTable{
		dbTableID:   dbTableID,
		dbTableName: dbTableName,
		dbColumns:   dbColumns,
		dbColumnIDs: dbColumnIDs,
	}
}

func (t dbTable) hasColumn(name string) (ok bool) {
	_, ok = t.dbColumnIDs[name]
	return ok
}
