package data

type dbTable struct {
	dbTableID            dbInteger
	dbTableName          dbChar
	dbColumnIDs          map[string]dbInteger
	dbColumns            []dbColumn
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
		dbColumnIDs: dbColumnIDs,
		dbColumns:   dbColumns,
	}
}

func (t dbTable) columnExists(name string) (ok bool) {
	_, ok = t.dbColumnIDs[name]
	return ok
}
