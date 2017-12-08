package data

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
}

func newDatabase(dbInfo dbInfo, dbTables []dbTable) database {
	dbTableIDs := map[string]dbInteger{}
	for i := range dbTables {
		dbTableIDs[string(dbTables[i].dbTableName)] = dbTables[i].dbTableID
	}

	return database{
		dbInfo:     dbInfo,
		dbTableIDs: dbTableIDs,
		dbTables:   dbTables,
	}
}

func (db database) tableExists(name string) (ok bool) {
	_, ok = db.dbTableIDs[name]
	return ok
}
