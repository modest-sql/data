package data

const (
	maxNameLength = 64
)

const (
	dbSysTablesID dbInteger = iota
	dbSysColumnsID
	dbDefaultNumericsID
	dbDefaultCharsID
)

type dbSysTable dbTable

func newTablesSysTable() dbSysTable {
	buildColumn := func(i dbInteger, typeID dbTypeID, typeSize dbInteger, name string) dbColumn {
		return dbColumn{
			dbTableID:        dbSysTablesID,
			dbColumnID:       i,
			dbColumnPosition: i,
			dbTypeID:         typeID,
			dbTypeSize:       typeSize,
			dbColumnName:     dbChar(name),
		}
	}

	columns := []dbColumn{
		buildColumn(0, dbIntegerTypeID, dbIntegerSize, "TABLE_ID"),
		buildColumn(1, dbIntegerTypeID, dbIntegerSize, "FIRST_RECORD_BLOCK"),
		buildColumn(2, dbIntegerTypeID, dbIntegerSize, "FIRST_AVAILABLE_RECORD_BLOCK"),
		buildColumn(3, dbCharTypeID, maxNameLength, "TABLE_NAME"),
	}

	return dbSysTable(newDBTable(dbSysTablesID, dbChar("SYS_TABLES"), columns))
}

func newColumnsSysTable() dbSysTable {
	buildColumn := func(i dbInteger, typeID dbTypeID, typeSize dbInteger, name string) dbColumn {
		return dbColumn{
			dbTableID:        dbSysColumnsID,
			dbColumnID:       i,
			dbColumnPosition: i,
			dbTypeID:         typeID,
			dbTypeSize:       typeSize,
			dbColumnName:     dbChar(name),
		}
	}

	columns := []dbColumn{
		buildColumn(0, dbIntegerTypeID, dbIntegerSize, "COLUMN_ID"),
		buildColumn(1, dbIntegerTypeID, dbIntegerSize, "TABLE_ID"),
		buildColumn(2, dbIntegerTypeID, dbIntegerSize, "COLUMN_POSITION"),
		buildColumn(3, dbIntegerTypeID, dbIntegerSize, "COLUMN_TYPE"),
		buildColumn(4, dbIntegerTypeID, dbIntegerSize, "COLUMN_SIZE"),
		buildColumn(5, dbIntegerTypeID, dbIntegerSize, "COLUMN_COUNTER"),
		buildColumn(6, dbIntegerTypeID, dbIntegerSize, "COLUMN_CONSTRAINTS"),
		buildColumn(7, dbIntegerTypeID, dbIntegerSize, "DEFAULT_CONSTRAINT_ID"),
		buildColumn(8, dbCharTypeID, maxNameLength, "COLUMN_NAME"),
	}

	return dbSysTable(newDBTable(dbSysColumnsID, dbChar("SYS_COLUMNS"), columns))
}
