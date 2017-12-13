package data

const (
	dbSysTablesID dbInteger = iota
	dbSysColumnsID
	dbDefaultNumericsID
	dbDefaultCharsID
)

const (
	firstTablesRecordBlockAddr = iota + 2
	firstColumnsRecordBlockAddr
	firstDefaultNumericsAddr
	firstDefaultCharsAddr
)

var sysTablesColumns = []dbColumn{
	buildColumn(0, dbSysTablesID, dbIntegerTypeID, dbIntegerSize, "TABLE_ID"),
	buildColumn(1, dbSysTablesID, dbIntegerTypeID, dbIntegerSize, "FIRST_RECORD_BLOCK"),
	buildColumn(2, dbSysTablesID, dbCharTypeID, maxNameLength, "TABLE_NAME"),
}

var sysColumnsColumns = []dbColumn{
	buildColumn(0, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "COLUMN_ID"),
	buildColumn(1, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "TABLE_ID"),
	buildColumn(2, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "COLUMN_POSITION"),
	buildColumn(3, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "COLUMN_TYPE"),
	buildColumn(4, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "COLUMN_SIZE"),
	buildColumn(5, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "COLUMN_COUNTER"),
	buildColumn(6, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "COLUMN_CONSTRAINTS"),
	buildColumn(7, dbSysColumnsID, dbIntegerTypeID, dbIntegerSize, "DEFAULT_CONSTRAINT_ID"),
	buildColumn(8, dbSysColumnsID, dbCharTypeID, maxNameLength, "COLUMN_NAME"),
}

var sysDefaultNumericsColumns = []dbColumn{
	buildColumn(0, dbDefaultNumericsID, dbIntegerTypeID, dbIntegerSize, "VALUE_ID"),
	buildColumn(1, dbDefaultNumericsID, dbIntegerTypeID, dbIntegerSize, "VALUE"),
}

var sysDefaultCharsColumns = []dbColumn{
	buildColumn(0, dbDefaultCharsID, dbIntegerTypeID, dbIntegerSize, "VALUE_ID"),
	buildColumn(1, dbDefaultCharsID, dbCharTypeID, maxCharLength, "VALUE"),
}

func buildColumn(sysTableID dbInteger, i dbInteger, typeID dbTypeID, typeSize dbInteger, name string) dbColumn {
	return dbColumn{
		dbTableID:        sysTableID,
		dbColumnID:       i,
		dbColumnPosition: i,
		dbTypeID:         typeID,
		dbTypeSize:       typeSize,
		dbColumnName:     dbChar(name),
	}
}

func newDBSysTable(dbTableID dbInteger, dbTableName dbChar, dbColumns []dbColumn, firstRecordBlockAddr dbInteger) dbTable {
	dbColumnIDs := map[string]dbInteger{}
	for i := range dbColumns {
		dbColumnIDs[dbColumns[i].name()] = dbColumns[i].dbColumnID
	}

	return dbTable{
		dbTableID:            dbTableID,
		dbTableName:          dbTableName,
		dbColumnIDs:          dbColumnIDs,
		dbColumns:            dbColumns,
		firstRecordBlockAddr: firstRecordBlockAddr,
	}
}

func newSysTables() []dbTable {
	return []dbTable{
		newTablesSysTable(),
		newColumnsSysTable(),
		newDefaultNumericsSysTable(),
		newDefaultCharsSysTable(),
	}
}

func newTablesSysTable() dbTable {
	return newDBSysTable(dbSysTablesID, dbChar("SYS_TABLES"), sysTablesColumns, firstTablesRecordBlockAddr)
}

func newColumnsSysTable() dbTable {
	return newDBSysTable(dbSysColumnsID, dbChar("SYS_COLUMNS"), sysColumnsColumns, firstColumnsRecordBlockAddr)
}

func newDefaultNumericsSysTable() dbTable {
	return newDBSysTable(dbDefaultNumericsID, dbChar("SYS_DEFAULT_NUMERICS"), sysDefaultNumericsColumns, firstDefaultNumericsAddr)
}

func newDefaultCharsSysTable() dbTable {
	return newDBSysTable(dbDefaultNumericsID, dbChar("SYS_DEFAULT_CHARS"), sysDefaultCharsColumns, firstDefaultCharsAddr)
}
