package data

type Table struct {
	TableName    string
	TableColumns []TableColumn
}

type TableColumn struct {
	ColumnName string
	ColumnType dbTypeID
	ColumnSize uint16
}

func dbColumnToTableColumn(c dbColumn) TableColumn {
	return TableColumn{
		ColumnName: c.name(),
		ColumnType: c.dbTypeID,
		ColumnSize: uint16(c.dbTypeSize),
	}
}

func dbTableToTable(t dbTable) Table {
	var columns []TableColumn
	for _, c := range t.dbColumns {
		columns = append(columns, dbColumnToTableColumn(c))
	}

	return Table{
		TableName:    t.name(),
		TableColumns: columns,
	}
}

func (db database) AllTables() (tables []Table) {
	for _, t := range db.dbTables {
		tables = append(tables, dbTableToTable(t))
	}
	return tables
}
