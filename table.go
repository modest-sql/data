package data

import "errors"

type Table struct {
	TableName    string
	TableColumns []TableColumn
}

type TableColumn struct {
	ColumnName string
	ColumnType dataType
	ColumnSize uint16
}

func (db Database) FindTable(tableName string) (*Table, error) {
	return nil, errors.New("Not implemented")
}
