package data

import "errors"

type Row map[string]interface{}

type Table struct {
	TableName    string
	TableColumns []TableColumn
}

type TableColumn struct {
	ColumnName string
	ColumnType dataType
	ColumnSize uint16
}

func (db Database) AllTables() (tables []*Table, err error) {
	for entryBlockNo := db.FirstEntryBlock; entryBlockNo != nullBlockNo; {
		tableEntryBlock, err := db.readTableEntryBlock(entryBlockNo)
		if err != nil {
			return nil, err
		}

		for _, tableEntry := range tableEntryBlock.tableEntries() {
			tableHeaderBlock, err := db.readHeaderBlock(tableEntry.HeaderBlock)
			if err != nil {
				return nil, err
			}

			tables = append(tables, tableHeaderBlock.Table(tableEntry.TableName()))
		}

		entryBlockNo = tableEntryBlock.NextEntryBlock
	}

	return tables, nil
}

func (db Database) FindTable(tableName string) (*Table, error) {
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	tableHeaderBlock, err := db.findHeaderBlock(tableName)
	if err != nil {
		return nil, err
	}

	return tableHeaderBlock.Table(tableEntry.TableName()), nil
}

func (db Database) ReadTable(tableName string) (rows []Row, err error) {
	return rows, errors.New("Not implemented")
}
