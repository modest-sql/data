package data

import (
	"github.com/modest-sql/common"
)

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

type ResultSet struct {
	Keys []string
	Rows []Row
}

func (db Database) AllTables() (tables []*Table, err error) {
	for entryBlockAddr := db.FirstEntryBlock; entryBlockAddr != nullBlockAddr; {
		tableEntryBlock, err := db.readTableEntryBlock(entryBlockAddr)
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

		entryBlockAddr = tableEntryBlock.NextEntryBlock
	}

	return tables, nil
}

func (db *Database) NewTable(tableName string, columns common.TableColumnDefiners) (table *Table, err error) {
	tableEntry, err := db.createTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	tableHeaderBlock := &tableHeaderBlock{}

	for _, column := range columns {
		tableColumn := tableColumn{
			DataType: dataTypeOf(column),
		}

		if tableColumn.DataType == char {
			c := column.(common.CharTableColumn)
			tableColumn.Size = uint16(c.Size())
		}

		copy(tableColumn.ColumnNameArray[:], column.ColumnName())

		tableHeaderBlock.AddTableColumn(tableColumn)
	}

	if err := db.writeTableHeaderBlock(tableEntry.HeaderBlock, tableHeaderBlock); err != nil {
		return nil, err
	}

	return tableHeaderBlock.Table(tableName), nil
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

func (db Database) ReadTable(tableName string) (*ResultSet, error) {
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	tableHeaderBlock, err := db.readHeaderBlock(tableEntry.HeaderBlock)
	if err != nil {
		return nil, err
	}

	recordSize, readers := tableHeaderBlock.recordReaders()
	tableColumns := tableHeaderBlock.TableColumns()

	rows := []Row{}
	for recordBlockAddr := tableHeaderBlock.FirstRecordBlock; recordBlockAddr != nullBlockAddr; {
		recordBlock, err := db.readRecordBlock(recordBlockAddr)
		if err != nil {
			return nil, err
		}

		for _, record := range recordBlock.Data.split(recordSize) {
			if record.isFree() {
				continue
			}

			row := Row{}

			for _, tableColumn := range tableColumns {
				columnName := tableColumn.ColumnName()
				row[columnName] = readers[columnName](record)
			}

			rows = append(rows, row)
		}

		recordBlockAddr = recordBlock.NextRecordBlock
	}

	keys := []string{}
	for _, tableColumn := range tableColumns {
		keys = append(keys, tableColumn.ColumnName())
	}

	return &ResultSet{Keys: keys, Rows: rows}, nil
}
