package data

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
		recordBlock, err := db.readRecordBlock(tableHeaderBlock.FirstRecordBlock)
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

func (db Database) Insert(tableName string, values map[string]interface{}) error {
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return err
	}
	tableHeaderBlock, err := db.readHeaderBlock(tableEntry.HeaderBlock)
	if err != nil {
		return err
	}

	block, err := db.readBlock(tableEntry.HeaderBlock)
	if err != nil {
		return err
	}

	recordSize, _ := tableHeaderBlock.recordReaders()
	tableColumns := tableHeaderBlock.TableColumns()
	rows := []Row{}

	for recordBlockAddr := tableHeaderBlock.FirstRecordBlock; recordBlockAddr != nullBlockAddr; {
		recordBlock, err := db.readRecordBlock(tableHeaderBlock.FirstRecordBlock)
		if err != nil {
			return err
		}

		for _, record := range recordBlock.Data.split(recordSize) {
			if record.isFree() {
				continue
			}

			row := Row{}
			for _, tableColumn := range tableColumns {
				columnName := tableColumn.ColumnName()
				if columnName == values[columnName].(string) {
					row[columnName] = values[columnName].(string)
				}
			}
			rows = append(rows, row)
		}

		err = db.writeBlock(recordBlockAddr, block)
		if err != nil {
			return err
		}
		recordBlockAddr = recordBlock.NextRecordBlock
	}
	return nil
}
