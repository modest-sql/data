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
	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	tableHeaderBlock, err := db.readHeaderBlock(tableEntry.HeaderBlock)
	if err != nil {
		return nil, err
	}

	recordSize, columnOffsets := tableHeaderBlock.columnOffsets()
	tableColumns := tableHeaderBlock.TableColumns()

	for recordBlockNo := tableHeaderBlock.FirstRecordBlock; recordBlockNo != nullBlockNo; {
		recordBlock, err := db.readRecordBlock(tableHeaderBlock.FirstRecordBlock)
		if err != nil {
			return nil, err
		}

		for _, record := range recordBlock.Data.split(recordSize) {
			if record.isFree() {
				continue
			}

			row := map[string]interface{}{}

			for _, tableColumn := range tableColumns {
				var value interface{}
				offset := columnOffsets[tableColumn.ColumnName()]

				switch tableColumn.DataType {
				case integer:
					value = record.readInteger(offset)
				case float:
					value = record.readFloat(offset)
				case datetime:
					value = record.readDatetime(offset)
				case boolean:
					value = record.readBoolean(offset)
				case char:
					value = record.readChar(offset, int(tableColumn.Size))
				}

				row[tableColumn.ColumnName()] = value
			}

			rows = append(rows, row)
		}

		recordBlockNo = recordBlock.NextRecordBlock
	}
	return rows, nil
}
