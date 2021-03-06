package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

type dbTable struct {
	dbTableID            dbInteger
	dbTableName          dbChar
	dbColumnIDs          map[string]dbInteger
	dbColumns            []dbColumn
	firstRecordBlockAddr dbInteger
}

func newDBTable(dbTableID dbInteger, dbTableName dbChar, dbColumns []dbColumn, firstRecordBlockAddr dbInteger) dbTable {
	dbColumnIDs := map[string]dbInteger{}
	for i := range dbColumns {
		dbColumnIDs[dbColumns[i].name()] = dbColumns[i].dbColumnID
	}

	sort.Sort(byColumnPosition(dbColumns))

	return dbTable{
		dbTableID:            dbTableID,
		dbTableName:          dbTableName,
		dbColumnIDs:          dbColumnIDs,
		dbColumns:            dbColumns,
		firstRecordBlockAddr: firstRecordBlockAddr,
	}
}

func (t dbTable) name() string {
	return trimName(t.dbTableName)
}

func (t dbTable) column(name string) (*dbColumn, error) {
	dbColumnID, ok := t.dbColumnIDs[qualifiedIdentifier(t, name)]
	if !ok {
		return nil, fmt.Errorf("Column `%s' does not exist in table `%s'", name, t.name())
	}

	for i := range t.dbColumns {
		if t.dbColumns[i].dbColumnID == dbColumnID {
			return &t.dbColumns[i], nil
		}
	}

	return nil, fmt.Errorf("Table `%s' does not contain column with ID %d", t.name(), dbColumnID)
}

func (t *dbTable) addColumn(dbColumn dbColumn) error {
	if dbColumn, _ := t.column(dbColumn.name()); dbColumn != nil {
		return fmt.Errorf("Duplicate column `%s' in table `%s'", dbColumn.name(), t.name())
	}

	dbColumn.dbTable = *t
	dbColumn.dbTableID = t.dbTableID

	t.dbColumnIDs[dbColumn.name()] = dbColumn.dbColumnID
	t.dbColumns = append(t.dbColumns, dbColumn)
	return nil
}

func (t *dbTable) deleteColumn(name string) error {
	dbColumnID, ok := t.dbColumnIDs[name]
	if !ok {
		return fmt.Errorf("Column `%s' does not exist in table `%s'", name, t.name())
	}

	for i := range t.dbColumns {
		if t.dbColumns[i].dbColumnID == dbColumnID {
			delete(t.dbColumnIDs, name)
			t.dbColumns[i] = t.dbColumns[len(t.dbColumns)-1]
			t.dbColumns = t.dbColumns[:len(t.dbColumns)-1]
			return nil
		}
	}

	return fmt.Errorf("Table `%s' does not contain column with ID %d", t.name(), dbColumnID)
}

func (t dbTable) newDBTuple() (tuple dbTuple) {
	tuple = dbTuple{}

	for i := range t.dbColumns {
		tuple[t.dbColumns[i].name()] = nil
	}

	return tuple
}

func (t dbTable) newDBRecord() (record dbRecord) {
	nulls := newBitmap(len(t.dbColumns))
	for i := range nulls {
		nulls[i] = ^byte(0)
	}

	return dbRecord{
		freeFlag: freeFlag,
		nulls:    nulls,
		dbTuple:  t.newDBTuple(),
	}
}

func (t dbTable) buildDBRecord(values map[string]dbType) (record dbRecord, err error) {
	record = t.newDBRecord()
	record.freeFlag = 0

	for key, value := range values {
		column, err := t.column(qualifiedIdentifier(t, key))
		if err != nil {
			return record, err
		}

		record.insertColumnValue(value, *column)
	}

	return record, nil
}

func (t dbTable) recordSize() (size int) {
	size += freeFlagSize
	size += bitmapSize(len(t.dbColumns)) //record's null bitmap size

	for i := range t.dbColumns {
		size += int(t.dbColumns[i].dbTypeSize)
	}
	return size
}

func (t dbTable) recordsPerBlock(blockSize int64) int {
	return (int(blockSize) - 8) / t.recordSize()
}

func (t dbTable) newDBRecordBlock(blockSize int64) (rb dbRecordBlock, err error) {
	recordsPerBlock := t.recordsPerBlock(blockSize)
	if recordsPerBlock == 0 {
		return rb, errors.New("Record does not fit in record block")
	}

	for i := 0; i < recordsPerBlock; i++ {
		rb.dbRecords = append(rb.dbRecords, t.newDBRecord())
	}

	return rb, nil
}

func (t dbTable) recordBlockBytes(recordBlock dbRecordBlock) (b []byte) {
	b = make([]byte, binary.Size(int64(0)))
	binary.LittleEndian.PutUint64(b, uint64(recordBlock.nextRecordBlock))

	for _, record := range recordBlock.dbRecords {
		freeFlagB := make([]byte, freeFlagSize)
		binary.LittleEndian.PutUint32(freeFlagB, record.freeFlag)

		b = append(b, freeFlagB...)
		b = append(b, record.nulls...)

		for _, column := range t.dbColumns {
			if record.columnIsNull(column) {
				b = append(b, make([]byte, column.dbTypeSize)...)
			} else {
				b = append(b, record.columnValue(column).bytes()...)
			}
		}
	}

	return b
}

func (t dbTable) loadRecordBlockBytes(b []byte) dbRecordBlock {
	recordSize := t.recordSize()
	rb := dbRecordBlock{nextRecordBlock: int64(binary.LittleEndian.Uint64(b))}

	for rs := b[recordsOffset:]; len(rs) >= recordSize; rs = rs[recordSize:] {
		record := dbRecord{
			freeFlag: binary.LittleEndian.Uint32(rs[:freeFlagSize]),
			nulls:    newBitmap(len(t.dbColumns)),
			dbTuple:  dbTuple{},
		}

		valueOffset := freeFlagSize + len(record.nulls)
		copy(record.nulls, rs[freeFlagSize:valueOffset])
		for i, column := range t.dbColumns {
			nextValueOffset := valueOffset + int(column.dbTypeSize)

			var value dbType
			if !record.nulls.At(uint(i)) {
				value = loadDBType(column.dbTypeID, rs[valueOffset:nextValueOffset])
			}
			record.dbTuple[column.name()] = value

			valueOffset = nextValueOffset
		}

		rb.dbRecords = append(rb.dbRecords, record)
	}

	return rb
}
