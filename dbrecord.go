package data

import "fmt"

const (
	freeFlag uint32 = 0x3314b318
)

type dbRecord struct {
	freeFlag uint32
	nulls    bitmap
	dbTuple  dbTuple
}

func (r dbRecord) isFree() bool {
	return r.freeFlag == freeFlag
}

func (r dbRecord) columnIsNull(dbColumn dbColumn) bool {
	return r.isFree() || r.nulls.At(uint(dbColumn.dbColumnPosition))
}

func (r dbRecord) columnValue(dbColumn dbColumn) (dbType, error) {
	if r.columnIsNull(dbColumn) {
		return nil, nil
	}

	value, ok := r.dbTuple[dbColumn.name()]
	if !ok {
		return nil, fmt.Errorf("Tuple has no element with name `%s'", dbColumn.name())
	}

	return value, nil
}

func (r *dbRecord) insertColumnValue(value dbType, dbColumn dbColumn) error {
	if _, ok := r.dbTuple[dbColumn.name()]; !ok {
		return fmt.Errorf("Tuple has no element with name `%s'", dbColumn.name())
	}

	if value == nil {
		r.nulls.Set(uint(dbColumn.dbColumnPosition))
	} else {
		r.nulls.Clear(uint(dbColumn.dbColumnPosition))
	}

	r.dbTuple[dbColumn.name()] = nil

	return nil
}
