package data

const (
	freeFlag     uint32 = 0x3314b318
	freeFlagSize        = 4
)

type dbRecord struct {
	freeFlag uint32
	nulls    bitmap
	dbTuple  dbTuple
}

func (r *dbRecord) removeFree() {
	r.freeFlag = 0
}

func (r dbRecord) isFree() bool {
	return r.freeFlag == freeFlag
}

func (r dbRecord) columnIsNull(dbColumn dbColumn) bool {
	return r.isFree() || r.nulls.At(uint(dbColumn.dbColumnPosition))
}

func (r dbRecord) columnValue(dbColumn dbColumn) dbType {
	if r.columnIsNull(dbColumn) {
		return nil
	}

	return r.dbTuple[dbColumn.name()]
}

func (r *dbRecord) insertColumnValue(value dbType, dbColumn dbColumn) {
	if value == nil {
		r.nulls.Set(uint(dbColumn.dbColumnPosition))
	} else {
		r.nulls.Clear(uint(dbColumn.dbColumnPosition))
	}

	r.dbTuple[dbColumn.name()] = value
}
