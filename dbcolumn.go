package data

type dbConstraintType uint8

const (
	dbPrimaryKeyConstraint dbConstraintType = 1 << iota
	dbForeignKeyConstraint
	dbAutoincrementConstraint
	dbNotNullConstraint
	dbDefaultValueConstraint
)

type dbColumn struct {
	dbTableID                  dbInteger
	dbColumnID                 dbInteger
	dbTypeID                   dbTypeID
	dbColumnName               dbChar
	dbDefaultValueConstraintID dbInteger
	dbConstraints              dbConstraintType
}

func (dc dbColumn) name() string {
	return string(dc.dbColumnName)
}

func (dc dbColumn) hasConstraint(constraint dbConstraintType) bool {
	return (dc.dbConstraints & constraint) != 0
}
