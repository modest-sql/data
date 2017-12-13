package data

import "fmt"

type dbConstraintType uint8

const (
	dbPrimaryKeyConstraint dbConstraintType = 1 << iota
	dbForeignKeyConstraint
	dbAutoincrementConstraint
	dbNotNullConstraint
	dbDefaultValueConstraint
)

var dbConstraintTypeNames = map[dbConstraintType]string{
	dbPrimaryKeyConstraint:    "PRIMARY_KEY",
	dbForeignKeyConstraint:    "FOREIGN_KEY",
	dbAutoincrementConstraint: "AUTOINCREMENT",
	dbNotNullConstraint:       "NOT_NULL",
	dbDefaultValueConstraint:  "DEFAULT_VALUE",
}

type dbColumn struct {
	dbTable                    dbTable
	dbTableID                  dbInteger
	dbColumnID                 dbInteger
	dbColumnPosition           dbInteger
	dbTypeID                   dbTypeID
	dbTypeSize                 dbInteger
	dbColumnName               dbChar
	dbAutoincrementCounter     dbInteger
	dbDefaultValueConstraintID dbInteger
	dbConstraints              dbConstraintType
}

func (dc dbColumn) name() string {
	return concatTable(dc.dbTable.name(), string(dc.dbColumnName))
}

func (dc dbColumn) hasConstraint(constraint dbConstraintType) bool {
	return (dc.dbConstraints & constraint) != 0
}

func (dc *dbColumn) addConstraint(constraint dbConstraintType) error {
	if dc.hasConstraint(constraint) {
		return fmt.Errorf("Duplicate `%s' constraint on column `%s'", dbConstraintTypeNames[constraint], dc.name())
	}

	dc.dbConstraints |= constraint
	return nil
}

func (dc *dbColumn) deleteConstraint(constraint dbConstraintType) error {
	if !dc.hasConstraint(constraint) {
		return fmt.Errorf("Column `%s' does not have `%s' constraint", dc.name(), dbConstraintTypeNames[constraint])
	}

	dc.dbConstraints &^= constraint
	return nil
}

func (dc *dbColumn) increment() error {
	if !dc.hasConstraint(dbAutoincrementConstraint) {
		return fmt.Errorf("Column `%s' does not have autoincrement constraint", dc.name())
	}

	dc.dbAutoincrementCounter++
	return nil
}

type byColumnPosition []dbColumn

func (c byColumnPosition) Len() int           { return len(c) }
func (c byColumnPosition) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c byColumnPosition) Less(i, j int) bool { return c[i].dbColumnPosition < c[j].dbColumnPosition }
