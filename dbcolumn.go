package data

import "fmt"
import "github.com/modest-sql/common"

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

func (db *database) newDBColumn(table dbTable, definition common.TableColumnDefiner, pos int) (column dbColumn, err error) {
	columnID := db.columns + 1

	columnName := make(dbChar, maxNameLength)
	copy(columnName, definition.ColumnName())

	var typeID dbTypeID
	var typeSize dbInteger

	switch v := definition.(type) {
	case common.IntegerTableColumn:
		typeID, typeSize = dbIntegerTypeID, dbIntegerSize
	case common.FloatTableColumn:
		typeID, typeSize = dbFloatTypeID, dbFloatSize
	case common.DatetimeTableColumn:
		typeID, typeSize = dbDateTimeTypeID, dbDateTimeSize
	case common.BooleanTableColumn:
		typeID, typeSize = dbBooleanTypeID, dbBooleanSize
	case common.CharTableColumn:
		typeID, typeSize = dbCharTypeID, dbInteger(v.Size())
	}

	column = dbColumn{
		dbTable:          table,
		dbTableID:        table.dbTableID,
		dbColumnID:       dbInteger(columnID),
		dbColumnPosition: dbInteger(pos),
		dbTypeID:         typeID,
		dbTypeSize:       typeSize,
		dbColumnName:     columnName,
	}

	var defaultID dbInteger
	if definition.DefaultValue() != nil {
		column.addConstraint(dbDefaultValueConstraint)

		value := castDBType(definition)
		if _, ok := definition.(common.CharTableColumn); ok {
			defaultID, err = db.newDefaultChar(value)
		} else {
			defaultID, err = db.newDefaultNumeric(value)
		}

		if err != nil {
			return dbColumn{}, err
		}
	}

	if definition.Autoincrementable() {
		column.addConstraint(dbAutoincrementConstraint)
	}

	if !definition.Nullable() {
		column.addConstraint(dbNotNullConstraint)
	}

	if definition.ForeignKey() {
		column.addConstraint(dbForeignKeyConstraint)
	}

	if definition.PrimaryKey() {
		column.addConstraint(dbPrimaryKeyConstraint)
	}

	column.dbDefaultValueConstraintID = defaultID

	db.dbInfo.columns++
	if err := db.writeDbInfo(); err != nil {
		return column, err
	}

	return column, nil
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

func (db *database) newDefaultNumeric(value dbType) (dbInteger, error) {
	defaultID := dbInteger(db.defaultNumerics + 1)

	values := map[string]dbType{
		"VALUE_ID": defaultID,
		"VALUE":    value,
	}

	if err := db.insert(db.sysNumerics(), values); err != nil {
		return 0, err
	}

	db.defaultNumerics++
	if err := db.writeDbInfo(); err != nil {
		return 0, err
	}
	return defaultID, nil
}

func (db *database) newDefaultChar(value dbType) (dbInteger, error) {
	defaultID := dbInteger(db.defaultChars + 1)

	tmp := make(dbChar, maxCharLength)
	copy(tmp, value.bytes())

	values := map[string]dbType{
		"VALUE_ID": defaultID,
		"VALUE":    tmp,
	}

	if err := db.insert(db.sysChars(), values); err != nil {
		return 0, err
	}

	db.defaultChars++
	if err := db.writeDbInfo(); err != nil {
		return 0, err
	}
	return defaultID, nil
}

type byColumnPosition []dbColumn

func (c byColumnPosition) Len() int           { return len(c) }
func (c byColumnPosition) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c byColumnPosition) Less(i, j int) bool { return c[i].dbColumnPosition < c[j].dbColumnPosition }
