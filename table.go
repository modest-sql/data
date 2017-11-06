package data

import (
	"errors"
	"fmt"
	"reflect"
)

// Table --
type Table struct {
	IDMovie int
	Title   [30]byte
}

// Buffer --
type Buffer struct {
	records map[string][]interface{}
}

// NewTable --
func (db *Database) NewTable(tableName string) (*Table, error) {

	table := &Table{}

	return table, nil
}

// FindTable --
func (db *Database) FindTable(tableName string, blockNo Address) (*Table, error) {

	record, _ := db.readRecordBlock(blockNo)
	myData := make(map[string]interface{})
	myData[tableName] = record.Data

	result := &Table{}
	err := result.fillStruct(myData)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// fillStruct --
func (t *Table) fillStruct(m map[string]interface{}) error {
	for k, v := range m {
		err := setField(t, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// setField --
func setField(obj interface{}, title string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(title)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", title)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", title)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.New("Provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}
