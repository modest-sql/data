package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
)

// Table --
type Table struct {
	TableName [60]byte `json:"table"`
	Field     `json:"field"`
}

// Field --
type Field struct {
	Column []string `json:"column"`
	IsKey  bool     `json:"Key"`
}

// NewTable --
func (db *Database) NewTable(tableName string) (*Table, error) {

	table := Table{}
	copy(table.TableName[:], tableName)

	return &table, nil
}

// FindTable --
func (db *Database) FindTable(tableName string) (*Table, error) {
	databaseFile, _ := LoadDatabase("test.db")

	byteValue, _ := ioutil.ReadAll(databaseFile.file)
	var idents []Table
	json.Unmarshal(byteValue, &idents)

	for _, json := range idents {
		if reflect.DeepEqual(tableName, json.TableName) {
			return &json, nil
		}
	}
	return nil, nil
}

// Insert --
func (t *Table) Insert(columns []string, values []interface{}) error {

	table := Table{Field: Field{Column: columns}}

	fmt.Println(table)
	return nil
}
