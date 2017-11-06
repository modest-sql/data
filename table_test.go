package data

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFindTable(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	expectedTable := &Table{
		TableName: "MOVIES",
		TableColumns: []TableColumn{
			TableColumn{ColumnName: "ID_MOVIE", ColumnType: integer},
			TableColumn{ColumnName: "TITLE", ColumnType: char, ColumnSize: 32},
		},
	}

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		_ [blockSize * 7]byte
		tableHeaderBlock
	}{
		DatabaseMetadata: DatabaseMetadata{
			FirstEntryBlock: 1,
			LastEntryBlock:  1},
		tableEntryBlock: tableEntryBlock{
			Signature:    tableEntryBlockSignature,
			EntriesCount: 1,
			TableEntriesArray: tableEntries{
				tableEntry{HeaderBlock: 9, TableNameArray: name(expectedTable.TableName)}},
		},
		tableHeaderBlock: tableHeaderBlock{
			Signature:   tableHeaderBlockSignature,
			ColumnCount: 2,
			TableColumnsArray: tableColumns{
				tableColumn{
					ColumnNameArray: name(expectedTable.TableColumns[0].ColumnName),
					DataType:        expectedTable.TableColumns[0].ColumnType,
				},
				tableColumn{
					ColumnNameArray: name(expectedTable.TableColumns[1].ColumnName),
					DataType:        expectedTable.TableColumns[1].ColumnType,
					Size:            expectedTable.TableColumns[1].ColumnSize,
				},
			}},
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	table, err := db.FindTable(expectedTable.TableName)
	if err != nil {
		t.Fatal(err)
	}

	if table == nil {
		t.Fatal("Table not found")
	}

	if !reflect.DeepEqual(table, expectedTable) {
		t.Error("Retrieved table does not match expected table")
	}
}
