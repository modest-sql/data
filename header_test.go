package data

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var dataTypeNames = map[dataType]string{
	integer:  "INTEGER",
	float:    "FLOAT",
	boolean:  "BOOLEAN",
	char:     "CHAR",
	datetime: "DATETIME",
}

func TestHeader(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	tableName := "MOVIES"

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	expectedTableHeaderBlock := tableHeaderBlock{
		Signature: tableHeaderBlockSignature,
		TableColumns: tableColumns{
			tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
			tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 5},
		}}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		tableHeaderBlock
	}{
		DatabaseMetadata: DatabaseMetadata{
			FirstEntryBlock: 1,
			LastEntryBlock:  1},
		tableEntryBlock: tableEntryBlock{
			Signature:    tableEntryBlockSignature,
			EntriesCount: 1,
			TableEntriesArray: tableEntries{
				tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
		},
		tableHeaderBlock: expectedTableHeaderBlock,
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		t.Fatal(err)
	}

	tableHeader, err := tableEntry.header()
	if err != nil {
		t.Fatal(err)
	}

	if tableHeader.FirstRecordBlock != expectedTableHeaderBlock.FirstRecordBlock {
		t.Errorf("Expected FirstRecordBlock to be %d, got %d", expectedTableHeaderBlock.FirstRecordBlock, tableHeader.FirstRecordBlock)
	}

	for _, tableColumn := range tableHeader.TableColumns {
		for _, expectedTableColumn := range expectedTableHeaderBlock.TableColumns {
			if tableColumn.ColumnName() != expectedTableColumn.ColumnName() {
				t.Errorf("Expected column name to be %s, got %s", expectedTableColumn.ColumnName(), tableColumn.ColumnName())
				continue
			}

			if tableColumn.DataType != expectedTableColumn.DataType {
				t.Errorf("Expected column data type to be %s, got %s", dataTypeNames[expectedTableColumn.DataType], dataTypeNames[tableColumn.DataType])
				continue
			}

			if tableColumn.Size != expectedTableColumn.Size {
				t.Errorf("Expected column size to be %d, got %d", expectedTableColumn.Size, tableColumn.Size)
			}
		}
	}
}
