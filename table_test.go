package data

import (
	"bytes"
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
	defer os.Remove(mockFile.Name())

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

func TestReadTable(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	tableName := "MOVIES"

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	movieTitle := func(str string) (b [32]byte) {
		copy(b[:], str)
		return b
	}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		tableHeaderBlock
		recordBlock
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
		tableHeaderBlock: tableHeaderBlock{
			Signature:        tableHeaderBlockSignature,
			FirstRecordBlock: 3,
			ColumnCount:      2,
			TableColumnsArray: tableColumns{
				tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
				tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
			}},
		recordBlock: recordBlock{
			Signature: recordBlockSignature,
		},
	}

	mockRecords := [3]struct {
		FreeFlag uint32
		IDMovie  uint32
		Title    [32]byte
	}{
		{0, 0, movieTitle("Lord of the Rings")},
		{0, 1, movieTitle("Harry Potter")},
		{0, 2, movieTitle("Avengers")},
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
		t.Fatal(err)
	}

	copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.ReadTable(tableName)
	if err != nil {
		t.Fatal(err)
	}

	if rowCount, expectedRowCount := len(rows), len(mockRecords); rowCount != expectedRowCount {
		t.Fatalf("Expected to read %d rows, got %d", expectedRowCount, rowCount)
	}

	for i, row := range rows {
		if val, ok := row["ID_MOVIE"]; !ok {
			t.Fatalf("Row %d does not contain column %s", i, "ID_MOVIE")
		} else if val != mockRecords[i].IDMovie {
			t.Errorf("Expected ID_MOVIE %d, got %d", mockRecords[i].IDMovie, val)
		}

		if val, ok := row["TITLE"]; !ok {
			t.Fatalf("Row %d does not contain column %s", i, "TITLE")
		} else if val != mockRecords[i].Title {
			t.Errorf("Expected TITLE `%s', got `%s'", mockRecords[i].Title, val)
		}
	}
}
