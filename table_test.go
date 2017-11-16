package data

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/modest-sql/common"
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

func TestNewTable(t *testing.T) {
	dbName := "mock.db"
	expectedColumnCount := 2
	expectedIDColumnName, expectedTitleColumnName := "ID_MOVIE", "TITLE"
	expectedTitleSize := 32

	createCmd := common.NewCreateTableCommand("MOVIES", common.TableColumnDefiners{
		common.NewIntegerTableColumn(expectedIDColumnName, nil, false, true),
		common.NewCharTableColumn(expectedTitleColumnName, nil, false, false, expectedTitleSize),
	})

	db, err := NewDatabase(dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(filepath.Join(databasesDirName, dbName))

	if _, err := db.NewTable(createCmd.TableName(), createCmd.TableColumnDefiners()); err != nil {
		t.Fatal(err)
	}

	result, err := db.FindTable(createCmd.TableName())
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatalf("Expected to find table with name `%s', none found", createCmd.TableName())
	}

	if columnCount := len(result.TableColumns); columnCount != expectedColumnCount {
		t.Fatalf("Expected table to have %d columns, got %d", expectedColumnCount, columnCount)
	}

	IDColumn, titleColumn := result.TableColumns[0], result.TableColumns[1]

	if IDColumn.ColumnName != expectedIDColumnName {
		t.Errorf("Expected column to have name `%s', got `%s'", expectedIDColumnName, IDColumn.ColumnName)
	}

	if IDColumn.ColumnType != integer {
		t.Errorf("Expected column to be of type `%s', got `%s'", dataTypeNames[integer], dataTypeNames[IDColumn.ColumnType])
	}

	if titleColumn.ColumnName != expectedTitleColumnName {
		t.Errorf("Expected column to have name `%s', got `%s'", expectedTitleColumnName, titleColumn.ColumnName)
	}

	if titleColumn.ColumnType != char {
		t.Errorf("Expected column to be of type `%s', got `%s'", dataTypeNames[char], dataTypeNames[titleColumn.ColumnType])
	}

	if titleColumn.ColumnSize != uint16(expectedTitleSize) {
		t.Errorf("Expected column to be of size %d, got %d", expectedTitleSize, titleColumn.ColumnSize)
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

	expectedMockRecordsCount := 3
	mockRecords := [102]struct {
		FreeFlag uint32
		IDMovie  uint32
		Title    [32]byte
	}{
		{0, 0, movieTitle("Lord of the Rings")},
		{0, 1, movieTitle("Harry Potter")},
		{0, 2, movieTitle("Avengers")},
	}

	for i := 3; i < 102; i++ {
		mockRecords[i] = struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			FreeFlag: freeFlag,
		}
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
	if err != nil {
		t.Fatal(err)
	}

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

	resultSet, err := db.ReadTable(tableName)
	if err != nil {
		t.Fatal(err)
	}

	rows := resultSet.Rows

	if rowCount := len(rows); rowCount != expectedMockRecordsCount {
		t.Fatalf("Expected to read %d rows, got %d", expectedMockRecordsCount, rowCount)
	}

	for i, row := range rows {
		if val, ok := row["ID_MOVIE"]; !ok {
			t.Fatalf("Row %d does not contain column %s", i, "ID_MOVIE")
		} else if val != int32(mockRecords[i].IDMovie) {
			t.Errorf("Expected ID_MOVIE %d, got %d", int32(mockRecords[i].IDMovie), val)
		}

		if val, ok := row["TITLE"]; !ok {
			t.Fatalf("Row %d does not contain column %s", i, "TITLE")
		} else if expectedTitle := string(bytes.TrimRight(mockRecords[i].Title[:], "\x00")); val != expectedTitle {
			t.Errorf("Expected TITLE `%s', got `%s'", expectedTitle, val)
		}
	}
}
