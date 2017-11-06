package data

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestReadRecordBlock(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")

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
				tableEntry{HeaderBlock: 2, TableNameArray: name("MOVIES")}},
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

	mockFile, err := os.Create(filepath.Join(databasesPath, "mock.db"))
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
}
