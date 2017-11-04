package data

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestReadTableEntryBlock(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	var expectedNextEntryBlock Address = 4
	var blockNo Address = 1
	blockOffset := int(metadataBlockSize + blockSize*(blockNo-1))
	expectedEntriesCount := 10

	mockData := make([]byte, metadataBlockSize+blockSize)
	blockBuffer := bytes.NewBuffer(mockData[metadataBlockSize:blockOffset])

	if err := binary.Write(blockBuffer, binary.LittleEndian, expectedNextEntryBlock); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < expectedEntriesCount; i++ {
		entry := &tableEntry{}

		if err := binary.Write(blockBuffer, binary.LittleEndian, entry); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	if err := binary.Write(mockFile, binary.LittleEndian, mockData); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	entryBlock, err := db.readTableEntryBlock(blockNo)
	if err != nil {
		t.Fatal(err)
	}

	if entryBlock.nextEntryBlock != expectedNextEntryBlock {
		t.Errorf("Expected nextEntryBlock to be %d, got %d", expectedNextEntryBlock, entryBlock.nextEntryBlock)
	}

	if entriesCount := len(entryBlock.tableEntries); entriesCount != expectedEntriesCount {
		t.Errorf("Expected table entry block to have %d entries, got %d", expectedEntriesCount, entriesCount)
	}
}

func TestFindTableEntry(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	var nextEntryBlock Address
	var blockNo Address = 1
	blockOffset := int(metadataBlockSize + blockSize*(blockNo-1))
	expectedTableNames := []string{"MOVIES", "THEATERS", "FUNCTIONS", "HALLS"}
	expectedHeaderBlocks := []Address{7, 12, 14, 20}

	mockData := make([]byte, metadataBlockSize+blockSize)
	blockBuffer := bytes.NewBuffer(mockData[metadataBlockSize:blockOffset])

	if err := binary.Write(blockBuffer, binary.LittleEndian, nextEntryBlock); err != nil {
		t.Fatal(err)
	}

	for i, expectedTableName := range expectedTableNames {
		entry := &tableEntry{headerBlock: expectedHeaderBlocks[i]}
		copy(entry.tableName[:], expectedTableName)

		if err := binary.Write(blockBuffer, binary.LittleEndian, entry); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	if err := binary.Write(mockFile, binary.LittleEndian, mockData); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	for i, expectedTableName := range expectedTableNames {
		entry, err := db.findTableEntry(expectedTableName)
		if err != nil {
			t.Fatal(err)
		}

		if entry == nil {
			t.Errorf("Expected TableEntry with name `%s', none found", expectedTableName)
		}

		if entry.headerBlock != expectedHeaderBlocks[i] {
			t.Errorf("Expected TableEntry with name `%s' to have header block %d, got %d", expectedTableName, expectedHeaderBlocks[i], entry.headerBlock)
		}
	}
}
