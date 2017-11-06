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
	var expectedEntriesCount uint32 = 10

	mockData := make([]byte, metadataBlockSize+blockSize)
	blockBuffer := bytes.NewBuffer(mockData[metadataBlockSize:blockOffset])

	if err := binary.Write(blockBuffer, binary.LittleEndian, tableEntryBlockSignature); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(blockBuffer, binary.LittleEndian, expectedNextEntryBlock); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(blockBuffer, binary.LittleEndian, expectedEntriesCount); err != nil {
		t.Fatal(err)
	}

	for i := uint32(0); i < expectedEntriesCount; i++ {
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

	if entryBlock.NextEntryBlock != expectedNextEntryBlock {
		t.Errorf("Expected nextEntryBlock to be %d, got %d", expectedNextEntryBlock, entryBlock.NextEntryBlock)
	}

	if entryBlock.EntriesCount != expectedEntriesCount {
		t.Errorf("Expected table entry block to have %d entries, got %d", expectedEntriesCount, entryBlock.EntriesCount)
	}

	if entriesCount := uint32(len(entryBlock.tableEntries())); entriesCount != entryBlock.EntriesCount {
		t.Errorf("Entry count in TableEntryBlock does not match actual length of table entries array, expected %d, got %d", entriesCount, entryBlock.EntriesCount)
	}
}

func TestFindTableEntry(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	var blockNo Address = 4
	blockCount := 4
	firstEntryBlock := byte(1)
	expectedTableNames := []string{"MOVIES", "THEATERS", "FUNCTIONS", "HALLS"}
	expectedHeaderBlocks := []Address{7, 12, 14, 20}
	mockData := make([]byte, metadataBlockSize+blockSize*blockCount)

	blockOffset := func(blockNo Address) int {
		return int(metadataBlockSize + blockSize*(blockNo-1))
	}

	mockData[0] = firstEntryBlock

	writeMockBlock := func(blockNo Address) {
		var nextEntryBlock Address
		blockBuffer := bytes.NewBuffer(mockData[metadataBlockSize:blockOffset(blockNo)])

		if int(blockNo) < blockCount {
			nextEntryBlock = blockNo + 1
		}

		if err := binary.Write(blockBuffer, binary.LittleEndian, tableEntryBlockSignature); err != nil {
			t.Fatal(err)
		}

		if err := binary.Write(blockBuffer, binary.LittleEndian, nextEntryBlock); err != nil {
			t.Fatal(err)
		}

		if err := binary.Write(blockBuffer, binary.LittleEndian, uint32(0)); err != nil {
			t.Fatal(err)
		}
	}

	writeMockBlock(1)
	writeMockBlock(2)
	writeMockBlock(3)

	blockBuffer := bytes.NewBuffer(mockData[metadataBlockSize:blockOffset(blockNo)])

	if err := binary.Write(blockBuffer, binary.LittleEndian, tableEntryBlockSignature); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(blockBuffer, binary.LittleEndian, nullBlockNo); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(blockBuffer, binary.LittleEndian, uint32(len(expectedTableNames))); err != nil {
		t.Fatal(err)
	}

	for i, expectedTableName := range expectedTableNames {
		entry := &tableEntry{HeaderBlock: expectedHeaderBlocks[i]}
		copy(entry.TableNameArray[:], expectedTableName)

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
			t.Fatalf("Expected TableEntry with name `%s', none found", expectedTableName)
		}

		if entry.HeaderBlock != expectedHeaderBlocks[i] {
			t.Errorf("Expected TableEntry with name `%s' to have header block %d, got %d", expectedTableName, expectedHeaderBlocks[i], entry.HeaderBlock)
		}
	}
}
