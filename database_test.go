package data_test

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/modest-sql/data"
)

func TestNewDatabase(t *testing.T) {
	var databaseName string = "test.db"
	var expectedFirstEntryBlock uint32 = 0
	var expectedFirstFreeBlock, expectedLastFreeBlock uint32 = 0, 0

	db, err := data.NewDatabase(databaseName)

	if err != nil {
		t.Fatal(err)
	}

	if db.FirstEntryBlock != expectedFirstEntryBlock {
		t.Errorf("Expected FirstEntryBlock to be %d, got %d", expectedFirstEntryBlock, db.FirstEntryBlock)
	}

	if db.FirstFreeBlock != expectedFirstFreeBlock {
		t.Errorf("Expected FirstFreeBlock to be %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
	}

	if db.LastFreeBlock != expectedLastFreeBlock {
		t.Errorf("Expected LastFreeBlock to be %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDatabase(t *testing.T) {
	var metadataSize = 128
	var expectedFirstEntryBlock uint32 = 7
	var expectedFirstFreeBlock, expectedLastFreeBlock uint32 = 26, 43
	mockData := []uint32{expectedFirstEntryBlock, expectedFirstFreeBlock, expectedLastFreeBlock}

	mockFile, err := ioutil.TempFile(os.TempDir(), "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	if err := binary.Write(mockFile, binary.LittleEndian, mockData); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(mockFile, binary.LittleEndian, make([]byte, metadataSize-len(mockData)*4)); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := data.LoadDatabase(mockFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	if db.FirstEntryBlock != expectedFirstEntryBlock {
		t.Errorf("Expected FirstEntryBlock to be %d, got %d", expectedFirstEntryBlock, db.FirstEntryBlock)
	}

	if db.FirstFreeBlock != expectedFirstFreeBlock {
		t.Errorf("Expected FirstFreeBlock to be %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
	}

	if db.LastFreeBlock != expectedLastFreeBlock {
		t.Errorf("Expected LastFreeBlock to be %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
