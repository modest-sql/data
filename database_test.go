package data_test

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/modest-sql/data"
)

func TestNewDatabase(t *testing.T) {
	var databaseName string = "test.db"
	var expectedFirstEntryBlock, expectedLastEntryBlock uint32 = 0, 0
	var expectedFirstFreeBlock, expectedLastFreeBlock uint32 = 0, 0
	var expectedFileSize int64 = 128

	db, err := data.NewDatabase(databaseName)

	if err != nil {
		t.Fatal(err)
	}

	if db.FirstEntryBlock != expectedFirstEntryBlock {
		t.Errorf("Expected FirstEntryBlock to be %d, got %d", expectedFirstEntryBlock, db.FirstEntryBlock)
	}

	if db.LastEntryBlock != expectedLastEntryBlock {
		t.Errorf("Expected LastEntryBlock to be %d, got %d", expectedLastEntryBlock, db.LastEntryBlock)
	}

	if db.FirstFreeBlock != expectedFirstFreeBlock {
		t.Errorf("Expected FirstFreeBlock to be %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
	}

	if db.LastFreeBlock != expectedLastFreeBlock {
		t.Errorf("Expected LastFreeBlock to be %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
	}

	fileSize, err := db.FileSize()
	if err != nil {
		t.Fatal(err)
	}

	if fileSize != expectedFileSize {
		t.Errorf("Expected file size of %d bytes, got %d bytes", expectedFileSize, fileSize)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDatabase(t *testing.T) {
	var databasesPath string = filepath.Join(".", "databases")
	var metadataSize = 128
	var expectedFirstEntryBlock, expectedLastEntryBlock uint32 = 7, 10
	var expectedFirstFreeBlock, expectedLastFreeBlock uint32 = 26, 43
	mockData := []uint32{expectedFirstEntryBlock, expectedLastEntryBlock, expectedFirstFreeBlock, expectedLastFreeBlock}
	mockData = append(mockData, make([]uint32, metadataSize/4-len(mockData))...)

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	if err := binary.Write(mockFile, binary.LittleEndian, mockData); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := data.LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	if db.FirstEntryBlock != expectedFirstEntryBlock {
		t.Errorf("Expected FirstEntryBlock to be %d, got %d", expectedFirstEntryBlock, db.FirstEntryBlock)
	}

	if db.LastEntryBlock != expectedLastEntryBlock {
		t.Errorf("Expected LastEntryBlock to be %d, got %d", expectedLastEntryBlock, db.LastEntryBlock)
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
