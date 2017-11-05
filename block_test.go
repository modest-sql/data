package data

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestReadBlock(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	var blocks uint32 = 7
	var blockNo Address = 4
	mockData := make([]byte, metadataBlockSize+blockSize*blocks)
	blockOffset := int(metadataBlockSize + blockSize*(blockNo-1))
	expectedString := "Modest SQL Database"

	copy(mockData[blockOffset:blockOffset+len(expectedString)], expectedString)

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

	dataBlock, err := db.readBlock(blockNo)
	if err != nil {
		t.Fatal(err)
	}

	dataString := string(dataBlock[:len(expectedString)])

	if dataString != expectedString {
		t.Errorf("Expected to read string `%s' from block, got `%s'", expectedString, dataString)
	}
}
