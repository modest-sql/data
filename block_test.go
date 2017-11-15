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
	var blockAddr Address = 4
	mockData := make([]byte, metadataBlockSize+blockSize*blocks)
	blockOffset := int(metadataBlockSize + blockSize*(blockAddr-1))
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

	dataBlock, err := db.readBlock(blockAddr)
	if err != nil {
		t.Fatal(err)
	}

	dataString := string(dataBlock[:len(expectedString)])

	if dataString != expectedString {
		t.Errorf("Expected to read string `%s' from block, got `%s'", expectedString, dataString)
	}
}

func TestAllocBlock(t *testing.T) {
	databasesPath := filepath.Join(".", databasesDirName)

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	t.Run("NoFreeBlocks", func(t *testing.T) {
		expectedBlockAddress, expectedBlockCount := Address(1), uint32(1)
		expectedFirstFreeBlock, expectedLastFreeBlock := Address(0), Address(0)

		mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(mockFile.Name())

		if err := binary.Write(mockFile, binary.LittleEndian, DatabaseMetadata{}); err != nil {
			t.Fatal(err)
		}

		mockFile.Close()

		db, err := LoadDatabase(filepath.Base(mockFile.Name()))
		if err != nil {
			t.Fatal(err)
		}

		blockAddr, err := db.allocBlock()
		if err != nil {
			t.Fatal(err)
		}

		if blockAddr != expectedBlockAddress {
			t.Errorf("Expected block address %d, got %d", expectedBlockAddress, blockAddr)
		}

		if db.FirstFreeBlock != expectedFirstFreeBlock {
			t.Errorf("Expected first free block %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
		}

		if db.LastFreeBlock != expectedLastFreeBlock {
			t.Errorf("Expected last free block %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
		}

		if db.BlockCount != expectedBlockCount {
			t.Errorf("Expected block count %d, got %d", expectedBlockCount, db.BlockCount)
		}
	})

	t.Run("OneFreeBlock", func(t *testing.T) {
		expectedBlockAddress, expectedBlockCount := Address(1), uint32(1)
		expectedFirstFreeBlock, expectedLastFreeBlock := Address(0), Address(0)

		mockDatabase := struct {
			DatabaseMetadata
			dummyBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstFreeBlock: 1,
				LastFreeBlock:  1,
				BlockCount:     1,
			},
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

		blockAddr, err := db.allocBlock()
		if err != nil {
			t.Fatal(err)
		}

		if blockAddr != expectedBlockAddress {
			t.Errorf("Expected block address %d, got %d", expectedBlockAddress, blockAddr)
		}

		if db.FirstFreeBlock != expectedFirstFreeBlock {
			t.Errorf("Expected first free block %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
		}

		if db.LastFreeBlock != expectedLastFreeBlock {
			t.Errorf("Expected last free block %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
		}

		if db.BlockCount != expectedBlockCount {
			t.Errorf("Expected block count %d, got %d", expectedBlockCount, db.BlockCount)
		}
	})

	t.Run("MoreThanOneFreeBlock", func(t *testing.T) {
		expectedBlockAddress, expectedBlockCount := Address(1), uint32(2)
		expectedFirstFreeBlock, expectedLastFreeBlock := Address(2), Address(2)

		mockDatabase := struct {
			DatabaseMetadata
			dummyBlocks [2]dummyBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstFreeBlock: 1,
				LastFreeBlock:  2,
				BlockCount:     2,
			},
			dummyBlocks: [2]dummyBlock{
				dummyBlock{NextBlock: 2},
			},
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

		blockAddr, err := db.allocBlock()
		if err != nil {
			t.Fatal(err)
		}

		if blockAddr != expectedBlockAddress {
			t.Errorf("Expected block address %d, got %d", expectedBlockAddress, blockAddr)
		}

		if db.FirstFreeBlock != expectedFirstFreeBlock {
			t.Errorf("Expected first free block %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
		}

		if db.LastFreeBlock != expectedLastFreeBlock {
			t.Errorf("Expected last free block %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
		}

		if db.BlockCount != expectedBlockCount {
			t.Errorf("Expected block count %d, got %d", expectedBlockCount, db.BlockCount)
		}
	})
}

func TestFreeBlock(t *testing.T) {
	databasesPath := filepath.Join(".", databasesDirName)

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	t.Run("NoFreeBlocks", func(t *testing.T) {
		blockAddr := Address(1)
		expectedBlockCount := uint32(1)
		expectedFirstFreeBlock, expectedLastFreeBlock := blockAddr, blockAddr

		mockDatabase := struct {
			DatabaseMetadata
			dummyBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				BlockCount: 1,
			},
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

		if err := db.freeBlock(blockAddr); err != nil {
			t.Fatal(err)
		}

		if db.FirstFreeBlock != expectedFirstFreeBlock {
			t.Errorf("Expected first free block %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
		}

		if db.LastFreeBlock != expectedLastFreeBlock {
			t.Errorf("Expected last free block %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
		}

		if db.BlockCount != expectedBlockCount {
			t.Errorf("Expected block count %d, got %d", expectedBlockCount, db.BlockCount)
		}
	})

	t.Run("OneFreeBlock", func(t *testing.T) {
		blockAddr := Address(2)
		expectedBlockCount := uint32(2)
		expectedFirstFreeBlock, expectedLastFreeBlock := blockAddr, Address(1)

		mockDatabase := struct {
			DatabaseMetadata
			dummyBlocks [2]dummyBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstFreeBlock: 1,
				LastFreeBlock:  1,
				BlockCount:     2,
			},
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

		if err := db.freeBlock(blockAddr); err != nil {
			t.Fatal(err)
		}

		if db.FirstFreeBlock != expectedFirstFreeBlock {
			t.Errorf("Expected first free block %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
		}

		if db.LastFreeBlock != expectedLastFreeBlock {
			t.Errorf("Expected last free block %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
		}

		if db.BlockCount != expectedBlockCount {
			t.Errorf("Expected block count %d, got %d", expectedBlockCount, db.BlockCount)
		}
	})

	t.Run("MoreThanOneFreeBlock", func(t *testing.T) {
		blockAddr := Address(3)
		expectedBlockCount := uint32(3)
		expectedFirstFreeBlock, expectedLastFreeBlock := blockAddr, Address(2)
		expectedNextBlock := Address(1)

		mockDatabase := struct {
			DatabaseMetadata
			dummyBlocks [3]dummyBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstFreeBlock: 1,
				LastFreeBlock:  2,
				BlockCount:     3,
			},
			dummyBlocks: [3]dummyBlock{
				dummyBlock{NextBlock: 2},
			},
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

		if err := db.freeBlock(blockAddr); err != nil {
			t.Fatal(err)
		}

		if db.FirstFreeBlock != expectedFirstFreeBlock {
			t.Errorf("Expected first free block %d, got %d", expectedFirstFreeBlock, db.FirstFreeBlock)
		}

		if db.LastFreeBlock != expectedLastFreeBlock {
			t.Errorf("Expected last free block %d, got %d", expectedLastFreeBlock, db.LastFreeBlock)
		}

		if db.BlockCount != expectedBlockCount {
			t.Errorf("Expected block count %d, got %d", expectedBlockCount, db.BlockCount)
		}

		block, err := db.readBlock(blockAddr)
		if err != nil {
			t.Fatal(err)
		}

		nextBlock := Address(binary.LittleEndian.Uint32(block[4:8]))
		if nextBlock != expectedNextBlock {
			t.Errorf("Expected block %d to have next block %d, got %d", blockAddr, expectedNextBlock, nextBlock)
		}
	})
}

func TestBlockSizes(t *testing.T) {
	t.Run("DatabaseMetadataBlock", func(t *testing.T) {
		b := DatabaseMetadata{}
		size := binary.Size(b)

		if size != metadataBlockSize {
			t.Errorf("Expected metadata block size to be %d, got %d", metadataBlockSize, size)
		}
	})

	t.Run("TableEntryBlock", func(t *testing.T) {
		b := tableEntryBlock{}
		size := binary.Size(b)

		if size != blockSize {
			t.Errorf("Expected table entry block size to be %d, got %d", blockSize, size)
		}
	})

	t.Run("TableHeaderBlock", func(t *testing.T) {
		b := tableHeaderBlock{}
		size := binary.Size(b)

		if size != blockSize {
			t.Errorf("Expected table header block size to be %d, got %d", blockSize, size)
		}
	})

	t.Run("RecordBlock", func(t *testing.T) {
		b := recordBlock{}
		size := binary.Size(b)

		if size != blockSize {
			t.Errorf("Expected record block size to be %d, got %d", blockSize, size)
		}
	})
}
