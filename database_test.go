package data_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/modest-sql/data"
)

const testBlockSize = 4096

func tmpDbName() string {
	return filepath.Join(".", time.Now().Format("20060102150405"))
}

func TestNewDatabase(t *testing.T) {
	var testDbPath string = tmpDbName()

	t.Run("FileSize", func(t *testing.T) {
		var expectedFileSize int64 = 2 * int64(testBlockSize)

		db, err := data.NewDatabase(testDbPath, testBlockSize)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(testDbPath)
		defer db.Close()

		file, err := os.Open(testDbPath)
		if err != nil {
			t.Fatal(err)
		}

		fileInfo, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if actualFileSize := fileInfo.Size(); actualFileSize != expectedFileSize {
			t.Errorf("Expected file size to be %d bytes, got %d bytes", expectedFileSize, actualFileSize)
		}

	})

	t.Run("DatabaseInfo", func(t *testing.T) {
		expectedDbInfo := data.DatabaseInfo{
			MagicBytes: data.MagicBytes,
			BlockSize:  testBlockSize,
			Blocks:     2,
			MetaTable:  data.MetaTableAddress,
		}

		db, err := data.NewDatabase(testDbPath, testBlockSize)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(testDbPath)
		defer db.Close()

		dbInfo := db.DatabaseInfo()
		if !reflect.DeepEqual(dbInfo, expectedDbInfo) {
			t.Error("Database did not return expected information")
		}
	})

	t.Run("IncorrectBlockSizes", func(t *testing.T) {
		blockSizes := struct {
			Low  uint32
			Mid  uint32
			High uint32
		}{0, 1000, 1073741824}

		if db, err := data.NewDatabase(testDbPath, blockSizes.Low); err == nil {
			t.Errorf("Database shouldn't allow block sizes lower than %d", data.MinBlockSize)
			db.Close()
			os.Remove(testDbPath)
		}

		if db, err := data.NewDatabase(testDbPath, blockSizes.Mid); err == nil {
			t.Errorf("Database shouldn't allow block sizes that are not a power of 2")
			db.Close()
			os.Remove(testDbPath)
		}

		if db, err := data.NewDatabase(testDbPath, blockSizes.High); err == nil {
			t.Errorf("Database shouldn't allow block sizes greater than %d", data.MaxBlockSize)
			db.Close()
			os.Remove(testDbPath)
		}
	})

	t.Run("CorrectBlockSizes", func(t *testing.T) {
		blockSizes := []uint32{data.MinBlockSize, 8192, data.MaxBlockSize}

		for _, blockSize := range blockSizes {
			db, err := data.NewDatabase(testDbPath, blockSize)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			os.Remove(testDbPath)
		}
	})
}

func TestLoadDatabase(t *testing.T) {
	var testDbPath string = filepath.Join("testdata", "test.db")

	expectedDbInfo := data.DatabaseInfo{
		BlockSize:      8192,
		Blocks:         4,
		FreeBlocks:     1,
		FirstFreeBlock: 4,
		LastFreeBlock:  4,
		MetaTable:      data.MetaTableAddress,
	}

	db, err := data.LoadDatabase(testDbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	dbInfo := db.DatabaseInfo()
	if !reflect.DeepEqual(dbInfo, expectedDbInfo) {
		t.Error("Database did not return expected information")
	}
}
