package data_test

import (
	"encoding/binary"
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
	var testBlockSize uint32 = 4096

	t.Run("MagicBytes", func(t *testing.T) {
		if _, err := data.NewDatabase(testDbPath, testBlockSize); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(testDbPath)

		file, err := os.Open(testDbPath)
		if err != nil {
			t.Fatal(err)
		}

		var magicBytes uint32
		if err := binary.Read(file, binary.LittleEndian, &magicBytes); err != nil {
			t.Fatalf("Error while reading magic bytes: %s", err.Error())
		}

		if magicBytes != data.MagicBytes {
			t.Error("Expected to write magic bytes on file")
		}
	})

	t.Run("DatabaseInfo", func(t *testing.T) {
		expectedDbInfo := data.DatabaseInfo{
			BlockSize: testBlockSize,
			Blocks:    1,
		}

		db, err := data.NewDatabase(testDbPath, testBlockSize)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(testDbPath)

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

		if _, err := data.NewDatabase(testDbPath, blockSizes.Low); err == nil {
			t.Errorf("Database shouldn't allow block sizes lower than %d", data.MinBlockSize)
		}
		os.Remove(testDbPath)

		if _, err := data.NewDatabase(testDbPath, blockSizes.Mid); err == nil {
			t.Errorf("Database shouldn't allow block sizes that are not a power of 2")
		}
		os.Remove(testDbPath)

		if _, err := data.NewDatabase(testDbPath, blockSizes.High); err == nil {
			t.Errorf("Database shouldn't allow block sizes greater than %d", data.MaxBlockSize)
		}
		os.Remove(testDbPath)
	})

	t.Run("CorrectBlockSizes", func(t *testing.T) {
		blockSizes := []uint32{data.MinBlockSize, 1024, data.MaxBlockSize}

		for _, blockSize := range blockSizes {
			if _, err := data.NewDatabase(testDbPath, blockSize); err != nil {
				t.Error(err)
			}
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
		MetaTable:      1,
	}

	db, err := data.LoadDatabase(testDbPath)
	if err != nil {
		t.Fatal(err)
	}

	dbInfo := db.DatabaseInfo()
	if !reflect.DeepEqual(dbInfo, expectedDbInfo) {
		t.Error("Database did not return expected information")
	}
}
