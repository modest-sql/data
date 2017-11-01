package data

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const (
	databasesDirName  = "databases"
	metadataBlockSize = 128
)

type DatabaseMetadata struct {
	FirstEntryBlock uint32
	FirstFreeBlock  uint32
	LastFreeBlock   uint32
	Padding         [metadataBlockSize - 4*3]byte
}

type Database struct {
	file *os.File
	DatabaseMetadata
}

func NewDatabase(databaseName string) (db *Database, err error) {
	databasesPath := filepath.Join(".", databasesDirName)
	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		return nil, err
	}

	databasePath := filepath.Join(databasesPath, databaseName)
	databaseFile, err := os.Create(databasePath)
	if err != nil {
		return nil, err
	}

	db = &Database{file: databaseFile}

	if err := db.syncMetadata(); err != nil {
		return nil, err
	}

	return db, nil
}

func LoadDatabase(databaseName string) (*Database, error) {
	return nil, errors.New("Not implemented")
}

func (db Database) FileSize() (int64, error) {
	fileInfo, err := db.file.Stat()
	if err != nil {
		return 0, err
	}

	return fileInfo.Size(), nil
}

func (db Database) Close() error {
	return db.file.Close()
}

func (db *Database) syncMetadata() error {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(db.file, binary.LittleEndian, db.DatabaseMetadata)
}
