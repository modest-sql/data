package data

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

const (
	databasesDirName  = "databases"
	metadataBlockSize = 128
	metadataFields    = 4
)

type DatabaseMetadata struct {
	FirstEntryBlock uint32
	LastEntryBlock  uint32
	FirstFreeBlock  uint32
	LastFreeBlock   uint32
	Padding         [metadataBlockSize - 4*metadataFields]byte
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

	databaseFile, err := os.Create(filepath.Join(databasesPath, databaseName))
	if err != nil {
		return nil, err
	}

	db = &Database{file: databaseFile}

	if err := db.writeMetadata(); err != nil {
		return nil, err
	}

	return db, nil
}

func LoadDatabase(databaseName string) (db *Database, err error) {
	databasesPath := filepath.Join(".", databasesDirName)
	databaseFile, err := os.OpenFile(filepath.Join(databasesPath, databaseName), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	db = &Database{file: databaseFile}

	if err := db.readMetadata(); err != nil {
		return nil, err
	}

	return db, nil
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

func (db *Database) writeMetadata() error {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(db.file, binary.LittleEndian, &db.DatabaseMetadata)
}

func (db *Database) readMetadata() error {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Read(db.file, binary.LittleEndian, &db.DatabaseMetadata)
}
