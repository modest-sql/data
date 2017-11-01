package data

import (
	"errors"
	"os"
)

type DatabaseMetadata struct {
	FirstEntryBlock uint32
	FirstFreeBlock  uint32
	LastFreeBlock   uint32
}

type Database struct {
	file *os.File
	DatabaseMetadata
}

func NewDatabase(databaseName string) (*Database, error) {
	return nil, errors.New("Not implemented")
}

func LoadDatabase(databaseName string) (*Database, error) {
	return nil, errors.New("Not implemented")
}

func (db Database) Close() error {
	return db.file.Close()
}
