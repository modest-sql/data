package data

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/modest-sql/common"
)

const (
	databasesDirName  = "databases"
	metadataBlockSize = 128
	metadataFields    = 5
)

type Address uint32

type DatabaseMetadata struct {
	FirstEntryBlock Address
	LastEntryBlock  Address
	FirstFreeBlock  Address
	LastFreeBlock   Address
	BlockCount      uint32
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

func (db Database) writeMetadata() error {
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

func (addr Address) offset() int64 {
	if addr == 0 {
		panic("Block address must be greater than 0")
	}

	return int64(metadataBlockSize + blockSize*(addr-1))
}

/*
CommandFactory creates instances of common.Command according to command object received
as parameter. Once the command is run, execution is moved to the callback function received as parameter.
*/
func (db *Database) CommandFactory(cmd interface{}, cb func(interface{}, error)) (command common.Command) {
	switch cmd := cmd.(type) {
	case *common.CreateTableCommand:
		command = common.NewCommand(
			cmd,
			common.Create,
			func() {
				cb(db.NewTable(cmd.TableName(), cmd.TableColumnDefiners()))
			},
		)

	case *common.InsertCommand:
		command = common.NewCommand(
			cmd,
			common.Insert,
			func() {
				cb(nil, db.Insert(cmd.TableName(), cmd.Values()))
			},
		)

	case *common.SelectTableCommand:
		command = common.NewCommand(
			cmd,
			common.Select,
			func() {
				cb(db.ReadTable(cmd.TableName()))
			},
		)
	}

	return command
}
