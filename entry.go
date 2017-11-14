package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	maxTableNameLength         = 60
	maxTableEntries            = 63
	tableEntryBlockPaddingSize = 52
)

type tableEntries [maxTableEntries]tableEntry

type tableEntryBlock struct {
	Signature         blockSignature
	NextEntryBlock    Address
	EntriesCount      uint32
	TableEntriesArray tableEntries
	Padding           [tableEntryBlockPaddingSize]byte
}

func (e tableEntryBlock) tableEntries() []tableEntry {
	return e.TableEntriesArray[:e.EntriesCount]
}

func (e tableEntryBlock) findTableEntry(tableName string) *tableEntry {
	tableEntries := e.tableEntries()

	for i := range tableEntries {
		if tableEntries[i].TableName() == strings.ToUpper(tableName) {
			return &tableEntries[i]
		}
	}

	return nil
}

type tableEntry struct {
	HeaderBlock    Address
	TableNameArray [maxTableNameLength]byte
}

func (t tableEntry) TableName() string {
	return string(bytes.TrimRight(t.TableNameArray[:], "\x00"))
}

func (t tableEntry) SetTableName(tableName string) {
	copy(t.TableNameArray[:], tableName)
}

func (db *Database) readTableEntryBlock(blockNo Address) (*tableEntryBlock, error) {
	block, err := db.readBlock(blockNo)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	tableEntryBlock := &tableEntryBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, tableEntryBlock); err != nil {
		return nil, err
	}

	if tableEntryBlock.Signature != tableEntryBlockSignature {
		return nil, fmt.Errorf("Block %d is not a TableEntryBlock", blockNo)
	}

	return tableEntryBlock, nil
}

func (db *Database) findTableEntry(tableName string) (*tableEntry, error) {
	for blockNo := db.FirstEntryBlock; blockNo != nullBlockNo; {
		tableEntryBlock, err := db.readTableEntryBlock(blockNo)
		if err != nil {
			return nil, err
		}

		if tableEntry := tableEntryBlock.findTableEntry(tableName); tableEntry != nil {
			return tableEntry, nil
		}

		blockNo = tableEntryBlock.NextEntryBlock
	}

	return nil, nil
}

func (db *Database) createTableEntry(tableName string) (*tableEntry, error) {
	return nil, errors.New("createTableEntry not implemented")
}

func (db *Database) deleteTableEntry(tableName string) error {
	return errors.New("deleteTableEntry not implemented")
}
