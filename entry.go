package data

import "errors"

const tableNameLength = 60

type tableEntries []tableEntry

type tableEntryBlock struct {
	nextEntryBlock Address
	tableEntries
}

type tableEntry struct {
	tableName   [tableNameLength]byte
	headerBlock Address
}

func (db *Database) readTableEntryBlock(blockNo Address) (*tableEntryBlock, error) {
	return nil, errors.New("Not implemented")
}

func (db *Database) findTableEntry(tableName string) (*tableEntry, error) {
	return nil, errors.New("Not implemented")
}
