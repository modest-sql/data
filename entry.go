package data

import "errors"
import "bytes"
import "encoding/binary"
import "fmt"

const (
	maxTableNameLength         = 60
	maxTableEntries            = 63
	tableEntryBlockPaddingSize = 47
)

type tableEntries [maxTableEntries]tableEntry

type tableEntryBlock struct {
	Signature         blockSignature
	NextEntryBlock    Address
	EntriesCount      uint32
	TableEntriesArray tableEntries
	Padding           [tableEntryBlockPaddingSize]byte
}

func (e tableEntryBlock) TableEntries() []tableEntry {
	return e.TableEntriesArray[:e.EntriesCount]
}

type tableEntry struct {
	TableNameArray [maxTableNameLength]byte
	HeaderBlock    Address
}

func (t tableEntry) TableName() string {
	return string(t.TableNameArray[:])
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
	return nil, errors.New("Not implemented")
}
