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

type tableEntry struct {
	HeaderBlock    Address
	TableNameArray [maxTableNameLength]byte
}

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

func (e *tableEntryBlock) addTableEntry(tableEntry tableEntry) bool {
	if e.EntriesCount >= maxTableEntries {
		return false
	}

	e.TableEntriesArray[e.EntriesCount] = tableEntry
	e.EntriesCount++

	return true
}

func (e tableEntryBlock) findTableEntry(tableName string) *tableEntry {
	tableEntries := e.tableEntries()

	for i := range tableEntries {
		if strings.ToUpper(tableEntries[i].TableName()) == strings.ToUpper(tableName) {
			return &tableEntries[i]
		}
	}

	return nil
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

func (db Database) writeTableEntryBlock(blockAddr Address, tableEntryBlock *tableEntryBlock) error {
	buffer := bytes.NewBuffer(nil)

	tableEntryBlock.Signature = tableEntryBlockSignature
	if err := binary.Write(buffer, binary.LittleEndian, tableEntryBlock); err != nil {
		return err
	}

	block := block{}
	copy(block[:], buffer.Bytes())

	return db.writeBlock(blockAddr, block)
}

func (db *Database) findTableEntry(tableName string) (*tableEntry, error) {
	for blockAddr := db.FirstEntryBlock; blockAddr != nullBlockAddr; {
		tableEntryBlock, err := db.readTableEntryBlock(blockAddr)
		if err != nil {
			return nil, err
		}

		if tableEntry := tableEntryBlock.findTableEntry(tableName); tableEntry != nil {
			return tableEntry, nil
		}

		blockAddr = tableEntryBlock.NextEntryBlock
	}

	return nil, nil
}

func (db *Database) createTableEntry(tableName string) (*tableEntry, error) {
	tableName = strings.ToUpper(tableName)

	tableHeaderBlockAddr, err := db.allocBlock()
	if err != nil {
		return nil, err
	}

	tableEntry := &tableEntry{HeaderBlock: tableHeaderBlockAddr}
	copy(tableEntry.TableNameArray[:], tableName)

	var lastTableEntryBlockAddr Address
	var lastTableEntryBlock *tableEntryBlock

	for tableEntryBlockAddr := db.FirstEntryBlock; tableEntryBlockAddr != nullBlockAddr; {
		tableEntryBlock, err := db.readTableEntryBlock(tableEntryBlockAddr)
		if err != nil {
			return nil, err
		}

		if tableEntryBlock.addTableEntry(*tableEntry) {
			if err := db.writeTableEntryBlock(tableEntryBlockAddr, tableEntryBlock); err != nil {
				return nil, err
			}

			return tableEntry, nil
		}

		lastTableEntryBlockAddr, lastTableEntryBlock = tableEntryBlockAddr, tableEntryBlock
		tableEntryBlockAddr = tableEntryBlock.NextEntryBlock
	}

	newTableEntryBlockAddr, err := db.allocBlock()
	if err != nil {
		return nil, err
	}

	newTableEntryBlock := &tableEntryBlock{}
	newTableEntryBlock.addTableEntry(*tableEntry)

	if db.FirstEntryBlock == nullBlockAddr {
		db.FirstEntryBlock = newTableEntryBlockAddr

		if err := db.writeMetadata(); err != nil {
			return nil, err
		}
	} else {
		lastTableEntryBlock.NextEntryBlock = newTableEntryBlockAddr

		if err := db.writeTableEntryBlock(lastTableEntryBlockAddr, lastTableEntryBlock); err != nil {
			return nil, err
		}
	}

	if err := db.writeTableEntryBlock(newTableEntryBlockAddr, newTableEntryBlock); err != nil {
		return nil, err
	}

	return tableEntry, nil
}

func (db *Database) deleteTableEntry(tableName string) error {
	return errors.New("deleteTableEntry not implemented")
}
