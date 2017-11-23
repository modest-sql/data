package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const maxConstraintLength = 4080

type constraint []byte

type constraintData [maxConstraintLength]byte

type rawTableConstraint []byte

type tableConstraintBlock struct {
	Signature           blockSignature
	NextConstraintBlock Address
	FullFlag            uint32
	ConstraintData      constraintData
}

func (b tableConstraintBlock) columnConstraints() (constraints []columnConstraint) {
	for _, constraint := range b.ConstraintData.split() {
		constraints = append(constraints, columnConstraint{
			ColumnIndex:      uint8(constraint[0]),
			DataSize:         binary.LittleEndian.Uint16(constraint[1:3]),
			Counter:          binary.LittleEndian.Uint32(constraint[3:7]),
			DefaultValueData: constraint[7:],
		})
	}

	return constraints
}

func (db Database) columnConstraints(tableName string) (constraints map[string]columnConstraint, err error) {
	constraints = map[string]columnConstraint{}

	tableEntry, err := db.findTableEntry(tableName)
	if err != nil {
		return nil, err
	}

	tableHeaderBlock, err := db.readHeaderBlock(tableEntry.HeaderBlock)
	if err != nil {
		return nil, err
	}

	constraintsArray := []columnConstraint{}
	columns := tableHeaderBlock.TableColumns()

	for constraintBlockAddr := tableEntry.ConstraintBlock; constraintBlockAddr != nullBlockAddr; {
		constraintBlock, err := db.readTableConstraintBlock(constraintBlockAddr)
		if err != nil {
			return nil, err
		}

		constraintsArray = append(constraintsArray, constraintBlock.columnConstraints()...)

		constraintBlockAddr = constraintBlock.NextConstraintBlock
	}

	for _, constraint := range constraintsArray {
		column := columns[constraint.ColumnIndex-1]
		constraints[column.ColumnName()] = constraint
	}

	return constraints, nil
}

type columnConstraint struct {
	ColumnIndex      uint8
	DataSize         uint16
	Counter          uint32
	DefaultValueData []byte
}

func (c columnConstraint) Bytes() (b []byte) {
	counterBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(counterBuffer, c.Counter)

	sizeBuffer := make([]byte, 2)
	binary.LittleEndian.PutUint16(sizeBuffer, c.DataSize)

	b = append(b, c.ColumnIndex)
	b = append(b, sizeBuffer...)
	b = append(b, counterBuffer...)
	b = append(b, c.DefaultValueData...)
	return b
}

func (c columnConstraint) DefaultIntValue() int64 {
	return int64(binary.LittleEndian.Uint64(c.DefaultValueData))
}

func (c columnConstraint) DefaultFloatValue() float64 {
	return float64(binary.LittleEndian.Uint32(c.DefaultValueData))
}

func (c columnConstraint) DefaultBooleanValue() bool {
	return c.DefaultValueData[0] != 0
}

func (c columnConstraint) DefaultDatetimeValue() int64 {
	return c.DefaultIntValue()
}

func (c columnConstraint) DefaultCharValue(size uint16) string {
	return string(bytes.TrimRight(c.DefaultValueData[:], "\x00"))
}

func (db Database) readTableConstraintBlock(blockAddr Address) (*tableConstraintBlock, error) {
	block, err := db.readBlock(blockAddr)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(block[:])
	tableConstraintBlock := &tableConstraintBlock{}

	if err := binary.Read(buffer, binary.LittleEndian, tableConstraintBlock); err != nil {
		return nil, err
	}

	if tableConstraintBlock.Signature != tableConstraintBlockSignature {

		return nil, fmt.Errorf("Block %d is not a TableConstraintBlock", blockAddr)
	}

	return tableConstraintBlock, nil
}

func (db Database) writeTableConstraintBlock(blockAddr Address, tableConstraintBlock *tableConstraintBlock) error {
	buffer := bytes.NewBuffer(nil)

	tableConstraintBlock.Signature = tableConstraintBlockSignature
	if err := binary.Write(buffer, binary.LittleEndian, tableConstraintBlock); err != nil {
		return err
	}

	block := block{}
	copy(block[:], buffer.Bytes())

	return db.writeBlock(blockAddr, block)
}

func (cd *constraintData) split() (constraints []constraint) {
	for startOffset := 0; startOffset < maxConstraintLength; {
		if cd[startOffset] == 0 {
			break
		}

		size := binary.LittleEndian.Uint16(cd[startOffset+1 : startOffset+3])
		endOffset := 7 + int(size)
		constraints = append(constraints, cd[startOffset:endOffset])
		startOffset += endOffset
	}

	return constraints
}

func (b *tableConstraintBlock) insertConstraint(newConstraint columnConstraint) bool {
	if b.FullFlag == fullFlag {
		return false
	}

	bytes := newConstraint.Bytes()
	var startOffset int
	for _, constraint := range b.ConstraintData.split() {
		if constraint[0] != 0 {
			startOffset += len(constraint)
			continue
		}

		endOffset := startOffset + len(bytes)
		if endOffset == maxConstraintLength-1 {
			b.FullFlag = fullFlag
		}

		if endOffset >= maxConstraintLength {
			return false
		}

		copy(b.ConstraintData[startOffset:endOffset], bytes)

		return true
	}

	endOffset := startOffset + len(bytes)
	copy(b.ConstraintData[startOffset:endOffset], bytes)

	return true
}

func (db *Database) insertConstraint(firstConstraintBlockAddr Address, constraint columnConstraint) error {
	var lastConstraintBlockAddr Address
	var lastConstraintBlock *tableConstraintBlock

	for constraintBlockAddr := firstConstraintBlockAddr; constraintBlockAddr != nullBlockAddr; {
		constraintBlock, err := db.readTableConstraintBlock(constraintBlockAddr)
		if err != nil {
			return err
		}

		if constraintBlock.insertConstraint(constraint) {
			return db.writeTableConstraintBlock(constraintBlockAddr, constraintBlock)
		}

		lastConstraintBlock, lastConstraintBlockAddr = constraintBlock, constraintBlockAddr
		constraintBlockAddr = constraintBlock.NextConstraintBlock
	}

	newConstraintBlockAddr, err := db.allocBlock()
	if err != nil {
		return err
	}

	lastConstraintBlock.NextConstraintBlock = newConstraintBlockAddr

	if err := db.writeTableConstraintBlock(lastConstraintBlockAddr, lastConstraintBlock); err != nil {
		return err
	}

	newConstraintBlock := &tableConstraintBlock{}
	newConstraintBlock.insertConstraint(constraint)

	return db.writeTableConstraintBlock(newConstraintBlockAddr, newConstraintBlock)
}
