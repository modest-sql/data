package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const maxConstraintLength = 4084

type constraintData [maxConstraintLength]byte

type rawTableConstraint []byte

type tableConstraintBlock struct {
	Signature           blockSignature
	NextConstraintBlock Address
	ConstraintCount     uint32
	ConstraintData      constraintData
}

func (b tableConstraintBlock) columnConstraints(columns []tableColumn) (constraints map[string]columnConstraint) {
	var startOffset int
	for _, column := range columns {
		constraintSize := 5 + column.ColumnSize()

		if startOffset+constraintSize >= blockSize {
			return constraints
		}

		rawConstraint := b.ConstraintData[startOffset : startOffset+constraintSize]

		constraint := columnConstraint{
			ConstraintFlags:  rawConstraint[0],
			Counter:          binary.LittleEndian.Uint32(rawConstraint[1:5]),
			DefaultValueData: rawConstraint[6:],
		}

		constraints[column.ColumnName()] = constraint

		startOffset += constraintSize
	}

	return constraints
}

func (db Database) columnConstraints(blockAddr Address, columns []tableColumn) (constraints map[string]columnConstraint, err error) {
	currentColumns := columns[:]

	for constraintBlockAddr := blockAddr; constraintBlockAddr != nullBlockAddr; {
		constraintBlock, err := db.readTableConstraintBlock(constraintBlockAddr)
		if err != nil {
			return nil, err
		}

		blockConstraints := constraintBlock.columnConstraints(currentColumns)
		for k, v := range blockConstraints {
			constraints[k] = v
		}

		currentColumns = currentColumns[len(blockConstraints):]
		constraintBlockAddr = constraintBlock.NextConstraintBlock
	}
	return constraints, nil
}

type columnConstraint struct {
	ConstraintFlags  byte
	Counter          uint32
	DefaultValueData []byte
}

func (c columnConstraint) Bytes() (b []byte) {
	b = append(b, c.ConstraintFlags)

	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, c.Counter)
	b = append(b, buffer...)

	b = append(b, c.DefaultValueData...)
	return b
}

func (c columnConstraint) HasDefaultValue() bool {
	return (c.ConstraintFlags & 1) != 0
}

func (c columnConstraint) IsAutoincrementable() bool {
	return (c.ConstraintFlags & (1 << 1)) != 0
}

func (c columnConstraint) IsNullable() bool {
	return (c.ConstraintFlags & (1 << 2)) != 0
}

func (c columnConstraint) IsPrimaryKey() bool {
	return (c.ConstraintFlags & (1 << 3)) != 0
}

func (c columnConstraint) IsForeignKey() bool {
	return (c.ConstraintFlags & (1 << 4)) != 0
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
