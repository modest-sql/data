package data

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	nullBlock            address   = 0
	recordBlockSignature signature = 0x4b25ad3b
	freeFlag             flag      = 0x13f6b89f
)

type flag uint32

type signature uint32

type storable interface {
	bytes() []byte
	size() int
}

type block struct {
	Signature signature
	NextBlock address
}

type recordBlock struct {
	block
	records    uint32
	recordList []record
}

type record struct {
	Free  flag
	Nulls bitmap
	Tuple tuple
}

func (s signature) bytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(s))
	return bytes
}

func (s signature) size() int {
	return binary.Size(s)
}

func (f flag) bytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(f))
	return bytes
}

func (f flag) size() int {
	return binary.Size(f)
}

func (r record) size() int {
	return r.Free.size() + r.Nulls.size() + r.Tuple.size()
}

func (r record) bytes() []byte {
	return append(r.Free.bytes(), append(r.Nulls.bytes(), r.Tuple.bytes()...)...)
}

func (b block) bytes() []byte {
	return append(b.Signature.bytes(), b.NextBlock.bytes()...)
}

func (b block) size() (size int) {
	return b.Signature.size() + b.NextBlock.size()
}

func (rb recordBlock) bytes() (bytes []byte) {
	bytes = rb.block.bytes()
	bytes = append(bytes, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(bytes[4:], rb.records)

	for _, record := range rb.recordList {
		bytes = append(bytes, record.bytes()...)
	}

	return bytes
}

func (rb recordBlock) size() (size int) {
	size = rb.block.size() + binary.Size(rb.records)

	recordsCount := len(rb.recordList)
	if recordsCount > 0 {
		size += recordsCount * rb.recordList[0].size()
	}

	return size
}

func newRecord(t tuple) record {
	return record{freeFlag, newBitmap(len(t)), t}
}

func buildRecord(t tuple) record {
	nulls := newBitmap(len(t))

	for i, element := range t {
		if element.isNull {
			nulls.Set(uint(i))
		}
	}

	return record{Nulls: nulls, Tuple: t}
}

func (rb *recordBlock) insert(t tuple) bool {
	recordSize := rb.recordList[0].size()
	recordsPerBlock := rb.size() / recordSize

	if int(rb.records) >= recordsPerBlock {
		return false
	}

	r := buildRecord(t)
	if recordSize != r.size() {
		panic("New record does not match record size in record block")
	}

	for i, record := range rb.recordList {
		if record.Free == freeFlag {
			rb.recordList[i] = r
			rb.records++
			return true
		}
	}

	return false
}

func (db Database) newRecordBlock(t tuple) (*recordBlock, error) {
	r := newRecord(t)
	recordSize := r.size()

	rb := &recordBlock{
		block: block{
			Signature: recordBlockSignature,
		},
	}

	recordBlockSize := (int(db.databaseInfo.BlockSize) - rb.size())
	if recordSize > recordBlockSize {
		return nil, errors.New("Record size is greater than record block size")
	}

	recordsPerBlock := recordBlockSize / recordSize
	for i := 0; i < recordsPerBlock; i++ {
		rb.recordList = append(rb.recordList, r)
	}

	return rb, nil
}

func (db Database) allocBlock() (newAddr address, err error) {
	rawBlock := &block{}

	if db.databaseInfo.FirstFreeBlock == 0 {
		db.databaseInfo.Blocks++
		newAddr = address(db.databaseInfo.Blocks)

		if _, err := db.file.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}

		if err := binary.Write(db.file, binary.LittleEndian, block{}); err != nil {
			return 0, err
		}
	} else {
		newAddr = db.databaseInfo.FirstFreeBlock

		b, err := db.readAt(db.databaseInfo.FirstFreeBlock)
		if err != nil {
			return 0, err
		}
		fill(rawBlock, b)

		if db.databaseInfo.LastFreeBlock == db.databaseInfo.FirstFreeBlock {
			db.databaseInfo.LastFreeBlock = rawBlock.NextBlock
		}

		db.databaseInfo.FirstFreeBlock = rawBlock.NextBlock
	}

	if err := db.writeAt(rawBlock, db.databaseInfo.MetaTable); err != nil {
		return 0, err
	}

	return newAddr, nil
}
