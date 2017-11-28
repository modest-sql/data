package data

import (
	"encoding/binary"
	"errors"
)

const (
	recordBlockSignature signature = 0x4b25ad3b
	freeFlag             flag      = 0x13f6b89f
)

type flag uint32

type signature uint32

type tuple []storable

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
	full    flag
	records []record
}

type record struct {
	Free  flag
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
	return r.Free.size() + r.Tuple.size()
}

func (r record) bytes() []byte {
	return append(r.Free.bytes(), r.Tuple.bytes()...)
}

func (t tuple) size() (size int) {
	for _, storable := range t {
		size += storable.size()
	}

	return size
}

func (t tuple) bytes() (bytes []byte) {
	for _, storable := range t {
		bytes = append(bytes, storable.bytes()...)
	}

	return bytes
}

func (b block) bytes() []byte {
	return append(b.Signature.bytes(), b.NextBlock.bytes()...)
}

func (b block) size() (size int) {
	return b.Signature.size() + b.NextBlock.size()
}

func (rb recordBlock) bytes() (bytes []byte) {
	bytes = append(rb.block.bytes(), rb.full.bytes()...)

	for _, record := range rb.records {
		bytes = append(bytes, record.bytes()...)
	}

	return bytes
}

func (rb recordBlock) size() (size int) {
	size = rb.block.size() + rb.full.size()

	for _, record := range rb.records {
		size += record.size()
	}

	return size
}

func newRecord(t tuple) record {
	return record{freeFlag, t}
}

func (db Database) newRecordBlock(r record) (*recordBlock, error) {
	recordSize := r.size()

	rb := &recordBlock{
		block: block{
			Signature: recordBlockSignature,
		},
	}

	if (int(db.databaseInfo.BlockSize) - rb.size()) < recordSize {
		return nil, errors.New("Tuple size is greater than record block size")
	}

	tuplesPerBlock := int(db.databaseInfo.BlockSize) / recordSize
	for i := 0; i < tuplesPerBlock; i++ {
		rb.records = append(rb.records, r)
	}

	return rb, nil
}
