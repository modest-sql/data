package data

import "encoding/binary"

const (
	recordBlockSignature signature = 0x4b25ad3b
	freeFlag             flag      = 0x13f6b89f
)

type flag uint32

func (f flag) bytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(f))
	return bytes
}

func (f flag) size() int {
	return binary.Size(f)
}

type recordBlock struct {
	block
	records    uint32
	recordList []record
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

func (db Database) newRecordBlock(columns []column) *recordBlock {
	r := newRecord(columns)
	recordSize := r.size()

	rb := &recordBlock{
		block: block{
			Signature: recordBlockSignature,
		},
	}

	recordBlockSize := (int(db.databaseInfo.BlockSize) - rb.size())
	if recordSize > recordBlockSize {
		panic("Record size is greater than record block size")
	}

	recordsPerBlock := recordBlockSize / recordSize
	for i := 0; i < recordsPerBlock; i++ {
		rb.recordList = append(rb.recordList, r)
	}

	return rb
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

func (db Database) readRecordBlockAt(columns []column, addr address) (*recordBlock, error) {
	b, err := db.readAt(addr)
	if err != nil {
		return nil, err
	}

	r := newRecord(columns)
	recordSize := r.size()
	rb := &recordBlock{
		block: block{
			Signature: signature(binary.LittleEndian.Uint32(b[:4])),
			NextBlock: address(binary.LittleEndian.Uint32(b[4:8])),
		},
		records: binary.LittleEndian.Uint32(b[8:12]),
	}

	recordSlice := b[12:]
	recordBlockSize := (int(db.databaseInfo.BlockSize) - rb.size())
	recordsPerBlock := recordBlockSize / recordSize
	for i := 0; i < recordsPerBlock; i++ {
		record := record{
			Free:  flag(binary.LittleEndian.Uint32(recordSlice[:4])),
			Nulls: newBitmap(len(columns)),
		}

		copy(record.Nulls, recordSlice[4:4+r.Nulls.size()])
		tupleSlice := recordSlice[4+r.Nulls.size():]

		for i, column := range columns {
			if record.Free == freeFlag || record.Nulls.At(uint(i)) {
				record.Tuple = append(record.Tuple, tupleElement{
					defaultSize: int(column.dataSize),
					isNull:      true,
				})
				tupleSlice = tupleSlice[column.dataSize:]
				continue
			}

			var storable storable
			switch column.dataType {
			case integerType:
				storable = Integer(binary.LittleEndian.Uint64(tupleSlice[:column.dataSize]))
			case floatType:
				storable = Float(binary.LittleEndian.Uint64(tupleSlice[:column.dataSize]))
			case datetimeType:
				storable = Datetime(binary.LittleEndian.Uint64(tupleSlice[:column.dataSize]))
			case booleanType:
				storable = Boolean(tupleSlice[0] != 0)
			case charType:
				storable = Char(tupleSlice[:column.dataSize])
			}

			record.Tuple = append(record.Tuple, tupleElement{
				defaultSize: int(column.dataSize),
				isNull:      false,
				value:       storable,
			})
			tupleSlice = tupleSlice[column.dataSize:]
		}

		rb.recordList = append(rb.recordList, record)
		recordSlice = recordSlice[recordSize:]
	}

	return rb, nil
}

type record struct {
	Free  flag
	Nulls bitmap
	Tuple tuple
}

func (r record) bytes() []byte {
	return append(r.Free.bytes(), append(r.Nulls.bytes(), r.Tuple.bytes()...)...)
}

func (r record) size() int {
	return r.Free.size() + r.Nulls.size() + r.Tuple.size()
}

func newRecord(columns []column) record {
	return record{freeFlag, newBitmap(len(columns)), newTuple(columns)}
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
