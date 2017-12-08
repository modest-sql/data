package data

import "encoding/binary"

type dbTypeID uint8

const (
	dbIntegerTypeID dbTypeID = iota
	dbFloatTypeID
	dbDateTimeTypeID
	dbBooleanTypeID
	dbCharTypeID
)

const (
	dbIntegerSize  = 8
	dbFloatSize    = 8
	dbDateTimeSize = 8
	dbBooleanSize  = 1
)

type dbType interface {
	dbTypeID() dbTypeID
	dbTypeSize() int
	bytes() []byte
}

type dbInteger int64

func (dt dbInteger) dbTypeID() dbTypeID {
	return dbIntegerTypeID
}

func (dt dbInteger) dbTypeSize() int {
	return dbIntegerSize
}

func (dt dbInteger) bytes() []byte {
	b := make([]byte, dbIntegerSize)
	binary.LittleEndian.PutUint64(b, uint64(dt))
	return b
}

type dbFloat float64

func (dt dbFloat) dbTypeID() dbTypeID {
	return dbFloatTypeID
}

func (dt dbFloat) dbTypeSize() int {
	return dbFloatSize
}

func (dt dbFloat) bytes() []byte {
	b := make([]byte, dbFloatSize)
	binary.LittleEndian.PutUint64(b, uint64(dt))
	return b
}

type dbDateTime int64

func (dt dbDateTime) dbTypeID() dbTypeID {
	return dbDateTimeTypeID
}

func (dt dbDateTime) dbTypeSize() int {
	return dbDateTimeSize
}

func (dt dbDateTime) bytes() []byte {
	b := make([]byte, dbDateTimeSize)
	binary.LittleEndian.PutUint64(b, uint64(dt))
	return b
}

type dbBoolean bool

func (dt dbBoolean) dbTypeID() dbTypeID {
	return dbBooleanTypeID
}

func (dt dbBoolean) dbTypeSize() int {
	return dbBooleanSize
}

func (dt dbBoolean) bytes() []byte {
	if dt {
		return []byte{1}
	}
	return []byte{0}
}

type dbChar []byte

func (dt dbChar) dbTypeID() dbTypeID {
	return dbCharTypeID
}

func (dt dbChar) dbTypeSize() int {
	return len(dt)
}

func (dt dbChar) bytes() []byte {
	return dt
}

func (dt dbChar) equals(other dbChar) bool {
	return string(dt) == string(other)
}
