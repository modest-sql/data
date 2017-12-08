package data

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
}

type dbInteger int64

func (dt dbInteger) dbTypeID() dbTypeID {
	return dbIntegerTypeID
}

func (dt dbInteger) dbTypeSize() int {
	return dbIntegerSize
}

type dbFloat float64

func (dt dbFloat) dbTypeID() dbTypeID {
	return dbFloatTypeID
}

func (dt dbFloat) dbTypeSize() int {
	return dbFloatSize
}

type dbDateTime int64

func (dt dbDateTime) dbTypeID() dbTypeID {
	return dbDateTimeTypeID
}

func (dt dbDateTime) dbTypeSize() int {
	return dbDateTimeSize
}

type dbBoolean bool

func (dt dbBoolean) dbTypeID() dbTypeID {
	return dbBooleanTypeID
}

func (dt dbBoolean) dbTypeSize() int {
	return dbBooleanSize
}

type dbChar []byte

func (dt dbChar) dbTypeID() dbTypeID {
	return dbCharTypeID
}

func (dt dbChar) dbTypeSize() int {
	return len(dt)
}

func (dt dbChar) equals(other dbChar) bool {
	return string(dt) == string(other)
}
