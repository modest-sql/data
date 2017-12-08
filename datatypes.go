package data

type dbTypeID uint8

const (
	dbIntegerTypeID dbTypeID = iota
	dbFloatTypeID
	dbDateTimeTypeID
	dbBooleanTypeID
	dbCharTypeID
)

type dbDataType interface {
	dbTypeID() dbTypeID
}

type dbInteger int64

func (dt dbInteger) dbTypeID() dbTypeID {
	return dbIntegerTypeID
}

type dbFloat float64

func (dt dbFloat) dbTypeID() dbTypeID {
	return dbFloatTypeID
}

type dbDateTime int64

func (dt dbDateTime) dbTypeID() dbTypeID {
	return dbDateTimeTypeID
}

type dbBoolean bool

func (dt dbBoolean) dbTypeID() dbTypeID {
	return dbBooleanTypeID
}

type dbChar []byte

func (dt dbChar) dbTypeID() dbTypeID {
	return dbCharTypeID
}

func (dt dbChar) equals(other dbChar) bool {
	return string(dt) == string(other)
}
