package data

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/modest-sql/common"
)

type dbTypeID uint8

const (
	dbIntegerTypeID dbTypeID = iota
	dbFloatTypeID
	dbBooleanTypeID
	dbDateTimeTypeID
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

func newChar(size dbInteger, str string) dbChar {
	tmp := make(dbChar, size)
	copy(tmp, str)
	return tmp
}

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

func loadDBType(dbTypeID dbTypeID, b []byte) dbType {
	switch dbTypeID {
	case dbIntegerTypeID:
		return dbInteger(binary.LittleEndian.Uint64(b))
	case dbFloatTypeID:
		return dbFloat(binary.LittleEndian.Uint64(b))
	case dbDateTimeTypeID:
		return dbDateTime(binary.LittleEndian.Uint64(b))
	case dbBooleanTypeID:
		if b[0] != 0 {
			return dbBoolean(true)
		}
		return dbBoolean(false)
	case dbCharTypeID:
		return dbChar(b)
	}

	return nil
}

func castDBType(definition common.TableColumnDefiner) dbType {
	value := definition.DefaultValue()
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case int64:
		if _, ok := definition.(common.IntegerTableColumn); ok {
			return dbInteger(v)
		}
		return dbDateTime(v)
	case float64:
		return dbFloat(v)
	case bool:
		return dbBoolean(v)
	case string:
		size := definition.(common.CharTableColumn).Size()
		tmp := make(dbChar, size)
		copy(tmp, v)
		return tmp
	}

	return nil
}

func convertValuesMap(table dbTable, values map[string]interface{}) (map[string]dbType, error) {
	dbValues := map[string]dbType{}
	for key, value := range values {
		column, err := table.column(key)
		if err != nil {
			return nil, err
		}

		if value == nil {
			dbValues[column.name()] = nil
			continue
		}

		var dbValue dbType
		switch v := value.(type) {
		case int64:
			if column.dbTypeID != dbIntegerTypeID && column.dbTypeID != dbDateTimeTypeID {
				return nil, fmt.Errorf("Column `%s' is not of type INTEGER or DATETIME", column.name())
			}

			if column.dbTypeID == dbIntegerTypeID {
				dbValue = dbInteger(v)
			} else {
				dbValue = dbDateTime(v)
			}
		case float64:
			if column.dbTypeID != dbFloatTypeID {
				return nil, fmt.Errorf("Column `%s' is not of type FLOAT", column.name())
			}
			dbValue = dbFloat(v)
		case bool:
			if column.dbTypeID != dbBooleanTypeID {
				return nil, fmt.Errorf("Column `%s' is not of type BOOLEAN", column.name())
			}
			dbValue = dbBoolean(v)
		case string:
			if column.dbTypeID != dbCharTypeID {
				return nil, fmt.Errorf("Column `%s' is not of type CHAR", column.name())
			}

			if len(v) > int(column.dbTypeSize) {
				return nil, fmt.Errorf("Column `%s' length can't be greater than %d bytes", column.name(), column.dbTypeSize)
			}

			dbValue = newChar(column.dbTypeSize, v)
		default:
			return nil, fmt.Errorf("Invalid %v type on column `%s'", reflect.TypeOf(v), column.name())
		}

		dbValues[column.name()] = dbValue
	}

	return dbValues, nil
}

func stdType(value dbType) interface{} {
	switch v := value.(type) {
	case dbInteger:
		return int64(v)
	case dbFloat:
		return float64(v)
	case dbDateTime:
		return int64(v)
	case dbBoolean:
		return bool(v)
	case dbChar:
		return trimName(v)
	}

	return nil
}
