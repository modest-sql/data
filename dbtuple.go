package data

type dbTuple map[string]dbType

func (t dbTuple) stdMap() map[string]interface{} {
	values := map[string]interface{}{}

	for key, value := range t {
		values[key] = stdType(value)
	}

	return values
}
