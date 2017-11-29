package data

import "errors"

func (db Database) validateConstraints(columns []column, values map[string]storable) error {
	return errors.New("Not implemented")
}
