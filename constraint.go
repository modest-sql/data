package data

func (db Database) validateConstraints(columns []column, values map[string]storable) error {
	for _, element := range columns {
		if element.IsNullable() && element.Autoincrementable()
		{
			return errors.New("Constraints is Autoincrementable")
		}

		if element.IsNullable() && element.IsPrimaryKey()
		{
			return errors.New("Constraints is Primary Key")
		}

		if element.HasDefaultValue() && element.Autoincrementable()
		{
			return errors.New("Constraints is Autoincrementable")
		}

		if element.HasDefaultValue() && element.IsPrimaryKey()
		{
			return errors.New("Constraints is Primary Key")
		}

		if element.Autoincrementable() && element.IsForeignKey()
		{
			return errors.New("Constraints is Foreign Key")
		}

		if element.bytes() != values[element.columnSize].bytes || element.size() != values[element.columnSize].size
		{
			return error.New("Constraints invalid")
		}
	}
	return nil
}
