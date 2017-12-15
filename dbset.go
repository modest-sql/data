package data

type dbSet []dbTuple

func (s dbSet) stdSet() []map[string]interface{} {
	newSet := []map[string]interface{}{}

	for i := range s {
		newSet = append(newSet, s[i].stdMap())
	}

	return newSet
}
