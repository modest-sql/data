package data

import (
	"github.com/modest-sql/common"
)

type binaryOperator func(dbType, dbType) bool

func operatorEquals(a dbType, b dbType) bool {
	return a == b
}

//Read https://en.wikipedia.org/wiki/Selection_(relational_algebra)
func selectionByAttribute(r dbSet, theta binaryOperator, a string, b string) (result dbSet) {
	for i := range r {
		if theta(r[i][a], r[i][b]) {
			result = append(result, r[i])
		}
	}
	return result
}

//Read https://en.wikipedia.org/wiki/Selection_(relational_algebra)
func selection(r dbSet, theta common.Expression) (result dbSet) {
	for i := range r {
		if theta.Evaluate(r[i].stdMap()).(bool) {
			result = append(result, r[i])
		}
	}
	return result
}

func projection(r dbSet, names []string) (result dbSet) {
	for i := range r {
		for name := range r[i] {
			if !containsName(name, names) {
				delete(r[i], name)
			}
		}
	}

	return r
}

func joinByAttribute(r dbSet, s dbSet, theta binaryOperator, a string, b string) (result dbSet) {
	for i := range r {
		for j := range s {
			result = append(result, mergeTuples(r[i], s[j]))
		}
	}

	return selectionByAttribute(result, theta, a, b)
}

func join(r dbSet, s dbSet, theta common.Expression) (result dbSet) {
	for i := range r {
		for j := range s {
			result = append(result, mergeTuples(r[i], s[j]))
		}
	}

	return selection(result, theta)
}

func mergeTuples(a dbTuple, b dbTuple) (result dbTuple) {
	result = dbTuple{}

	for key, value := range a {
		result[key] = value
	}

	for key, value := range b {
		result[key] = value
	}

	return result
}

func containsName(name string, names []string) bool {
	for i := range names {
		if names[i] == "*" || name == names[i] {
			return true
		}
	}
	return false
}
