package data

type Row dbTuple

type ResultSet struct {
	Keys []string
	Rows []Row
}
