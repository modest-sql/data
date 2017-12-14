package data

import (
	"fmt"
	"testing"
)

func TestSystemBlockSize(t *testing.T) {
	db, err := NewDatabase("test.db", 4096)
	if err != nil {
		t.Fatal(err)
	}

	set, err := db.tableSet(db.dbSysTables[0])
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(set)
}
