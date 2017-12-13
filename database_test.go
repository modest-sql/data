package data

import (
	"fmt"
	"testing"

	"github.com/modest-sql/common"
)

func TestSystemBlockSize(t *testing.T) {
	db, err := NewDatabase("test.db", 4096)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.NewTable("MOVIES", []common.TableColumnDefiner{}); err != nil {
		t.Fatal(err)
	}

	set, err := db.tableSet(db.sysTables())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(set)
}
