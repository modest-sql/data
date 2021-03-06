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

	columns := []common.TableColumnDefiner{
		common.NewIntegerTableColumn("ID", nil, false, false, true, false),
		common.NewCharTableColumn("NAME", "TEST", true, false, false, false, 10),
	}

	for i := 0; i < 12; i++ {
		if err := db.NewTable(fmt.Sprintf("TABLE%d", i), columns); err != nil {
			t.Fatal(err)
		}
	}

	values := map[string]interface{}{
		"ID":   int64(57),
		"NAME": "HELLO",
	}

	if err := db.Insert("TABLE0", values); err != nil {
		t.Fatal(err)
	}

	if err := db.Insert("TABLE0", values); err != nil {
		t.Fatal(err)
	}

	if err := db.Insert("TABLE0", values); err != nil {
		t.Fatal(err)
	}

	if err := db.Insert("TABLE0", values); err != nil {
		t.Fatal(err)
	}

	if err := db.Insert("TABLE0", values); err != nil {
		t.Fatal(err)
	}

	if err := db.Drop("TABLE5"); err != nil {
		t.Fatal(err)
	}

	tablesSet, err := db.tableSet(db.sysTables())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(tablesSet[5])
}
