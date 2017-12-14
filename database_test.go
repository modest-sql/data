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

	fmt.Printf("DB1: %+v\n", db.dbInfo)
	//fmt.Printf("%+v\n", db.dbTables)

	db2, err := LoadDatabase("test.db")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("DB2: %+v\n", db2.dbInfo)

	table, err := db.table("TABLE2")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%+v\n", table)
}
