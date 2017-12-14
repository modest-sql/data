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
		if err := db.NewTable(fmt.Sprintf("TABLE_%d", i), columns); err != nil {
			t.Fatal(err)
		}
	}

	tablesSet, err := db.tableSet(db.sysTables())
	if err != nil {
		t.Fatal(err)
	}

	columnsSet, err := db.tableSet(db.sysColumns())
	if err != nil {
		t.Fatal(err)
	}

	numericsSet, err := db.tableSet(db.sysNumerics())
	if err != nil {
		t.Fatal(err)
	}

	charsSet, err := db.tableSet(db.sysChars())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(charsSet)

	fmt.Printf("TABLES: %d\n", len(tablesSet))
	fmt.Printf("COLUMNS: %d\n", len(columnsSet))
	fmt.Printf("NUMERICS: %d\n", len(numericsSet))
	fmt.Printf("CHARS: %d\n", len(charsSet))
	fmt.Printf("BLOCKS: %d\n", db.dbInfo.blocks)
}
