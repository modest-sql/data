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

	for i := 0; i < 500; i++ {
		if err := db.NewTable(fmt.Sprintf("TABLE_%d", i), []common.TableColumnDefiner{}); err != nil {
			t.Fatal(err)
		}
	}

	set, err := db.tableSet(db.sysTables())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(len(set))
	fmt.Printf("BLOCKS: %d\n", db.dbInfo.blocks)
}
