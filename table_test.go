package data

import (
	"os"
	"reflect"
	"testing"
)

func TestDatabase_FindTable(t *testing.T) {
	type fields struct {
		file             *os.File
		DatabaseMetadata DatabaseMetadata
	}
	type args struct {
		tableName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Table
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &Database{
				file:             tt.fields.file,
				DatabaseMetadata: tt.fields.DatabaseMetadata,
			}
			got, err := db.FindTable(tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Database.FindTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Database.FindTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
