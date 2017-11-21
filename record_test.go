package data

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestReadRecordBlock(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	movieTitle := func(str string) (b [32]byte) {
		copy(b[:], str)
		return b
	}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		tableHeaderBlock
		recordBlock
	}{
		DatabaseMetadata: DatabaseMetadata{
			FirstEntryBlock: 1,
			LastEntryBlock:  1},
		tableEntryBlock: tableEntryBlock{
			Signature:    tableEntryBlockSignature,
			EntriesCount: 1,
			TableEntriesArray: tableEntries{
				tableEntry{HeaderBlock: 2, TableNameArray: name("MOVIES")}},
		},
		tableHeaderBlock: tableHeaderBlock{
			Signature:        tableHeaderBlockSignature,
			FirstRecordBlock: 3,
			ColumnCount:      2,
			TableColumnsArray: tableColumns{
				tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
				tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
			}},
		recordBlock: recordBlock{
			Signature: recordBlockSignature,
		},
	}

	mockRecords := [3]struct {
		FreeFlag uint32
		IDMovie  uint32
		Title    [32]byte
	}{
		{0, 0, movieTitle("Lord of the Rings")},
		{0, 1, movieTitle("Harry Potter")},
		{0, 2, movieTitle("Avengers")},
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := ioutil.TempFile(databasesPath, "modestdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(mockFile.Name())

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
		t.Fatal(err)
	}

	copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	recordBlock, err := db.readRecordBlock(mockDatabase.tableHeaderBlock.FirstRecordBlock)
	if err != nil {
		t.Fatal(err)
	}

	if recordBlock.NextRecordBlock != mockDatabase.NextRecordBlock {
		t.Errorf("Expected next record block to be %d, got %d", mockDatabase.NextRecordBlock, recordBlock.NextRecordBlock)
	}

	for i := range recordBlock.Data {
		if recordBlock.Data[i] != mockDatabase.Data[i] {
			t.Fatal("Record block data does not equal expected record block data")
		}
	}
}

func TestUpdateRecords(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	tableName := "MOVIES"
	newTitle := "REPLACED_TITLE"

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	movieTitle := func(str string) (b [32]byte) {
		copy(b[:], str)
		return b
	}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		tableHeaderBlock
		recordBlock
	}{
		DatabaseMetadata: DatabaseMetadata{
			FirstEntryBlock: 1,
			LastEntryBlock:  1},
		tableEntryBlock: tableEntryBlock{
			Signature:    tableEntryBlockSignature,
			EntriesCount: 1,
			TableEntriesArray: tableEntries{
				tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
		},
		tableHeaderBlock: tableHeaderBlock{
			Signature:        tableHeaderBlockSignature,
			FirstRecordBlock: 3,
			ColumnCount:      2,
			TableColumnsArray: tableColumns{
				tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
				tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
			}},
		recordBlock: recordBlock{
			Signature: recordBlockSignature,
		},
	}

	expectedMockRecordsCount := 3
	mockRecords := [102]struct {
		FreeFlag uint32
		IDMovie  uint32
		Title    [32]byte
	}{
		{0, 0, movieTitle("Lord of the Rings")},
		{0, 1, movieTitle("Harry Potter")},
		{0, 2, movieTitle("Avengers")},
	}

	for i := 3; i < 102; i++ {
		mockRecords[i] = struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			FreeFlag: freeFlag,
		}
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
	if err != nil {
		t.Fatal(err)
	}

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
		t.Fatal(err)
	}

	copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	values := map[string]interface{}{
		"TITLE": newTitle,
	}

	if err := db.updateRecords(tableName, values); err != nil {
		t.Fatal(err)
	}

	resultSet, err := db.ReadTable(tableName)
	if err != nil {
		t.Fatal(err)
	}

	rows := resultSet.Rows

	if rowCount := len(rows); rowCount != expectedMockRecordsCount {
		t.Fatalf("Expected to read %d rows, got %d", expectedMockRecordsCount, rowCount)
	}

	for i, row := range rows {
		if val, ok := row["ID_MOVIE"]; !ok {
			t.Fatalf("Row %d does not contain column %s", i, "ID_MOVIE")
		} else if val != int32(mockRecords[i].IDMovie) {
			t.Errorf("Expected ID_MOVIE %d, got %d", int32(mockRecords[i].IDMovie), val)
		}

		if val, ok := row["TITLE"]; !ok {
			t.Fatalf("Row %d does not contain column %s", i, "TITLE")
		} else if val != newTitle {
			t.Errorf("Expected TITLE `%s', got `%s'", newTitle, val)
		}
	}
}

func TestDeleteRecords(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	tableName := "MOVIES"
	expectedMockRecordsCount := 0
	expectedDeletedRecordsCount := 3

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	movieTitle := func(str string) (b [32]byte) {
		copy(b[:], str)
		return b
	}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		tableHeaderBlock
		recordBlock
	}{
		DatabaseMetadata: DatabaseMetadata{
			FirstEntryBlock: 1,
			LastEntryBlock:  1},
		tableEntryBlock: tableEntryBlock{
			Signature:    tableEntryBlockSignature,
			EntriesCount: 1,
			TableEntriesArray: tableEntries{
				tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
		},
		tableHeaderBlock: tableHeaderBlock{
			Signature:        tableHeaderBlockSignature,
			FirstRecordBlock: 3,
			ColumnCount:      2,
			TableColumnsArray: tableColumns{
				tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
				tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
			}},
		recordBlock: recordBlock{
			Signature: recordBlockSignature,
		},
	}

	mockRecords := [102]struct {
		FreeFlag uint32
		IDMovie  uint32
		Title    [32]byte
	}{
		{0, 0, movieTitle("Lord of the Rings")},
		{0, 1, movieTitle("Harry Potter")},
		{0, 2, movieTitle("Avengers")},
	}

	for i := 3; i < 102; i++ {
		mockRecords[i] = struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			FreeFlag: freeFlag,
		}
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
	if err != nil {
		t.Fatal(err)
	}

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
		t.Fatal(err)
	}

	copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	deletedRecordsCount, err := db.deleteRecords(tableName)
	if err != nil {
		t.Fatal(err)
	}

	if deletedRecordsCount != expectedDeletedRecordsCount {
		t.Fatalf("Expected to delete %d records, got %d", expectedDeletedRecordsCount, deletedRecordsCount)
	}

	resultSet, err := db.ReadTable(tableName)
	if err != nil {
		t.Fatal(err)
	}

	rows := resultSet.Rows

	if rowCount := len(rows); rowCount != expectedMockRecordsCount {
		t.Fatalf("Expected to read %d rows, got %d", expectedMockRecordsCount, rowCount)
	}
}

func TestInsertRecord(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	tableName := "MOVIES"
	expectedRecordsCount := 4
	expectedID, expectedTitle := uint32(3), "Thor Ragnarok"

	values := tableValues{
		"ID_MOVIE": expectedID,
		"TITLE":    expectedTitle,
	}

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	movieTitle := func(str string) (b [32]byte) {
		copy(b[:], str)
		return b
	}

	mockDatabase := struct {
		DatabaseMetadata
		tableEntryBlock
		tableHeaderBlock
		recordBlock
	}{
		DatabaseMetadata: DatabaseMetadata{
			FirstEntryBlock: 1,
			LastEntryBlock:  1},
		tableEntryBlock: tableEntryBlock{
			Signature:    tableEntryBlockSignature,
			EntriesCount: 1,
			TableEntriesArray: tableEntries{
				tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
		},
		tableHeaderBlock: tableHeaderBlock{
			Signature:        tableHeaderBlockSignature,
			FirstRecordBlock: 3,
			ColumnCount:      2,
			TableColumnsArray: tableColumns{
				tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
				tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
			}},
		recordBlock: recordBlock{
			Signature: recordBlockSignature,
		},
	}

	mockRecords := [102]struct {
		FreeFlag uint32
		IDMovie  uint32
		Title    [32]byte
	}{
		{0, 0, movieTitle("Lord of the Rings")},
		{0, 1, movieTitle("Harry Potter")},
		{0, 2, movieTitle("Avengers")},
	}

	for i := 3; i < 102; i++ {
		mockRecords[i] = struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			FreeFlag: freeFlag,
		}
	}

	if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
	if err != nil {
		t.Fatal(err)
	}

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
		t.Fatal(err)
	}

	copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

	if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
		t.Fatal(err)
	}

	mockFile.Close()

	db, err := LoadDatabase(filepath.Base(mockFile.Name()))
	if err != nil {
		t.Fatal(err)
	}

	tableHeaderBlock, err := db.readHeaderBlock(2)
	if err != nil {
		t.Fatal(err)
	}

	recordBlock, err := db.readRecordBlock(3)
	if err != nil {
		t.Fatal(err)
	}

	if !recordBlock.insertRecord(values.record(tableHeaderBlock.TableColumns())) {
		t.Fatal("Expected to insert record in block")
	}

	if err := db.writeRecordBlock(3, recordBlock); err != nil {
		t.Fatal(err)
	}

	resultSet, err := db.ReadTable(tableName)
	if err != nil {
		t.Fatal(err)
	}

	rows := resultSet.Rows

	if rowCount := len(rows); rowCount != expectedRecordsCount {
		t.Fatalf("Expected to read %d rows, got %d", expectedRecordsCount, rowCount)
	}

	resultID, resultTitle := rows[3]["ID_MOVIE"], rows[3]["TITLE"]
	if resultID.(int32) != int32(expectedID) {
		t.Errorf("Expected to read movie id %d, got %d", expectedID, resultID)
	}

	if resultTitle != expectedTitle {
		t.Errorf("Expected to read movie title `%s', got `%s'", expectedTitle, resultTitle)
	}
}

func TestInsert(t *testing.T) {
	databasesPath := filepath.Join(".", "databases")
	tableName := "MOVIES"
	expectedRecordsCount := 4
	expectedID, expectedTitle := uint32(3), "Thor Ragnarok"

	values := tableValues{
		"ID_MOVIE": expectedID,
		"TITLE":    expectedTitle,
	}

	name := func(str string) (b [60]byte) {
		copy(b[:], str)
		return b
	}

	movieTitle := func(str string) (b [32]byte) {
		copy(b[:], str)
		return b
	}

	t.Run("MoreThanOneAvailableSpace", func(t *testing.T) {
		mockDatabase := struct {
			DatabaseMetadata
			tableEntryBlock
			tableHeaderBlock
			recordBlocks [2]recordBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstEntryBlock: 1,
				LastEntryBlock:  1},
			tableEntryBlock: tableEntryBlock{
				Signature:    tableEntryBlockSignature,
				EntriesCount: 1,
				TableEntriesArray: tableEntries{
					tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
			},
			tableHeaderBlock: tableHeaderBlock{
				Signature:        tableHeaderBlockSignature,
				FirstRecordBlock: 3,
				ColumnCount:      2,
				TableColumnsArray: tableColumns{
					tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
					tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
				}},
			recordBlocks: [2]recordBlock{
				recordBlock{
					Signature:       recordBlockSignature,
					NextRecordBlock: 4,
					FullFlag:        fullFlag,
				},
				recordBlock{
					Signature: recordBlockSignature,
				},
			},
		}

		mockRecords := [102]struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			{0, 0, movieTitle("Lord of the Rings")},
			{0, 1, movieTitle("Harry Potter")},
			{0, 2, movieTitle("Avengers")},
		}

		padding := [102]struct {
			FreeFlag uint32
			Padding  [36]byte
		}{}

		for i := 0; i < 102; i++ {
			padding[i] = struct {
				FreeFlag uint32
				Padding  [36]byte
			}{
				FreeFlag: freeFlag,
			}
		}

		for i := 3; i < 102; i++ {
			mockRecords[i] = struct {
				FreeFlag uint32
				IDMovie  uint32
				Title    [32]byte
			}{
				FreeFlag: freeFlag,
			}
		}

		if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
			t.Fatal(err)
		}

		mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
		if err != nil {
			t.Fatal(err)
		}

		buffer := bytes.NewBuffer(nil)
		if err := binary.Write(buffer, binary.LittleEndian, padding); err != nil {
			t.Fatal(err)
		}
		copy(mockDatabase.recordBlocks[0].Data[:], buffer.Bytes())

		buffer = bytes.NewBuffer(nil)
		if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
			t.Fatal(err)
		}
		copy(mockDatabase.recordBlocks[1].Data[:], buffer.Bytes())

		if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
			t.Fatal(err)
		}

		mockFile.Close()

		db, err := LoadDatabase(filepath.Base(mockFile.Name()))
		if err != nil {
			t.Fatal(err)
		}

		if err := db.Insert(tableName, values); err != nil {
			t.Fatal(err)
		}

		resultSet, err := db.ReadTable(tableName)
		if err != nil {
			t.Fatal(err)
		}

		rows := resultSet.Rows

		if rowCount := len(rows); rowCount != expectedRecordsCount {
			t.Fatalf("Expected to read %d rows, got %d", expectedRecordsCount, rowCount)
		}

		resultID, resultTitle := rows[3]["ID_MOVIE"], rows[3]["TITLE"]
		if resultID.(int32) != int32(expectedID) {
			t.Errorf("Expected to read movie id %d, got %d", expectedID, resultID)
		}

		if resultTitle != expectedTitle {
			t.Errorf("Expected to read movie title `%s', got `%s'", expectedTitle, resultTitle)
		}
	})

	t.Run("OneAvailableSpace", func(t *testing.T) {
		expectedRecordsCount := 102

		mockDatabase := struct {
			DatabaseMetadata
			tableEntryBlock
			tableHeaderBlock
			recordBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstEntryBlock: 1,
				LastEntryBlock:  1},
			tableEntryBlock: tableEntryBlock{
				Signature:    tableEntryBlockSignature,
				EntriesCount: 1,
				TableEntriesArray: tableEntries{
					tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
			},
			tableHeaderBlock: tableHeaderBlock{
				Signature:        tableHeaderBlockSignature,
				FirstRecordBlock: 3,
				ColumnCount:      2,
				TableColumnsArray: tableColumns{
					tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
					tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
				}},
			recordBlock: recordBlock{
				Signature: recordBlockSignature,
			},
		}

		mockRecords := [102]struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			{0, 0, movieTitle("Lord of the Rings")},
			{0, 1, movieTitle("Harry Potter")},
			{0, 2, movieTitle("Avengers")},
		}

		for i := 3; i < 101; i++ {
			mockRecords[i] = struct {
				FreeFlag uint32
				IDMovie  uint32
				Title    [32]byte
			}{
				IDMovie: uint32(i),
				Title:   movieTitle("No title"),
			}
		}

		mockRecords[101] = struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			FreeFlag: freeFlag,
		}

		if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
			t.Fatal(err)
		}

		mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
		if err != nil {
			t.Fatal(err)
		}

		buffer := bytes.NewBuffer(nil)
		if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
			t.Fatal(err)
		}

		copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

		if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
			t.Fatal(err)
		}

		mockFile.Close()

		db, err := LoadDatabase(filepath.Base(mockFile.Name()))
		if err != nil {
			t.Fatal(err)
		}

		if err := db.Insert(tableName, values); err != nil {
			t.Fatal(err)
		}

		recordBlock, err := db.readRecordBlock(3)
		if err != nil {
			t.Fatal(err)
		}

		if recordBlock.FullFlag != fullFlag {
			t.Error("Expected record block to be marked as full")
		}

		resultSet, err := db.ReadTable(tableName)
		if err != nil {
			t.Fatal(err)
		}

		rows := resultSet.Rows

		if rowCount := len(rows); rowCount != expectedRecordsCount {
			t.Fatalf("Expected to read %d rows, got %d", expectedRecordsCount, rowCount)
		}

		resultID, resultTitle := rows[101]["ID_MOVIE"], rows[101]["TITLE"]
		if resultID.(int32) != int32(expectedID) {
			t.Errorf("Expected to read movie id %d, got %d", expectedID, resultID)
		}

		if resultTitle != expectedTitle {
			t.Errorf("Expected to read movie title `%s', got `%s'", expectedTitle, resultTitle)
		}
	})

	t.Run("NoAvailableSpace", func(t *testing.T) {
		expectedBlockCount := uint32(4)
		expectedNextRecordBlock := Address(4)

		mockDatabase := struct {
			DatabaseMetadata
			tableEntryBlock
			tableHeaderBlock
			recordBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstEntryBlock: 1,
				LastEntryBlock:  1,
				BlockCount:      3,
			},
			tableEntryBlock: tableEntryBlock{
				Signature:    tableEntryBlockSignature,
				EntriesCount: 1,
				TableEntriesArray: tableEntries{
					tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
			},
			tableHeaderBlock: tableHeaderBlock{
				Signature:        tableHeaderBlockSignature,
				FirstRecordBlock: 3,
				ColumnCount:      2,
				TableColumnsArray: tableColumns{
					tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
					tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
				}},
			recordBlock: recordBlock{
				Signature: recordBlockSignature,
				FullFlag:  fullFlag,
			},
		}

		mockRecords := [102]struct {
			FreeFlag uint32
			IDMovie  uint32
			Title    [32]byte
		}{
			{0, 0, movieTitle("Lord of the Rings")},
			{0, 1, movieTitle("Harry Potter")},
			{0, 2, movieTitle("Avengers")},
		}

		for i := 3; i < 102; i++ {
			mockRecords[i] = struct {
				FreeFlag uint32
				IDMovie  uint32
				Title    [32]byte
			}{
				FreeFlag: freeFlag,
			}
		}

		if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
			t.Fatal(err)
		}

		mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(mockFile.Name())

		buffer := bytes.NewBuffer(nil)
		if err := binary.Write(buffer, binary.LittleEndian, mockRecords); err != nil {
			t.Fatal(err)
		}

		copy(mockDatabase.recordBlock.Data[:], buffer.Bytes())

		if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
			t.Fatal(err)
		}

		mockFile.Close()

		db, err := LoadDatabase(filepath.Base(mockFile.Name()))
		if err != nil {
			t.Fatal(err)
		}

		if err := db.Insert(tableName, values); err != nil {
			t.Fatal(err)
		}

		recordBlock, err := db.readRecordBlock(3)
		if err != nil {
			t.Fatal(err)
		}

		if recordBlock.NextRecordBlock != expectedNextRecordBlock {
			t.Errorf("Expected record block to point to new record block %d, got %d", expectedNextRecordBlock, recordBlock.NextRecordBlock)
		}

		if db.DatabaseMetadata.BlockCount != expectedBlockCount {
			t.Fatalf("Expected database to have %d blocks, got %d", expectedBlockCount, db.DatabaseMetadata.BlockCount)
		}

		resultSet, err := db.ReadTable(tableName)
		if err != nil {
			t.Fatal(err)
		}

		rows := resultSet.Rows

		if rowCount := len(rows); rowCount != expectedRecordsCount {
			t.Fatalf("Expected to read %d rows, got %d", expectedRecordsCount, rowCount)
		}

		resultID, resultTitle := rows[3]["ID_MOVIE"], rows[3]["TITLE"]
		if resultID.(int32) != int32(expectedID) {
			t.Errorf("Expected to read movie id %d, got %d", expectedID, resultID)
		}

		if resultTitle != expectedTitle {
			t.Errorf("Expected to read movie title `%s', got `%s'", expectedTitle, resultTitle)
		}
	})

	t.Run("NoRecordBlocks", func(t *testing.T) {
		expectedBlockCount := uint32(3)
		expectedFirstRecordBlock := Address(3)
		expectedRecordsCount := 1

		mockDatabase := struct {
			DatabaseMetadata
			tableEntryBlock
			tableHeaderBlock
		}{
			DatabaseMetadata: DatabaseMetadata{
				FirstEntryBlock: 1,
				LastEntryBlock:  1,
				BlockCount:      2,
			},
			tableEntryBlock: tableEntryBlock{
				Signature:    tableEntryBlockSignature,
				EntriesCount: 1,
				TableEntriesArray: tableEntries{
					tableEntry{HeaderBlock: 2, TableNameArray: name(tableName)}},
			},
			tableHeaderBlock: tableHeaderBlock{
				Signature:   tableHeaderBlockSignature,
				ColumnCount: 2,
				TableColumnsArray: tableColumns{
					tableColumn{ColumnNameArray: name("ID_MOVIE"), DataType: integer},
					tableColumn{ColumnNameArray: name("TITLE"), DataType: char, Size: 32},
				},
			},
		}

		if err := os.MkdirAll(databasesPath, os.ModePerm); err != nil {
			t.Fatal(err)
		}

		mockFile, err := os.Create(filepath.Join("databases", "mock.db"))
		if err != nil {
			t.Fatal(err)
		}

		if err := binary.Write(mockFile, binary.LittleEndian, mockDatabase); err != nil {
			t.Fatal(err)
		}

		mockFile.Close()

		db, err := LoadDatabase(filepath.Base(mockFile.Name()))
		if err != nil {
			t.Fatal(err)
		}

		if err := db.Insert(tableName, values); err != nil {
			t.Fatal(err)
		}

		tableHeaderBlock, err := db.readHeaderBlock(2)
		if err != nil {
			t.Fatal(err)
		}

		if tableHeaderBlock.FirstRecordBlock != expectedFirstRecordBlock {
			t.Fatalf("Expected table header block to point to record block %d, got %d", expectedFirstRecordBlock, tableHeaderBlock.FirstRecordBlock)
		}

		if db.DatabaseMetadata.BlockCount != expectedBlockCount {
			t.Fatalf("Expected database to have %d blocks, got %d", expectedBlockCount, db.DatabaseMetadata.BlockCount)
		}

		resultSet, err := db.ReadTable(tableName)
		if err != nil {
			t.Fatal(err)
		}

		rows := resultSet.Rows

		if rowCount := len(rows); rowCount != expectedRecordsCount {
			t.Fatalf("Expected to read %d rows, got %d", expectedRecordsCount, rowCount)
		}

		resultID, resultTitle := rows[0]["ID_MOVIE"], rows[0]["TITLE"]
		if resultID.(int32) != int32(expectedID) {
			t.Errorf("Expected to read movie id %d, got %d", expectedID, resultID)
		}

		if resultTitle != expectedTitle {
			t.Errorf("Expected to read movie title `%s', got `%s'", expectedTitle, resultTitle)
		}
	})
}
