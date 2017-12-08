package data

type dbRecordBlock struct {
	nextRecordBlock int64
	dbRecords       []dbRecord
}

func (rb *dbRecordBlock) insertRecord(record dbRecord) bool {
	for i := range rb.dbRecords {
		if rb.dbRecords[i].isFree() {
			rb.dbRecords[i] = record
			return true
		}
	}

	return false
}

func (rb *dbRecordBlock) deleteAllRecords() {
	for i := range rb.dbRecords {
		rb.dbRecords[i].freeFlag = freeFlag
	}
}
