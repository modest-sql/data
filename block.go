package data

const blockSize = 4096

type block [blockSize]byte

func (db Database) readBlock(blockNo Address) (b block, err error) {
	if _, err = db.file.ReadAt(b[:], blockNo.offset()); err != nil {
		return b, err
	}

	return b, nil
}
