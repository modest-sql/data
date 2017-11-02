package data

const blockSize = 4096

type block [blockSize]byte

func blockOffset(blockNo Address) int64 {
	return int64(metadataBlockSize + blockNo*blockSize)
}

func (db Database) readBlock(blockNo Address) (b block, err error) {
	if _, err = db.file.ReadAt(b[:], blockNo.offset()); err != nil {
		return b, err
	}

	return b, nil
}
