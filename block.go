package data

const blockSize = 4096

type block [blockSize]byte

func blockOffset(blockNo uint32) int64 {
	return int64(metadataBlockSize + blockNo*blockSize)
}

func (db Database) readBlock(blockNo uint32) (b block, err error) {
	if _, err = db.file.ReadAt(b[:], blockOffset(blockNo)); err != nil {
		return b, err
	}

	return b, nil
}
