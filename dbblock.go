package data

import "encoding/binary"

type dbBlock []byte

func (b dbBlock) nextBlock() int64 {
	return int64(binary.LittleEndian.Uint64(b[:8]))
}

func (b dbBlock) putNextBlock(addr int64) {
	binary.LittleEndian.PutUint64(b[:8], uint64(addr))
}
