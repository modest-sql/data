package data

type bitmap []byte

func newBitmap(size int) bitmap {
	if size <= 0 {
		return []byte{}
	}

	return make([]byte, ((size-1)/8)+1)
}

func (bm bitmap) At(i uint) bool {
	return (bm[int(i)/len(bm)] & (1 << (i % uint(len(bm))))) != 0
}

func (bm *bitmap) Set(i uint) {
	(*bm)[int(i)/len(*bm)] |= (1 << (i % uint(len(*bm))))
}

func (bm *bitmap) Clear(i uint) {
	(*bm)[int(i)/len(*bm)] &^= (1 << (i % uint(len(*bm))))
}

func (bm bitmap) bytes() []byte {
	return bm
}

func (bm bitmap) size() int {
	return len(bm)
}
