package data

type bitmap []byte

func newBitmap(length int) bitmap {
	if length <= 0 {
		return []byte{}
	}

	return make([]byte, ((length-1)/8)+1)
}

func (bm bitmap) At(i uint) bool {
	return (bm[int(i)/bm.length()] & (1 << (i % uint(bm.length())))) != 0
}

func (bm *bitmap) Set(i uint) {
	(*bm)[int(i)/bm.length()] |= (1 << (i % uint(bm.length())))
}

func (bm *bitmap) Clear(i uint) {
	(*bm)[int(i)/bm.length()] &^= (1 << (i % uint(bm.length())))
}

func (bm bitmap) length() int {
	return len(bm) * 8
}
