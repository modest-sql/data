package data

type tuple []tupleElement

type tupleElement struct {
	defaultSize int
	isNull      bool
	value       storable
}

func (t tuple) size() (size int) {
	for _, storable := range t {
		size += storable.size()
	}

	return size
}

func (t tuple) bytes() (bytes []byte) {
	for _, storable := range t {
		bytes = append(bytes, storable.bytes()...)
	}

	return bytes
}

func (i tupleElement) size() int {
	if i.isNull {
		return i.defaultSize
	}

	return i.value.size()
}

func (i tupleElement) bytes() []byte {
	if i.isNull {
		return make([]byte, i.defaultSize)
	}

	return i.value.bytes()
}
