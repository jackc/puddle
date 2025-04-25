package puddle

import (
	"math/bits"
)

type ints interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

// log2Int returns log2 of an integer. This function panics if val < 0. For val
// == 0, returns 0.
func log2Int[T ints](val T) uint8 {
	if val <= 0 {
		panic("log2 of non-positive number does not exist")
	}
	return uint8(bits.Len64(uint64(val)) - 1)
}
