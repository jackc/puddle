package puddle

type resList[T any] []*Resource[T]

func (l *resList[T]) append(val *Resource[T]) { *l = append(*l, val) }

func (l *resList[T]) popBack() *Resource[T] {
	idx := len(*l) - 1
	val := (*l)[idx]
	(*l)[idx] = nil // Avoid memory leak
	*l = (*l)[:idx]

	return val
}

func (l *resList[T]) remove(val *Resource[T]) {
	for i, elem := range *l {
		if elem == val {
			(*l)[i] = (*l)[len(*l)-1]
			(*l)[len(*l)-1] = nil // Avoid memory leak
			(*l) = (*l)[:len(*l)-1]
			return
		}
	}

	panic("BUG: removeResource could not find res in slice")
}
