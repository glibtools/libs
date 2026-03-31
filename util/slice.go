package util

type Slice[T any] []T

// Find returns the first item that matches the predicate.
func (s Slice[T]) Find(predicate func(T, int) bool) (result T, found bool) {
	for i, item := range s {
		if predicate(item, i) {
			return item, true
		}
	}
	return
}

// FindIndex returns the index of the first item that matches the predicate.or -1 if no item matches.
func (s Slice[T]) FindIndex(predicate func(T, int) bool) int {
	for i, item := range s {
		if predicate(item, i) {
			return i
		}
	}
	return -1
}

func (s Slice[T]) Len() int { return len(s) }

// MustFind returns the first item that matches the predicate.
// If no item is found, it panics.
func (s Slice[T]) MustFind(predicate func(T, int) bool) T {
	item, found := s.Find(predicate)
	if !found {
		panic("item not found")
	}
	return item
}

func Contains[T comparable](a []T, x T) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func FromSlice[T any](items []T) Slice[T] { return items }
