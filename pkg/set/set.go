package set

var val = struct{}{} // Placeholder in Set implementation.

// Not thread-safe implementation of Set.
type Set struct {
	m map[interface{}]struct{} // Use map as a building block.
}

// New returns a pointer to a Set instance.
func New() *Set {
	s := &Set{
		m: make(map[interface{}]struct{}),
	}
	return s
}

// Add a new element to Set.
func (s *Set) Add(elem interface{}) {
	s.m[elem] = val
}

// Contains returns true if Set contains an element.
func (s *Set) Contains(elem interface{}) bool {
	_, ok := s.m[elem]
	return ok
}

// Remove an element from Set.
func (s *Set) Remove(elem interface{}) {
	delete(s.m, elem)
}

// Slice returns a list of interface{}
func (s *Set) Slice() []interface{} {
	slice := make([]interface{}, 0)
	for key := range s.m {
		slice = append(slice, key)
	}
	return slice
}
