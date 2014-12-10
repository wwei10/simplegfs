package set

import (
	"fmt"
	"testing"
)

func testContains(s Set, keys []string, want []bool, t *testing.T) {
	for i := range keys {
		got := s.Contains(keys[i])
		if got != want[i] {
			t.Error("contains", keys[i], "got", got, "want", want[i])
		}
	}
}

func TestSet(t *testing.T) {
	s := New()
	s.Add("a")
	s.Add("b")
	s.Add("c")
	containsTests := []struct {
		key  string
		want bool
	}{
		{"a", true},
		{"b", true},
		{"c", true},
		{"d", false},
		{"e", false},
	}
	for _, test := range containsTests {
		got := s.Contains(test.key)
		if got != test.want {
			t.Error("contains", test.key, "got", got, "want", test.want)
		}
	}
	fmt.Println(s)
}

func TestDelete(t *testing.T) {
	s := New()
	key := "a"
	s.Add(key)
	s.Remove(key)
	want := false
	if got := s.Contains(key); got != want {
		t.Error("delete", key, "got", got, "want", want)
	}
	fmt.Println(s)
}

func TestSlice(t *testing.T) {
	s := New()
	s.Add(1)
	s.Add(2)
	s.Add(3)
	s.Add(4)
	s.Add(5)
	if len(s.Slice()) != 5 {
		t.Error(s.Slice())
	}
	fmt.Println(s.Slice())
}
