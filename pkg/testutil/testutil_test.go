package testutil

import (
  "testing"
)

func TestAssertEquals(t *testing.T) {
  AssertEquals(t, 1, 1)
  AssertEquals(t, "abc", "abc")
  AssertEquals(t, true, true)
}

func TestAssertTrue(t *testing.T) {
  AssertTrue(t, 1 == 1)
  AssertTrue(t, "abc" == "abc")
}

func TestAssertFalse(t *testing.T) {
  AssertFalse(t, 1 != 1)
  AssertFalse(t, "abc" != "abc")
}
