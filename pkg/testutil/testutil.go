package testutil

import (
  "fmt"
  "reflect"
  "runtime"
  "testing"
)

const (
  END = "\033[0m" // Encodes end
  RED = "\033[91m"
)

// Returns a formatted string containing file name,
// line number and function name.
func getInfo() string {
  pc, file, line, ok := runtime.Caller(2)
  testName := ""
  if ok {
    test := runtime.FuncForPC(pc)
    if test != nil {
      testName = test.Name()
    }
  }
  return fmt.Sprintf("\nfile: %s\nline: %d\nfunction: %s\nmessage", file, line, testName)
}

// Throws an error if the type of got and the type of want mismatch
// or their values don't match.
// Note: Supported types - int, string, bool
func AssertEquals(t *testing.T, got, want interface{}) {
  context := getInfo()
  gotType := reflect.TypeOf(got)
  wantType := reflect.TypeOf(want)
  if gotType != wantType {
    t.Errorf("%s: type mismatch", context)
  }
  switch got.(type) {
  case int:
    if got.(int) != want.(int) {
      msg := fmt.Sprintf("got: '%d', want: '%d'", got.(int), want.(int))
      t.Errorf("%s: %s", inRed(context), inRed(msg))
    }
  case string:
    if got.(string) != want.(string) {
      msg := fmt.Sprintf("got: '%s', want: '%s'", got.(string), want.(string))
      t.Errorf("%s: %s", inRed(context), inRed(msg))
    }
  case bool:
    if got.(bool) != want.(bool) {
      msg := fmt.Sprintf("got: %t, want: %t", got.(bool), want.(bool))
      t.Errorf("%s: %s", inRed(context), inRed(msg))
    }
  default:
    t.Errorf("%s: %s", inRed(context), inRed("unsupported type"))
  }
}

// Throws an error if got is not true
func AssertTrue(t *testing.T, got bool) {
  context := getInfo()
  if !got {
    t.Errorf("%s: %s", inRed(context), inRed("the condition should be false"))
  }
}

// Throws an error if got is true
func AssertFalse(t *testing.T, got bool) {
  context := getInfo()
  if got {
    t.Errorf("%s: %s", inRed(context), inRed("the condition should be true"))
  }
}

func inRed(s string) string {
  return RED + s + END
}
