package ebolt_test

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func newAsserter(t *testing.T) func(cond bool, msg string, args ...interface{}) {
	return func(cond bool, msg string, args ...interface{}) {
		if cond {
			return
		}

		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		}

		s := fmt.Sprintf(msg, args...)
		t.Fatalf("\n%s: %d: Assertion failed: %s\n", file, line, s)
	}
}

func newBenchAsserter(b *testing.B) func(cond bool, msg string, args ...interface{}) {
	return func(cond bool, msg string, args ...interface{}) {
		if cond {
			return
		}

		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		}

		s := fmt.Sprintf(msg, args...)
		b.Errorf("\n%s: %d: Assertion failed: %s\n", file, line, s)
	}
}

var testDir = flag.String("testdir", "", "Use 'T' as the testdir for file I/O tests")

func getTmpdir(t *testing.T) string {
	assert := newAsserter(t)
	tmpdir := t.TempDir()

	if len(*testDir) > 0 {
		tmpdir = filepath.Join(*testDir, t.Name())
		err := os.MkdirAll(tmpdir, 0700)
		assert(err == nil, "mkdir %s: %s", tmpdir, err)
		t.Cleanup(func() {
			if t.Failed() {
				t.Logf("preserving %s ..\n", tmpdir)
			} else {
				os.RemoveAll(tmpdir)
			}
		})
	}
	return tmpdir
}
