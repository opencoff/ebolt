// ebolt_test.go -- test harness

package ebolt_test

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha3"
	"fmt"
	"math/rand/v2"
	"path"
	"path/filepath"
	"testing"

	"github.com/opencoff/ebolt"
)

func newBolt(fn string, pw string) (ebolt.DB, error) {
	var key [32]byte

	if len(pw) == 0 {
		crand.Read(key[:])
	} else {
		h := sha3.New256()
		h.Write([]byte(pw))
		h.Sum(key[:0])
	}

	return ebolt.Open(fn, key[:], nil)
}

func TestSetGetOne(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "t.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "boltdb: %s", err)

	k := "a/b/001"
	v := make([]byte, 64)

	err = db.Set(k, v)
	assert(err == nil, "set: %s: %s", k, err)

	z, err := db.Get(k)
	assert(err == nil, "get: %s: %s", k, err)
	assert(z != nil, "get: %s: Value NIL", k)
	assert(bytes.Equal(v, z), "get: %s: Content mismatch", k)
}

func TestSetGetAll(t *testing.T) {
	assert := newAsserter(t)

	paths := []string{
		"a/001",
		"a/002",
		"a/003",
		"a/004",
	}

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "t.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "boltdb: %s", err)

	m := make(map[string][]byte)
	for _, nm := range paths {
		m[nm] = randbytes()
	}

	for k, v := range m {
		err := db.Set(k, v)
		assert(err == nil, "set: %s: %s", k, err)
	}

	for k, v := range m {
		z, err := db.Get(k)
		assert(err == nil, "get: %s: %s", k, err)
		assert(z != nil, "get: %s: Value NIL", k)
		assert(bytes.Equal(v, z), "get: %s: Content mismatch", k)
	}

	// test All()
	m2, err := db.All("a")
	assert(err == nil, "All: %s", err)
	done := make(map[string]bool)
	for k, v := range m {
		v2, ok := m2[k]
		assert(ok, "%s: expected to find %s, saw %s", k, fn, v2)
		assert(bytes.Equal(v2, v), "all: %s: content mismatch", k)
		done[k] = true
	}
	for k := range m2 {
		assert(done[k], "all: %s: unknown elem", k)
	}

	// delete a few
	for i := range paths {
		nm := paths[i]
		if i&1 > 0 {
			err := db.Del(nm)
			assert(err == nil, "del: %s: %s", nm, err)

			v, err := db.Get(nm)
			assert(err == nil, "get: %s: %s", nm, err)
			assert(v == nil, "get: %s: expected to be deleted", nm)
		}
	}
}

func update(m map[string]map[string]bool, dn string) map[string]map[string]bool {
	orig := dn
	for {
		dir := filepath.Dir(dn)
		fn := filepath.Base(dn)
		if dir == "." || dir == "/" || dir == orig {
			break
		}
		z, ok := m[dir]
		if !ok {
			z = make(map[string]bool)
		}
		z[fn] = true
		m[dir] = z
		dn = dir
	}
	return m
}

func pr0(m map[string]bool, ind string) {
	for k := range m {
		fmt.Printf("%s%s:\n", ind, k)
	}
}

func pr(m map[string]map[string]bool, ind string) {
	for k, v := range m {
		fmt.Printf("%s%s:\n", ind, k)
		pr0(v, ind+"   ")
	}
}

func TestDir(t *testing.T) {
	assert := newAsserter(t)

	paths := []string{
		"a/b/c/001",
		"a/b/c/002",
		"a/c/c/001",
		"a/c/c/002",
		"a/d/c/001",
		"a/d/c/002",
	}

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "t.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "boltdb: %s", err)

	m := make(map[string][]byte)
	subdirs := make(map[string]map[string]bool)
	for _, nm := range paths {
		m[nm] = randbytes()
		subdirs = update(subdirs, nm)
	}

	for k, v := range m {
		err := db.Set(k, v)
		assert(err == nil, "set: %s: %s", k, err)
	}

	dirs, err := db.Dir("a/b")
	assert(err == nil, "Dir: %s", err)

	want := subdirs["a/b"]
	for _, nm := range dirs {
		assert(want[nm], "missing %s", nm)
	}

}

func TestMany(t *testing.T) {
	assert := newAsserter(t)

	paths := []string{
		"a/b/001",
		"a/b/002",
		"a/b/003",
		"a/b/004",
		"a/b/005",
		"a/b/006",
	}

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "t.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "boltdb: %s", err)

	m := make(map[string]string)
	kv := make([]ebolt.KV, 0, len(paths))
	for _, nm := range paths {
		bn := filepath.Base(nm)
		x := ebolt.KV{
			Key: nm,
			Val: []byte(bn),
		}
		kv = append(kv, x)
		m[nm] = nm
	}

	err = db.SetMany(kv)
	assert(err == nil, "set-many: %s", err)

	keys, err := db.AllKeys("a/b")
	assert(err == nil, "allKeys: %s", err)
	rev := make(map[string]bool)
	for _, nm := range keys {
		_, ok := m[nm]
		assert(ok, "allkeys: %s not found", nm)
		rev[nm] = true
	}

	for k := range m {
		_, ok := rev[k]
		assert(ok, "allKeys: rev %s not found", k)
	}

	// delete many
	err = db.DelMany(paths)
	assert(err == nil, "del-many: %s", err)

	// now these shouldn't exist
	for _, nm := range paths {
		v, err := db.Get(nm)
		assert(err == nil, "get-after-del: %s", err)
		assert(len(v) == 0, "get-after-del: found %s: %x\n", nm, v)
	}
}

func randbytes() []byte {
	n := rand.N[int](256) + 32
	b := make([]byte, n)

	crand.Read(b)
	return b
}
