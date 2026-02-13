// bolt_tx.go - boltdb transactions

package ebolt

import (
	"fmt"
	"io"
	"strings"

	bolt "go.etcd.io/bbolt"
)

type journalOpType uint8

const (
	J_OP_NONE journalOpType = iota
	J_OP_SET
	J_OP_DEL
)

type op struct {
	ty   journalOpType
	path [][]byte
	leaf []byte
	val  []byte
}

type xact struct {
	*bolt.Tx

	db *bdb

	// change set: nil for RO transactions
	ops []op
}

var _ Tx = &xact{}

func splitLeaf(p string) []string {
	v := strings.Split(p, "/")
	switch len(v) {
	case 0:
		return []string{".root"}
	case 1:
		z := make([]string, 2)
		z[0] = ".root"
		z[1] = v[0]
		return z
	default:
		return v
	}
}

func splitBucket(p string) []string {
	if len(p) == 0 {
		return []string{".root"}
	}
	return strings.Split(p, "/")
}

func (t *xact) encPath(v []string) [][]byte {
	c := t.db.c
	z := make([][]byte, len(v))
	for i := range v {
		z[i] = c.encSegment(v[i])
	}
	return z
}

// Our representation of a bucket and the path to it.
type bucket struct {
	*bolt.Bucket

	// path to bucket
	path [][]byte

	// the leaf node
	leaf []byte
}

// given a path to a leaf-node (the "K" in KV) - return the intermediate
// buckets and encrypted leaf
func (t *xact) leaf2bucket(p string) *bucket {
	v := splitLeaf(p)
	z := t.encPath(v)
	n := len(z)

	nm, z := z[n-1], z[:n-1]

	bu := t.Bucket(z[0])
	if bu == nil {
		return nil
	}
	for _, x := range z[1:] {
		if bu = bu.Bucket(x); bu == nil {
			return nil
		}
	}

	b := &bucket{
		Bucket: bu,
		path:   z,
		leaf:   nm,
	}
	return b
}

// given a path to a leaf-node (the "K" in KV) - make the intermediate
// buckets and return encrypted leaf name
func (t *xact) mkleaf2bucket(p string) (*bucket, error) {
	v := splitLeaf(p)
	z := t.encPath(v)
	n := len(z)
	nm, z := z[n-1], z[:n-1]

	bu, err := t.CreateBucketIfNotExists(z[0])
	if err != nil {
		return nil, &StorageError{"new-bucket", p, err}
	}

	for _, x := range z[1:] {
		if bu, err = bu.CreateBucketIfNotExists(x); err != nil {
			return nil, &StorageError{"new-bucket", p, err}
		}
	}

	b := &bucket{
		Bucket: bu,
		path:   z,
		leaf:   nm,
	}
	return b, nil
}

// given a dir name, return the encrypted path segments
func (t *xact) dir2bucket(p string) *bolt.Bucket {
	v := splitBucket(p)
	z := t.encPath(v)

	bu := t.Bucket(z[0])
	if bu == nil {
		return nil
	}
	for _, x := range z[1:] {
		if bu = bu.Bucket(x); bu == nil {
			return nil
		}
	}
	return bu
}

func (t *xact) Get(p string) ([]byte, error) {
	b := t.leaf2bucket(p)
	if b == nil {
		return nil, &StorageError{"get", p, fmt.Errorf("bucket not found for %s", p)}
	}

	v := b.Get(b.leaf)
	if v == nil {
		return nil, nil
	}
	_, ret, err := t.db.c.decryptKV(v)
	if err != nil {
		return nil, &StorageError{"get", p, err}
	}
	return ret, nil
}

func (t *xact) Set(p string, v []byte) error {
	b, err := t.mkleaf2bucket(p)
	if err != nil {
		return &StorageError{"set", p, err}
	}
	v = t.db.c.encryptKV(p, v)

	if err = b.Put(b.leaf, v); err != nil {
		return &StorageError{"set", p, err}
	}

	t.recordSet(b, v)
	return err
}

func (t *xact) SetMany(kv []KV) error {
	if len(kv) == 0 {
		return nil
	}
	if len(kv) == 1 {
		x := &kv[0]
		return t.Set(x.Key, x.Val)
	}

	c := t.db.c
	for i := range kv {
		w := &kv[i]
		b, err := t.mkleaf2bucket(w.Key)
		if err != nil {
			return &StorageError{"set-many", w.Key, err}
		}
		v := c.encryptKV(w.Key, w.Val)
		if err = b.Put(b.leaf, v); err != nil {
			return &StorageError{"set-many", w.Key, err}
		}

		// Add to transaction
		t.recordSet(b, v)
	}
	return nil
}

func (t *xact) Del(p string) error {
	b := t.leaf2bucket(p)
	if b == nil {
		return &StorageError{"del", p, fmt.Errorf("bucket not found for %s", p)}
	}

	if err := b.Delete(b.leaf); err != nil {
		return &StorageError{"del", p, err}
	}
	t.recordDel(b)
	return nil
}

func (t *xact) DelMany(v []string) error {
	for _, p := range v {
		b := t.leaf2bucket(p)
		if b == nil {
			return &StorageError{"del", p, fmt.Errorf("bucket not found for %s", p)}
		}
		if err := b.Delete(b.leaf); err != nil {
			return &StorageError{"del", p, err}
		}
		t.recordDel(b)
	}
	return nil
}

func (t *xact) All(p string) (map[string][]byte, error) {
	ret := make(map[string][]byte)
	bu := t.dir2bucket(p)
	if bu == nil {
		return nil, &StorageError{"all", p, fmt.Errorf("bucket not found")}
	}

	c := t.db.c
	err := bu.ForEach(func(_, v []byte) error {
		nm, v, err := c.decryptKV(v)
		if err != nil {
			return &StorageError{"all", p, err}
		}
		ret[nm] = v
		return nil
	})

	return ret, err
}

func (t *xact) AllKeys(p string) ([]string, error) {
	bu := t.dir2bucket(p)
	if bu == nil {
		return nil, &StorageError{"all", p, fmt.Errorf("bucket not found")}
	}

	var keys []string
	c := t.db.c
	err := bu.ForEach(func(_, v []byte) error {
		nm, _, err := c.decryptKV(v)
		if err != nil {
			return &StorageError{"all", p, err}
		}
		keys = append(keys, nm)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (t *xact) Dir(p string) ([]string, error) {
	bu := t.dir2bucket(p)
	if bu == nil {
		return nil, &StorageError{"all", p, fmt.Errorf("bucket not found")}
	}

	var ret []string
	c := t.db.c
	err := bu.ForEachBucket(func(k []byte) error {
		nm, err := c.decSegment(k)
		if err != nil {
			return err
		}
		ret = append(ret, nm)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// -- Transaction Mgmt --

// create a new xact instance and record the encryptor
func (b *bdb) beginXact(wr bool) (*xact, error) {
	tx, err := b.db.Begin(wr)
	if err != nil {
		return nil, &StorageError{"begin-tx", "", err}
	}

	var ops []op
	if wr {
		ops = make([]op, 0, 4)
	}

	t := &xact{
		Tx:  tx,
		db:  b,
		ops: ops,
	}

	return t, nil
}

func (t *xact) Commit() error {

	if err := t.Tx.Commit(); err != nil {
		return err
	}

	// quick exit for RO transaction
	if t.ops == nil {
		return nil
	}

	// RW transaction
	//wseq := t.db.wseq.Add(1)

	// XXX Send the transaction bunch
	// Send it to a journal goroutine that will distribute to each replica
	return nil
}

func (t *xact) Rollback() error {
	if t.ops != nil {
		t.ops = t.ops[:0]
	}
	return t.Tx.Rollback()
}

func (t *xact) backup(wr io.Writer) (int64, error) {
	return t.WriteTo(wr)
}

func (t *xact) recordSet(b *bucket, v []byte) {
	o := op{
		ty:   J_OP_SET,
		path: b.path,
		leaf: b.leaf,
		val:  v,
	}

	t.ops = append(t.ops, o)
}

func (t *xact) recordDel(b *bucket) {
	o := op{
		ty:   J_OP_DEL,
		path: b.path,
		leaf: b.leaf,
	}

	t.ops = append(t.ops, o)
}
