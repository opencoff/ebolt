// bolt_tx.go - boltdb transactions

package ebolt

import (
	"fmt"
	"io"
	"strings"

	bolt "go.etcd.io/bbolt"
)

type xact struct {
	*bolt.Tx
	errs []error
	c    *encryptor
}

var _ Tx = &xact{}

// create a new xact instance and record the encryptor
func (b *bdb) beginXact(wr bool) (*xact, error) {
	tx, err := b.db.Begin(wr)
	if err != nil {
		return nil, &StorageError{"begin-tx", "", err}
	}

	t := &xact{
		Tx: tx,
		c:  b.c,
	}
	return t, nil
}

func (t *xact) Commit() error {
	return t.Tx.Commit()
}

func (t *xact) Rollback() error {
	return t.Tx.Rollback()
}

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
	z := make([][]byte, len(v))
	for i := range v {
		z[i] = t.c.encSegment(v[i])
	}
	return z
}

// given a path to a leaf-node (the "K" in KV) - return the intermediate
// buckets and encrypted leaf
func (t *xact) leaf2bucket(p string) (*bolt.Bucket, []byte) {
	v := splitLeaf(p)
	z := t.encPath(v)
	n := len(z)

	nm, z := z[n-1], z[:n-1]

	bu := t.Bucket(z[0])
	if bu == nil {
		return nil, nil
	}
	for _, x := range z[1:] {
		if bu = bu.Bucket(x); bu == nil {
			return nil, nil
		}
	}
	return bu, nm
}

// given a path to a leaf-node (the "K" in KV) - make the intermediate
// buckets and return encrypted leaf name
func (t *xact) mkleaf2bucket(p string) (*bolt.Bucket, []byte, error) {
	v := splitLeaf(p)
	z := t.encPath(v)
	n := len(z)
	nm, z := z[n-1], z[:n-1]

	bu, err := t.CreateBucketIfNotExists(z[0])
	if err != nil {
		return nil, nil, &StorageError{"new-bucket", p, err}
	}

	for _, x := range z[1:] {
		if bu, err = bu.CreateBucketIfNotExists(x); err != nil {
			return nil, nil, &StorageError{"new-bucket", p, err}
		}
	}
	return bu, nm, nil
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
	bu, nm := t.leaf2bucket(p)
	if bu == nil {
		return nil, &StorageError{"get", p, fmt.Errorf("bucket not found for %s", p)}
	}
	v := bu.Get(nm)
	if v == nil {
		return nil, nil
	}
	_, ret, err := t.c.decryptKV(v)
	if err != nil {
		return nil, &StorageError{"get", p, err}
	}
	return ret, nil
}

func (t *xact) Set(p string, v []byte) error {
	bu, nm, err := t.mkleaf2bucket(p)
	if err != nil {
		return &StorageError{"set", p, err}
	}
	v = t.c.encryptKV(p, v)
	if err = bu.Put(nm, v); err != nil {
		return &StorageError{"set", p, err}
	}

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

	for i := range kv {
		w := &kv[i]
		bu, nm, err := t.mkleaf2bucket(w.Key)
		if err != nil {
			return &StorageError{"set-many", w.Key, err}
		}
		v := t.c.encryptKV(w.Key, w.Val)
		if err = bu.Put(nm, v); err != nil {
			return &StorageError{"set-many", w.Key, err}
		}
	}
	return nil
}

func (t *xact) Del(p string) error {
	bu, nm := t.leaf2bucket(p)
	if bu == nil {
		return &StorageError{"del", p, fmt.Errorf("bucket not found for %s", p)}
	}

	if err := bu.Delete(nm); err != nil {
		return &StorageError{"del", p, err}
	}
	return nil
}

func (t *xact) DelMany(v []string) error {
	for _, p := range v {
		bu, nm := t.leaf2bucket(p)
		if bu == nil {
			return &StorageError{"del", p, fmt.Errorf("bucket not found for %s", p)}
		}
		if err := bu.Delete(nm); err != nil {
			return &StorageError{"del", p, err}
		}
	}
	return nil
}

func (t *xact) All(p string) (map[string][]byte, error) {
	ret := make(map[string][]byte)
	bu := t.dir2bucket(p)
	if bu == nil {
		return nil, &StorageError{"all", p, fmt.Errorf("bucket not found")}
	}
	err := bu.ForEach(func(_, v []byte) error {
		nm, v, err := t.c.decryptKV(v)
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
	err := bu.ForEach(func(_, v []byte) error {
		nm, _, err := t.c.decryptKV(v)
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
	err := bu.ForEachBucket(func(k []byte) error {
		nm, err := t.c.decSegment(k)
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

func (t *xact) backup(wr io.Writer) (int64, error) {
	return t.WriteTo(wr)
}
