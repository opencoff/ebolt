// bolt_tx.go - boltdb transactions

package ebolt

import (
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	bolt "go.etcd.io/bbolt"
)

type xact struct {
	*bolt.Tx
	errs []error
	c    *encryptor
}

var _ Tx = &xact{}

func split(p string) (string, string) {
	dn := filepath.Dir(p)
	bn := filepath.Base(p)
	if dn == "." {
		dn = ".root"
	}
	return dn, bn
}

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

func (t *xact) mkbucket(p string) (*bolt.Bucket, error) {
	v := strings.Split(p, "/")
	if len(v) == 0 {
		return nil, nil
	}

	bu, err := t.CreateBucketIfNotExists([]byte(v[0]))
	if err != nil {
		return nil, &StorageError{"new-bucket", v[0], err}
	}

	for _, nm := range v[1:] {
		k := []byte(nm)
		nb, err := bu.CreateBucketIfNotExists(k)
		if err != nil {
			return nil, &StorageError{"new-bucket", nm, err}
		}
		bu = nb
	}
	return bu, nil
}

func (t *xact) bucket(p string) *bolt.Bucket {
	v := strings.Split(p, "/")
	if len(v) == 0 {
		return nil
	}

	bu := t.Bucket([]byte(v[0]))
	if bu == nil {
		return nil
	}
	for _, nm := range v[1:] {
		if bu = bu.Bucket([]byte(nm)); bu == nil {
			return nil
		}
	}
	return bu
}

func (t *xact) Get(p string) ([]byte, error) {
	dn, nm := split(p)
	k := t.c.encKey(nm)

	bu := t.bucket(dn)
	if bu == nil {
		return nil, &StorageError{"get", dn, fmt.Errorf("bucket not found for %s", p)}
	}
	v := bu.Get(k)
	if v == nil {
		return nil, nil
	}
	_, ret, err := t.c.decryptKV(v)
	if err != nil {
		return nil, &StorageError{"get", dn, err}
	}
	return ret, nil
}

func (t *xact) Set(p string, v []byte) error {
	dn, nm := split(p)
	k := t.c.encKey(nm)
	bu, err := t.mkbucket(dn)
	if err != nil {
		return &StorageError{"set", p, err}
	}
	v = t.c.encryptKV(nm, v)
	if err = bu.Put(k, v); err != nil {
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
		dn, nm := split(w.Key)
		bu, err := t.mkbucket(dn)
		if err != nil {
			return &StorageError{"set-many", w.Key, err}
		}
		k := t.c.encKey(nm)
		v := t.c.encryptKV(nm, w.Val)
		if err = bu.Put(k, v); err != nil {
			return &StorageError{"set-many", w.Key, err}
		}
	}
	return nil
}

func (t *xact) Del(p string) error {
	dn, nm := split(p)
	k := t.c.encKey(nm)
	bu := t.bucket(dn)
	if bu == nil {
		return &StorageError{"del", dn, fmt.Errorf("bucket not found for %s", p)}
	}

	if err := bu.Delete(k); err != nil {
		return &StorageError{"del", p, err}
	}
	return nil
}

func (t *xact) DelMany(v []string) error {
	for _, p := range v {
		dn, nm := split(p)
		bu := t.bucket(dn)
		if bu == nil {
			return &StorageError{"del", dn, fmt.Errorf("bucket not found for %s", p)}
		}
		k := t.c.encKey(nm)
		if err := bu.Delete(k); err != nil {
			return &StorageError{"del", p, err}
		}
	}
	return nil
}

// All returns the key-value pairs in the bucket 'p'.
func (t *xact) All(p string) (map[string][]byte, error) {
	ret := make(map[string][]byte)
	bu := t.bucket(p)
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

// All returns the keys pairs in the bucket 'p'.
func (t *xact) AllKeys(p string) ([]string, error) {
	bu := t.bucket(p)
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

func (t *xact) Dir(p string) ([][]byte, error) {
	bu := t.bucket(p)
	if bu == nil {
		return nil, &StorageError{"all", p, fmt.Errorf("bucket not found")}
	}

	var ret [][]byte
	err := bu.ForEachBucket(func(k []byte) error {
		ret = append(ret, slices.Clone(k))
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
