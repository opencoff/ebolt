// bolt_tx.go - boltdb transactions

package ebolt

import (
	"fmt"
	"io"
	"path/filepath"

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

// Encrypt every segment and return the prefix and leaf.
func (t *xact) splitEncrypt(p string) ([][]byte, []byte) {
	v := filepath.SplitList(filepath.Clean(p))
	if len(v) == 1 {
		z := v[0]
		v = make([]string, 2)
		v[0] = ".root"
		v[1] = z
	}

	n := len(v)
	out := make([][]byte, n)
	for i := range n {
		out[i] = t.c.encSegment(v[i])
	}

	return out[:n-1], out[n-1]
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

func (t *xact) mkbucket(p string) (*bolt.Bucket, []byte, error) {
	v, nm := t.splitEncrypt(p)
	bu, err := t.CreateBucketIfNotExists(v[0])
	if err != nil {
		return nil, nil, &StorageError{"new-bucket", p, err}
	}

	for i := range v[1:] {
		if bu, err = bu.CreateBucketIfNotExists(v[i]); err != nil {
			return nil, nil, &StorageError{"new-bucket", p, err}
		}
	}
	return bu, nm, nil
}

func (t *xact) bucket(p string) (*bolt.Bucket, []byte) {
	v, nm := t.splitEncrypt(p)
	bu := t.Bucket(v[0])
	if bu == nil {
		return nil, nil
	}
	for i := range v[1:] {
		if bu = bu.Bucket(v[i]); bu == nil {
			return nil, nil
		}
	}
	return bu, nm
}

func (t *xact) Get(p string) ([]byte, error) {
	bu, nm := t.bucket(p)
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
	bu, nm, err := t.mkbucket(p)
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
		bu, nm, err := t.mkbucket(w.Key)
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
	bu, nm := t.bucket(p)
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
		bu, nm := t.bucket(p)
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
	bu, _ := t.bucket(p)
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
	bu, _ := t.bucket(p)
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
	bu, _ := t.bucket(p)
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
