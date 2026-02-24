// ebolt.go -- Encrypted wrapper for boltdb

package ebolt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/netip"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Options struct {
	bolt.Options
}

type PrimaryOptions struct {
	bolt.Options

	// Server's TLS Cert
	Cert *tls.Certificate

	// mTLS CA pool
	CA *x509.CertPool

	// Listen addr:port
	AddrPort netip.AddrPort

	// optional: allowed IPs to connect to this primary
	Replica []netip.Addr

	// Other options

	// How frequently do we batch writes to replicas
	BatchInterval time.Duration

	// Or How much data to accumulate before we replicate
	BatchSize int

	// # of transactions to buffer
	CommitChanSize int
}

type ReplicaOptions struct {
	bolt.Options

	// mTLS Client certificate
	Cert *tls.Certificate

	// CA roots
	CA *x509.CertPool

	// IP:Port of the primary
	Primary netip.AddrPort
}

type bdb struct {
	db *bolt.DB

	// encrypts KV
	c *encryptor

	// replica manager - can be nil
	p *primary

	// replica - can be nil
	r *replica
}

var _ DB = &bdb{}

// Create or open a new encrypted bolt db. The supplied 'key' will
// be expanded and used to encrypt the values stored in the db. The
// leaf of a key-path is obfuscated while preserving the intermediate
// paths in plaintext. This compromise gives us better performance
// without sacrificing too much privacy.
func Open(fn string, key []byte, opt *Options) (DB, error) {
	var bopt *bolt.Options

	if opt != nil {
		bopt = &opt.Options
	}

	db, err := bolt.Open(fn, 0600, bopt)
	if err != nil {
		return nil, fmt.Errorf("db %s: %w", fn, err)
	}

	c, err := newEncryptor(key)
	if err != nil {
		return nil, err
	}

	b := &bdb{
		db: db,
		c:  c,
	}

	return b, nil
}

func OpenPrimary(dn string, key []byte, opt *PrimaryOptions) (DB, error) {
	return nil, nil
}

func OpenReplica(dn string, opt *ReplicaOptions) (RDB, error) {
	return nil, nil
}

// Close finalizes all transactions and releases database resources.
func (b *bdb) Close() error {
	if err := b.db.Close(); err != nil {
		return err
	}

	if b.p != nil {
		if err := b.p.Close(); err != nil {
			return fmt.Errorf("db: primary: %w", err)
		}
	}

	if b.r != nil {
		if err := b.r.Close(); err != nil {
			return fmt.Errorf("db: replica: %w", err)
		}
	}

	return nil
}

// BeginTransaction starts a new transaction that can be either read-only
// or read-write. Multiple read-only transactions can run concurrently,
// but write transactions are exclusive.
func (b *bdb) BeginTransaction(wr bool) (Tx, error) {
	return b.beginXact(wr)
}

// Get retrieves and decrypts the value stored at the specified path.
// The path format "a/b/name" is interpreted where intermediate components
// are buckets and the final component is the key.
func (b *bdb) Get(p string) ([]byte, error) {
	tx, err := b.beginXact(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	return tx.Get(p)
}

// Set encrypts and stores a value at the specified path, automatically
// creating any intermediate buckets as needed. The leaf component of the
// path is obfuscated while bucket names remain in plaintext.
func (b *bdb) Set(p string, v []byte) error {
	tx, err := b.beginXact(true)
	if err != nil {
		return err
	}

	defer tx.Commit()
	return tx.Set(p, v)
}

// SetMany encrypts and stores multiple key-value pairs. Each key follows
// the path format with automatic bucket creation.
func (b *bdb) SetMany(kv []KV) error {
	if len(kv) == 0 {
		return nil
	}
	if len(kv) == 1 {
		x := &kv[0]
		return b.Set(x.Key, x.Val)
	}

	tx, err := b.beginXact(true)
	if err != nil {
		return err
	}

	defer tx.Commit()
	return tx.SetMany(kv)
}

// Get retrieves and decrypts the value stored at the specified path.
// The path format "a/b/name" is interpreted where intermediate components
// are buckets and the final component is the key.
func (b *bdb) Del(p string) error {
	tx, err := b.beginXact(true)
	if err != nil {
		return err
	}

	defer tx.Commit()
	return tx.Del(p)
}

// DelMany deletes multiple keys in a single transaction.
// Each path is processed according to the hierarchical bucket structure.
func (b *bdb) DelMany(v []string) error {
	tx, err := b.beginXact(true)
	if err != nil {
		return err
	}

	defer tx.Commit()
	return tx.DelMany(v)
}

// All retrieves all entries within a given bucket path, returning a map
// of decrypted key-value pairs. The keys in the map are the original
// unobfuscated key-paths.
func (b *bdb) All(p string) (map[string][]byte, error) {
	tx, err := b.beginXact(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	return tx.All(p)
}

// AllKeys returns all keys within a given bucket path without
// retrieving their values. The returned keys are the original
// unobfuscated key-paths.
func (b *bdb) AllKeys(p string) ([]string, error) {
	tx, err := b.beginXact(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	return tx.AllKeys(p)
}

// Dir returns all sub-buckets under the specified path without
// retrieving individual key-value pairs. In boltdb terminology,
// this returns all sub-buckets of a bucket.
func (b *bdb) Dir(p string) ([]string, error) {
	tx, err := b.beginXact(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()
	return tx.Dir(p)
}

// Backup performs a live backup of the encrypted database to the provided
// io.Writer, returning the number of bytes written. The database remains
// usable during the backup process.
func (b *bdb) Backup(wr io.Writer) (int64, error) {
	tx, err := b.beginXact(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	return tx.backup(wr)
}

type StorageError struct {
	Op  string
	Key string
	Err error
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("%s: <%s>: %s", e.Op, e.Key, e.Err)
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

type ReplayError struct {
	badOp op
	Err   error
}

func (e *ReplayError) Error() string {
	return fmt.Sprintf("replay: %s", e.Err)
}

func (e *ReplayError) Unwrap() error {
	return e.Err
}

var _ error = &StorageError{}
