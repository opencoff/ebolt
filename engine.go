// db.go -- raw k,v storage abstraction

// ebolt is an encrypted wrapper over etcd/bbolt. It also implements
// a convenient notion of "path" for the key - where each intermediate
// "dir" is a boltdb bucket. And the last part of the key-path is the
// a "key" in the last bucket.
//
// All values stored in the db are encrypted with AES-256-GCM.
// The last part of the key-path is obfuscated with its MAC.
package ebolt

import (
	"io"
)

// KV represents a "key, value" pair for storage operations
type KV struct {
	Key string
	Val []byte
}

// Ops interface defines the core operations for the encrypted database.
type Ops interface {
	// Get retrieves and decrypts the value stored at the specified path.
	// The path format "a/b/name" is interpreted where intermediate components
	// are buckets and the final component is the key.
	Get(p string) ([]byte, error)

	// Set encrypts and stores a value at the specified path, automatically
	// creating any intermediate buckets as needed. The leaf component of the
	// path is obfuscated while bucket names remain in plaintext.
	Set(p string, v []byte) error

	// SetMany encrypts and stores multiple key-value pairs. Each key follows
	// the path format with automatic bucket creation.
	SetMany(v []KV) error

	// Del removes the encrypted value at the specified path.
	Del(p string) error

	// DelMany deletes multiple keys in a single transaction.
	// Each path is processed according to the hierarchical bucket structure.
	DelMany(v []string) error

	// All retrieves all entries within a given bucket path, returning a map
	// of decrypted key-value pairs. The keys in the map are the original
	// unobfuscated keys (including their full path).
	All(p string) (map[string][]byte, error)

	// AllKeys returns all keys within a given bucket path without
	// retrieving their values. The returned keys are the original
	// unobfuscated keys (including their full path).
	AllKeys(p string) ([]string, error)

	// Dir returns all sub-buckets under the specified path without
	// retrieving individual key-value pairs. In boltdb terminology,
	// this returns all sub-buckets of a bucket.
	Dir(p string) ([]string, error)
}

// DB interface extends Ops with database management functionality
type DB interface {
	// Ops embeds all operations from the Ops interface
	Ops

	// Close finalizes all transactions and releases database resources.
	Close() error

	// BeginTransaction starts a new transaction that can be either read-only
	// or read-write. Multiple read-only transactions can run concurrently,
	// but write transactions are exclusive.
	BeginTransaction(writable bool) (Tx, error)

	// Backup performs a live backup of the encrypted database to the provided
	// io.Writer, returning the number of bytes written. The database remains
	// usable during the backup process.
	Backup(wr io.Writer) (int64, error)
}

// Tx interface represents an active transaction. This enables callers to perform
// multiple operations in 'Ops' and commit in the end or abort.
type Tx interface {
	// Ops embeds all operations from the Ops interface
	Ops

	// Commit persists all changes made within this transaction to the database.
	// After calling Commit, the transaction is no longer usable.
	Commit() error

	// Rollback discards all changes made within this transaction.
	// After calling Rollback, the transaction is no longer usable.
	Rollback() error
}
