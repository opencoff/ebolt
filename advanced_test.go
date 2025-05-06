// advanced_test.go -- additional test coverage for ebolt

package ebolt_test

import (
	"bytes"
	crand "crypto/rand"
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test transaction commit and rollback
func TestTransactionCommitRollback(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "tx.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "boltdb: %s", err)
	defer db.Close()

	// Test commit
	tx, err := db.BeginTransaction(true)
	assert(err == nil, "begin tx: %s", err)

	k := "tx/commit/key"
	v := []byte("commit-value")

	err = tx.Set(k, v)
	assert(err == nil, "tx set: %s", err)

	// Commit the transaction
	err = tx.Commit()
	assert(err == nil, "tx commit: %s", err)

	// Verify the value was committed
	val, err := db.Get(k)
	assert(err == nil, "get after commit: %s", err)
	assert(bytes.Equal(val, v), "value mismatch after commit")

	// Test rollback
	tx, err = db.BeginTransaction(true)
	assert(err == nil, "begin tx: %s", err)

	k2 := "tx/rollback/key"
	v2 := []byte("rollback-value")

	err = tx.Set(k2, v2)
	assert(err == nil, "tx set: %s", err)

	// Rollback the transaction
	err = tx.Rollback()
	assert(err == nil, "tx rollback: %s", err)

	// Verify the value was not committed
	val, err = db.Get(k2)
	assert(err != nil, "found rolled-back bucket %s", k2)
	assert(val == nil, "value should be nil after rollback")
}

// Test concurrent transactions
func TestConcurrentTransactions(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "concurrent.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "boltdb: %s", err)
	defer db.Close()

	// Setup some initial data
	err = db.Set("concurrent/key", []byte("initial"))
	assert(err == nil, "initial set: %s", err)

	// Test that multiple read transactions can run concurrently
	var wg sync.WaitGroup
	readErrors := make(chan error, 3)

	// Start 3 concurrent read transactions
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Begin a read transaction
			tx, err := db.BeginTransaction(false)
			if err != nil {
				readErrors <- err
				return
			}

			// Read the value
			val, err := tx.Get("concurrent/key")
			if err != nil {
				tx.Rollback()
				readErrors <- err
				return
			}

			if !bytes.Equal(val, []byte("initial")) {
				tx.Rollback()
				readErrors <- errors.New("unexpected value")
				return
			}

			// Small delay to simulate work
			time.Sleep(5 * time.Millisecond)

			// Close the transaction
			tx.Rollback()
		}(i)
	}

	// Wait for all readers to finish
	wg.Wait()

	// Check for any errors
	select {
	case err := <-readErrors:
		t.Fatalf("Read transaction error: %v", err)
	default:
		// No errors
	}

	// Now test a write transaction
	txWrite, err := db.BeginTransaction(true)
	assert(err == nil, "begin write tx: %s", err)

	// Write a new value
	err = txWrite.Set("concurrent/key", []byte("updated"))
	assert(err == nil, "write tx set: %s", err)

	// Commit the write transaction
	err = txWrite.Commit()
	assert(err == nil, "write tx commit: %s", err)

	// Verify the value was updated
	val, err := db.Get("concurrent/key")
	assert(err == nil, "get after write: %s", err)
	assert(bytes.Equal(val, []byte("updated")), "value not updated correctly")
}

// Test encryption verification
func TestEncryptionVerification(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "encrypt.db")

	// Create a database with a specific key
	db1, err := newBolt(fn, "key0")
	assert(err == nil, "open db1: %s", err)

	// Store a value
	testKey := "secret/data"
	testValue := []byte("highly confidential information")
	err = db1.Set(testKey, testValue)
	assert(err == nil, "set: %s", err)
	db1.Close()

	// Try to open with a different key - should be able to open but data should be unreadable
	db2, err := newBolt(fn, "other key")
	assert(err == nil, "open db2: %s", err)

	// Attempt to read the value with the wrong key
	val, err := db2.Get(testKey)
	// Either we get an error or nil value, but we shouldn't get the correct data
	assert(val == nil || !bytes.Equal(val, testValue), "encryption failed: data readable with wrong key")
	db2.Close()

	// Reopen with the correct key
	db3, err := newBolt(fn, "key0")
	assert(err == nil, "reopen db: %s", err)
	defer db3.Close()

	// Should be able to read the value
	val, err = db3.Get(testKey)
	assert(err == nil, "get with correct key: %s", err)
	assert(bytes.Equal(val, testValue), "value mismatch with correct key")

	// Verify the file contains encrypted data by reading it directly
	fileData, err := os.ReadFile(fn)
	assert(err == nil, "read file: %s", err)

	// The raw file should not contain our plaintext
	assert(!bytes.Contains(fileData, testValue), "plaintext found in database file")
}

// Test backup and restore
func TestBackupRestore(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	srcFn := path.Join(tmp, "source.db")
	backupFn := path.Join(tmp, "backup.db")

	// Create and populate source database
	db, err := newBolt(srcFn, "Password")
	assert(err == nil, "open source db: %s", err)

	// Add some data
	testData := map[string][]byte{
		"backup/key1":        []byte("value1"),
		"backup/key2":        []byte("value2"),
		"backup/nested/key3": []byte("value3"),
	}

	for k, v := range testData {
		err = db.Set(k, v)
		assert(err == nil, "set %s: %s", k, err)
	}

	// Create a backup
	backupFile, err := os.Create(backupFn)
	assert(err == nil, "create backup file: %s", err)

	bytesWritten, err := db.Backup(backupFile)
	assert(err == nil, "backup: %s", err)
	assert(bytesWritten > 0, "backup wrote 0 bytes")
	backupFile.Close()
	db.Close()

	// Open the backup file with the same key
	backupDb, err := newBolt(backupFn, "Password")
	assert(err == nil, "open backup db: %s", err)
	defer backupDb.Close()

	// Verify all data is present in the backup
	for k, v := range testData {
		val, err := backupDb.Get(k)
		assert(err == nil, "get from backup %s: %s", k, err)
		assert(bytes.Equal(val, v), "backup value mismatch for %s", k)
	}

	// Test backup during active operations
	activeDb, err := newBolt(srcFn, "Password")
	assert(err == nil, "open active db: %s", err)
	defer activeDb.Close()

	// Start a transaction but don't commit yet
	tx, err := activeDb.BeginTransaction(true)
	assert(err == nil, "begin tx: %s", err)

	// Set a new value in the transaction
	err = tx.Set("backup/during/tx", []byte("tx-value"))
	assert(err == nil, "tx set: %s", err)

	// Create another backup during the active transaction
	backupFile2, err := os.Create(backupFn + ".2")
	assert(err == nil, "create backup file 2: %s", err)
	defer backupFile2.Close()

	// This should succeed even with an active transaction
	_, err = activeDb.Backup(backupFile2)
	assert(err == nil, "backup during tx: %s", err)

	// Now commit the transaction
	err = tx.Commit()
	assert(err == nil, "tx commit: %s", err)
}

// Test error handling
func TestErrorHandling(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "errors.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "open db: %s", err)
	defer db.Close()

	// Test non-existent bucket
	val, err := db.Get("nonexistent/path")
	// The error behavior depends on the implementation
	// Some implementations return an error, others return nil value
	// We just check that we don't get a valid value
	assert(val == nil, "get nonexistent should return nil")

	// Test path with special handling - empty path might be handled specially
	// by the implementation, so we'll use a different test case
	err = db.Set(".", []byte("value"))
	assert(err == nil, "root path error: %s", err)

	// Test getting all keys from non-existent bucket
	_, err = db.All("nonexistent")
	assert(err != nil, "all nonexistent should error")

	// Test getting all keys from non-existent bucket
	_, err = db.AllKeys("nonexistent")
	assert(err != nil, "allkeys nonexistent should error")

	// Test getting dir from non-existent bucket
	_, err = db.Dir("nonexistent")
	assert(err != nil, "dir nonexistent should error")
}

// Test edge cases
func TestEdgeCases(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "edge.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "open db: %s", err)
	defer db.Close()

	// Test empty value
	err = db.Set("edge/empty", []byte{})
	assert(err == nil, "set empty: %s", err)

	val, err := db.Get("edge/empty")
	assert(err == nil, "get empty: %s", err)
	assert(len(val) == 0, "empty value should have length 0")

	// Test very large value
	largeVal := make([]byte, 1024*1024) // 1MB
	crand.Read(largeVal)
	err = db.Set("edge/large", largeVal)
	assert(err == nil, "set large: %s", err)

	val, err = db.Get("edge/large")
	assert(err == nil, "get large: %s", err)
	assert(bytes.Equal(val, largeVal), "large value mismatch")

	// Test very long path
	longPath := strings.Repeat("a/", 10) + "key"
	err = db.Set(longPath, []byte("long-path-value"))
	assert(err == nil, "set long path: %s", err)

	val, err = db.Get(longPath)
	assert(err == nil, "get long path: %s", err)
	assert(bytes.Equal(val, []byte("long-path-value")), "long path value mismatch")

	// Test special characters in path
	specialPath := "special/!@#$%^&*()/key"
	err = db.Set(specialPath, []byte("special-value"))
	assert(err == nil, "set special path: %s", err)

	val, err = db.Get(specialPath)
	assert(err == nil, "get special path: %s", err)
	assert(bytes.Equal(val, []byte("special-value")), "special path value mismatch")
}

// Test using a closed database
func TestClosedDB(t *testing.T) {
	assert := newAsserter(t)

	tmp := getTmpdir(t)
	fn := path.Join(tmp, "closed.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "open db: %s", err)

	// Close the database
	err = db.Close()
	assert(err == nil, "close: %s", err)

	// Operations on closed database should fail
	_, err = db.Get("key")
	assert(err != nil, "get on closed db should error")

	err = db.Set("key", []byte("value"))
	assert(err != nil, "set on closed db should error")

	_, err = db.BeginTransaction(true)
	assert(err != nil, "begin tx on closed db should error")
}

// Benchmark basic operations
func BenchmarkBasicOperations(b *testing.B) {
	assert := newBenchAsserter(b)

	tmp := b.TempDir()
	fn := path.Join(tmp, "bench.db")

	db, err := newBolt(fn, "")
	assert(err == nil, "open db: %s", err)
	defer db.Close()

	// Setup
	key := "bench/key"
	value := make([]byte, 1024) // 1KB value
	crand.Read(value)

	// Benchmark Set
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k := key + string(rune(i%100))
			err := db.Set(k, value)
			if err != nil {
				b.Fatalf("set: %s", err)
			}
		}
	})

	// Benchmark Get
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k := key + string(rune(i%100))
			_, err := db.Get(k)
			if err != nil {
				b.Fatalf("get: %s", err)
			}
		}
	})

	// Benchmark Transaction
	b.Run("Transaction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginTransaction(true)
			if err != nil {
				b.Fatalf("begin tx: %s", err)
			}
			k := "bench/tx/" + string(rune(i%100))
			err = tx.Set(k, value)
			if err != nil {
				b.Fatalf("tx set: %s", err)
			}
			err = tx.Commit()
			if err != nil {
				b.Fatalf("tx commit: %s", err)
			}
		}
	})
}

// Helper function to clear a byte slice
func clear(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
