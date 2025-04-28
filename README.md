# ebolt
A secure, encrypted wrapper over [bbolt](https://github.com/etcd-io/bbolt) providing hierarchical key-value storage
with transparent encryption capabilities.

## Features

- **Hierarchical Path Structure**: Use intuitive paths like "users/profiles/john"
  with automatic bucket creation for the key/value pairs.
- **Transparent Encryption**: All keys & values are encrypted and decrypted with
   AES-256-GCM.
- **Key Obfuscation**: The DB path segments are individually encrypted.
- **Transaction Support**: Full atomic operations with commit/rollback capabilities.
- **Backup Support**: Live, encrypted database backups without interrupting service.
- **Cross-Platform**: Works on Linux, macOS, and Windows.

## Installation

```bash
go get github.com/opencoff/ebolt
```

## Overview

Ebolt enhances the popular bbolt key-value store by adding:

1. **Encryption Layer**: All stored keys & values are encrypted before writing and decrypted when read
2. **Path-Based Access**: Keys are specified as paths (e.g., "users/settings/theme") where
   intermediate components become buckets
3. **Auto-Vivification**: Intermediate buckets are automatically created when setting values
4. **Simple API**: Simple API to get/set, query

This library is ideal for applications that need to store sensitive data while maintaining the performance
and simplicity of bbolt.

## Usage Examples

### Basic Operations

```go
package main

import (
    "fmt"
    "log"

    "github.com/opencoff/ebolt"
    "github.com/opencoff/go-utils"
)

func main() {
    pw, err := utils.Askpass("Enter DB Password", true)
    if err != nil {
        log.Fatal(err)
    }

    // This is just an example. For production uses, you
    // must use a strong KDF like Argon2i to derive a
    // key from the user's passphrase.

    // Open or create an encrypted database
    db, err := ebolt.Open("users.db", []byte(pw), nil)
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    // Store values with hierarchical paths (buckets auto-created)
    if err := db.Set("app/settings/theme", []byte("dark")); err != nil {
        log.Fatalf("Failed to set theme: %v", err)
    }
    
    if err := db.Set("app/settings/language", []byte("en-US")); err != nil {
        log.Fatalf("Failed to set language: %v", err)
    }

    // Retrieve a specific value
    theme, err := db.Get("app/settings/theme")
    if err != nil {
        log.Fatalf("Failed to get theme: %v", err)
    }
    fmt.Printf("Theme: %s\n", theme)

    // Get all settings
    settings, err := db.All("app/settings")
    if err != nil {
        log.Fatalf("Failed to get settings: %v", err)
    }

    fmt.Println("All settings:")
    for k, v := range settings {
        fmt.Printf("  %s: %s\n", k, v)
    }
}
```

### Using Transactions

```go
package main

import (
    "fmt"
    "log"

    "github.com/opencoff/ebolt"
    "github.com/opencoff/go-utils"
)

func main() {
    pw, err := utils.Askpass("Enter DB Password", true)
    if err != nil {
        log.Fatal(err)
    }

    // This is just an example. For production uses, you
    // must use a strong KDF like Argon2i to derive a
    // key from the user's passphrase.

    db, err := ebolt.Open("users.db", []byte(pw), nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Start a writable transaction
    tx, err := db.BeginTransaction(true)
    if err != nil {
        log.Fatal(err)
    }

    // Set multiple values atomically
    err = tx.SetMany([]ebolt.KV{
        {Key: "users/1001/name", Val: []byte("Alice Smith")},
        {Key: "users/1001/email", Val: []byte("alice@example.com")},
        {Key: "users/1001/role", Val: []byte("admin")},
    })

    if err != nil {
        tx.Rollback()
        log.Fatalf("Transaction failed: %v", err)
    }

    // Commit changes
    if err = tx.Commit(); err != nil {
        log.Fatalf("Commit failed: %v", err)
    }

    // Read the data back
    name, _ := db.Get("users/1001/name")
    fmt.Printf("User name: %s\n", name)

    // Get all keys in a bucket
    keys, err := db.AllKeys("users/1001")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("User profile fields:")
    for _, key := range keys {
        fmt.Printf("  - %s\n", key)
    }
}
```

### Database Backup

```go
package main

import (
    "log"
    "os"
    "time"

    "github.com/opencoff/ebolt"
    "github.com/opencoff/go-utils"
)

func main() {
    pw, err := utils.Askpass("Enter DB Password", true)
    if err != nil {
        log.Fatal(err)
    }

    // This is just an example. For production uses, you
    // must use a strong KDF like Argon2i to derive a
    // key from the user's passphrase.

    // Open the database
    db, err := ebolt.Open("production.db", []byte(pw), nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create a backup file
    backupFile, err := os.Create("backup-" + time.Now().Format("20060102") + ".db")
    if err != nil {
        log.Fatal(err)
    }
    defer backupFile.Close()

    // Perform live backup
    bytes, err := db.Backup(backupFile)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Backup completed successfully: %d bytes written", bytes)
}
```

## Interface Documentation

### Types and Interfaces

```go
// KV represents a key-value pair for storage operations
type KV struct {
    Key string
    Val []byte
}

// Ops interface defines the core operations for the encrypted database
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
    Dir(p string) ([][]byte, error)
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

// Tx interface represents an active transaction
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
```

### Database Encryption Keys
If your db encryption key is already part of some KMS regime or a previous HKDF-like key
expansion, then it's safe to use with `ebolt.Open()`.

Please DO NOT use string passwords as the input to "ebolt.Open()". This is a terrible idea.
Consult your favorite cryptographer to safely convert a string passphrase into usable
key material. I tend to use the following construct to generate a 32-byte key.

```
    salt = randombytes(32)
    key  = argon2id(32, passphrase, salt, Time, Mem, Par)
```
Of course, one has to store "salt" safely in some place. And choose "Time", "Mem", "Par" to
account for your security needs.

## Implementation Notes

- The encryption is applied only to the values stored in the database, not to the database
  file itself.
- Only leaf keys are obfuscated, keeping bucket names readable for easier debugging and navigation.
- Performance impact of encryption is expected to be minimal for most use cases.

### Cryptography
`cipher.go` implements the necessary cryptography. The user provided key is expanded with domain
separation into two keys and a nonce. Each of the keys is used to construct an AEAD for keys and
values respectively. Each segment of the path is encrypted with a common nonce, while the values
all get unique, random nonces. In pseudo code:

```
    keymat = HKDF-expand(master_key, "AES Keys and Nonce")
    key_k, keymat = keymat[:32], keymat[32:]
    val_k, keymat = keymat[:32], keymat[32:]
    nonce = keymat

    key_cipher = aes_256_GCM(key_k)
    val_cipher = aes_256_GCM(val_k)
```


## Related Projects

- [go-logger](https://github.com/opencoff/go-logger) - Simple logging library
- [go-fio](https://github.com/opencoff/go-fio) - Cross-platform file I/O utilities with support for
  concurrent file tree walking and directory tree comparison

## License

MIT License
