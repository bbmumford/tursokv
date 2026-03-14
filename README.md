# tursokv

A Redis-like key-value store backed by Turso/libraryQL with support for embedded replicas, encryption, and concurrent writes.

## Overview

`tursokv` provides a Redis-compatible API backed by Turso/libraryQL. It uses the `go-libsql` CGO driver which supports advanced features not available in pure-Go drivers.

## When to Use TursoKV

| Use Case | TursoKV | TursoRaft |
|----------|---------|-----------|
| **Edge/Client apps** | ✅ Best fit | ❌ Overkill |
| **Single-node caching** | ✅ Best fit | ❌ Overkill |
| **Mesh node coordination** | ❌ No consensus | ✅ Best fit |
| **Distributed sessions** | ❌ Eventual consistency | ✅ Strong consistency |
| **Global sync via cloud** | ✅ Via Turso Cloud | ✅ Via Turso Cloud |
| **Offline-first mobile/edge** | ✅ Embedded replica | ❌ Requires cluster |

**Rule of thumb**: 
- **TursoKV** = Single-node or edge devices syncing to cloud
- **TursoRaft** = Multiple mesh nodes requiring consensus

## Features

### Turso-Specific Capabilities (CGO Required)

| Feature | Description | Link |
|---------|-------------|------|
| **Concurrent Writes** | MVCC-based `BEGIN CONCURRENT` for parallel writers | [Blog](https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes) |
| **Native Encryption** | AES-256/AEGIS-256 encryption at rest | [Blog](https://turso.tech/blog/introducing-fast-native-encryption-in-turso-database) |
| **Embedded Replicas** | Local SQLite that syncs with Turso Cloud | [Blog](https://turso.tech/blog/introducing-databases-anywhere-with-turso-sync) |

### Redis-Compatible Operations

- **Strings**: GET, SET, SETEX, INCR, DECR, APPEND, GETRANGE, etc.
- **Hashes**: HGET, HSET, HDEL, HGETALL, HMGET, HMSET, HINCRBY, etc.
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LINSERT, LTRIM, etc.
- **Sets**: SADD, SREM, SMEMBERS, SISMEMBER, SUNION, SINTER, SDIFF, SMOVE, etc.
- **Sorted Sets**: ZADD (NX/XX/GT/LT/CH), ZREM, ZRANGE, ZRANGEBYSCORE, ZINCRBY, ZPOPMIN, ZPOPMAX, etc.
- **Keys**: DEL, EXISTS, EXPIRE, TTL, KEYS, SCAN, RENAME, TYPE, etc.

## Installation

```bash
go get github.com/bbmumford/tursokv
```

**⚠️ CGO Required**: This package uses `go-libsql` which requires CGO and native libraries.

```bash
# macOS
brew install libsql

# Linux - download from Turso
# See: https://github.com/tursodatabase/go-libsql#installation

# Build with CGO
CGO_ENABLED=1 go build ./...
```

## Usage

### Basic Local Database

```go
db, err := tursokv.Open("./data.db", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// String operations
db.Str().Set("name", "alice")
name, _ := db.Str().Get("name")

// Hash operations  
db.Hash().Set("user:1", "email", "alice@example.com")
email, _ := db.Hash().Get("user:1", "email")

// With expiration
db.Str().SetEx("session", "token123", 3600) // 1 hour TTL
```

### With Turso Cloud Sync (Edge Deployment)

Perfect for edge applications that need to sync with a central cloud database:

```go
db, err := tursokv.Open("./local-replica.db", &tursokv.Options{
    // Turso Cloud connection
    SyncURL:   "libsql://mydb-myorg.turso.io",
    AuthToken: "your-auth-token",
    
    // Auto-sync interval (default: 1 minute)
    SyncInterval: 30 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Data persists locally AND syncs to cloud
db.Str().Set("session:abc", "user:42")

// Manual sync when needed
db.Sync(context.Background())

// Checkpoint to optimize WAL
db.Checkpoint(context.Background())
```

### With Encryption at Rest

```go
db, err := tursokv.Open("./encrypted.db", &tursokv.Options{
    // 32-byte key for AES-256 encryption
    EncryptionKey: "0123456789abcdef0123456789abcdef",
    
    // Cipher: "aegis256" (default, faster) or "aes256"
    Cipher: "aegis256",
})
```

### With Concurrent Writes (MVCC)

```go
db, err := tursokv.Open("./data.db", &tursokv.Options{
    UseConcurrent: true, // Enable BEGIN CONCURRENT (default: true)
})

// Multiple goroutines can write concurrently
// Row-level conflict detection at commit time
go func() { db.Str().Set("key1", "value1") }()
go func() { db.Str().Set("key2", "value2") }()
```

### Transactions

```go
err := db.Update(func(tx *tursokv.Tx) error {
    tx.Str().Set("balance:alice", "100")
    tx.Str().Set("balance:bob", "50")
    
    // Atomically transfer funds
    tx.Str().IncrBy("balance:alice", -25)
    tx.Str().IncrBy("balance:bob", 25)
    
    return nil // commit
})
```

### Full Configuration

```go
db, err := tursokv.Open("./data.db", &tursokv.Options{
    // Turso Cloud sync (for edge deployments)
    SyncURL:      "libsql://mydb-myorg.turso.io",
    AuthToken:    "your-token",
    SyncInterval: time.Minute,
    
    // Security
    EncryptionKey: "32-byte-key-for-encryption!!!!!",
    Cipher:        "aegis256", // or "aes256"
    
    // Performance
    UseConcurrent: true,           // MVCC writes (default: true)
    Timeout:       5 * time.Second,
    ReadOnly:      false,
    
    // Logging
    Logger: slog.Default(),
})
```
```

## API Reference

### String Operations (`db.Str()`)

```go
Set(key, value string) error
Get(key string) (string, error)
SetEx(key, value string, seconds int) error
GetEx(key string, seconds int) (string, error)
Incr(key string) (int64, error)
IncrBy(key string, delta int64) (int64, error)
Decr(key string) (int64, error)
Append(key, value string) (int, error)
GetRange(key string, start, end int) (string, error)
SetRange(key string, offset int, value string) (int, error)
StrLen(key string) (int, error)
```

### Hash Operations (`db.Hash()`)

```go
Set(key, field, value string) error
Get(key, field string) (string, error)
GetAll(key string) (map[string]string, error)
Del(key string, fields ...string) (int, error)
Exists(key, field string) (bool, error)
Keys(key string) ([]string, error)
Vals(key string) ([]string, error)
Len(key string) (int, error)
IncrBy(key, field string, delta int64) (int64, error)
```

### List Operations (`db.List()`)

```go
LPush(key string, values ...string) (int, error)
RPush(key string, values ...string) (int, error)
LPop(key string) (string, error)
RPop(key string) (string, error)
LRange(key string, start, stop int) ([]string, error)
LLen(key string) (int, error)
LIndex(key string, index int) (string, error)
LSet(key string, index int, value string) error
```

### Set Operations (`db.Set()`)

```go
Add(key string, members ...string) (int, error)
Rem(key string, members ...string) (int, error)
Members(key string) ([]string, error)
IsMember(key, member string) (bool, error)
Card(key string) (int, error)
```

### Sorted Set Operations (`db.ZSet()`)

```go
Add(key string, score float64, member string) (int, error)
Rem(key string, members ...string) (int, error)
Score(key, member string) (float64, error)
Range(key string, start, stop int) ([]string, error)
RangeByScore(key string, min, max float64) ([]string, error)
Card(key string) (int, error)
Rank(key, member string) (int, error)
IncrBy(key, member string, delta float64) (float64, error)
```

### Key Operations (`db.Key()`)

```go
Del(keys ...string) (int, error)
Exists(keys ...string) (int, error)
Expire(key string, seconds int) (bool, error)
ExpireAt(key string, timestamp int64) (bool, error)
TTL(key string) (int, error)
Persist(key string) (bool, error)
Rename(oldkey, newkey string) error
Type(key string) (string, error)
Keys(pattern string) ([]string, error)
Scan(cursor int, pattern string, count int) (int, []string, error)
```

## Schema

TursoKV uses the following SQLite schema:

```sql
-- Key metadata (type, expiration, version)
CREATE TABLE rkey (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    type INTEGER NOT NULL,  -- 1=string, 2=list, 3=set, 4=hash, 5=zset
    version INTEGER NOT NULL DEFAULT 0,
    etime INTEGER,  -- Expiration time (Unix ms)
    mtime INTEGER NOT NULL
);

-- Type-specific value tables
CREATE TABLE rstring (key_id INTEGER PRIMARY KEY, value BLOB);
CREATE TABLE rlist (key_id INTEGER, pos REAL, value BLOB);
CREATE TABLE rset (key_id INTEGER, member BLOB);
CREATE TABLE rhash (key_id INTEGER, field TEXT, value BLOB);
CREATE TABLE rzset (key_id INTEGER, member BLOB, score REAL);
```

## Dependencies

- `github.com/tursodatabase/go-libsql` - libraryQL/SQLite driver with Turso features

## Building

Requires CGO and the libraryQL native library:

```bash
# macOS
brew install libsql

# Or download from Turso
# https://github.com/tursodatabase/go-libsql#installation

# Build
CGO_ENABLED=1 go build ./...
```

## Credits

Inspired by [Redka](https://github.com/nalgeon/redka) by Anton Zhiyanov.

## License

MIT
