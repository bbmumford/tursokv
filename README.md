# tursokv

tursokv is a Redis-like key-value store backed by the Turso Database engine, with concurrent writes, encryption at rest and embedded replicas that sync with Turso Cloud.

## Overview

`tursokv` provides a Redis-like API over SQLite-format tables. It talks to the engine through [`turso.tech/database/tursogo`](https://docs.turso.tech/sdk/go/quickstart), the Turso Go SDK: pure Go at build time (no CGO), with the engine itself loaded as a shared library at run time.

## When to Use TursoKV

| Use Case | TursoKV | TursoRaft |
|----------|---------|-----------|
| **Edge/Client apps** | ✅ Best fit | ❌ Overkill |
| **Single-node caching** | ✅ Best fit | ❌ Overkill |
| **Mesh node coordination** | ❌ No consensus | ✅ Best fit |
| **Distributed sessions** | ❌ Replicas that both write can lose each other's writes | ✅ Strong consistency |
| **Global sync via cloud** | ⚠️ One writing replica per database | ✅ Via Turso Cloud |
| **Offline-first mobile/edge** | ⚠️ Embedded replica, one writer per database | ❌ Requires cluster |

**Rule of thumb**:
- **TursoKV** = Single-node, or edge devices syncing to cloud where one replica writes each database
- **TursoRaft** = Multiple mesh nodes requiring consensus

## Features

| Feature | Applies to | Description |
|---------|------------|-------------|
| **Concurrent writes** | Local files | The file runs in the MVCC journal mode and every transaction begins with `BEGIN CONCURRENT`, so transactions that write different rows proceed in parallel ([blog](https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes)) |
| **Encryption at rest** | Local files | AES-256-GCM with a caller-supplied key ([blog](https://turso.tech/blog/introducing-fast-native-encryption-in-turso-database)) |
| **Embedded replicas** | Synced databases | A local file that pushes its writes to Turso Cloud and pulls the remote's changes ([blog](https://turso.tech/blog/introducing-databases-anywhere-with-turso-sync)) |
| **Referential integrity** | Every database | Foreign keys are enforced on every connection, so deleting a key always deletes its values |

### Redis-Like Operations

- **Strings**: GET, SET, SETEX, SETNX, MGET, MSET, INCR, INCRBY, DECR, APPEND, STRLEN
- **Hashes**: HGET, HSET, HSETNX, HMGET, HMSET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LINDEX, LRANGE, LLEN, LSET, LTRIM, LINSERT, LREM
- **Sets**: SADD, SREM, SISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SUNION, SINTER, SDIFF, SMOVE
- **Sorted Sets**: ZADD (NX/XX/GT/LT/CH), ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZINCRBY, ZPOPMIN, ZPOPMAX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE
- **Keys**: GET (metadata), EXISTS, DEL, EXPIRE, EXPIREAT, PERSIST, KEYS

## Installation

```bash
go get github.com/bbmumford/tursokv
```

`tursokv` depends on `github.com/bbmumford/policypool`, resolved from a sibling checkout by the `replace github.com/bbmumford/policypool => ../policypool` line in `go.mod`, and on `github.com/bbmumford/filekind`, which policypool imports, resolved the same way from `../filekind`.

### Building and running

- **Build**: no CGO is needed; `CGO_ENABLED=0 go build ./...` works.
- **Run**: tursogo embeds the engine as a shared library, writes it to a cache directory on first use and loads it with `dlopen` (through purego), even in a `CGO_ENABLED=0` binary. The runtime therefore needs:
  - libc and the dynamic loader. A `scratch` or `distroless/static` image cannot load the engine; use an image such as `gcr.io/distroless/cc-debian12`.
  - A writable cache directory: `$TURSO_GO_CACHE_DIR` when set, otherwise the user cache directory, otherwise the temp directory.
- **Platforms with an embedded engine**: darwin/amd64, darwin/arm64, linux/amd64, linux/arm64, windows/amd64. On musl-based Linux, build with `-tags musl`.

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
db.Str().SetEx("session", "token123", time.Hour)
```

`":memory:"` opens an in-memory database. It lives on a single connection for as long as the `DB` is open.

### Write Modes

`Options.WriteMode` selects how writes are taken, and `db.WriteMode()` reports what `Open` established:

| Mode | Begins transactions with | Pool | Journal |
|------|--------------------------|------|---------|
| `WriteModeConcurrent` | `BEGIN CONCURRENT` | 4 connections | `mvcc` |
| `WriteModeSerial` | `BEGIN` | 1 connection | `wal` (local file), as the sync engine sets it (replica) |

The zero value, `WriteModeAuto`, resolves to `WriteModeConcurrent` for a local file and to `WriteModeSerial` for `":memory:"` or a synced database. `Open` refuses `WriteModeConcurrent` for those two rather than downgrading it:
- each connection to `":memory:"` opens its own database;
- the sync engine refuses the MVCC journal mode while it captures changes.

`Open` sets the journal mode on every open and reads it back, so a file can move between modes across opens.

In `WriteModeConcurrent`, an `Update` whose transaction writes a row that another open transaction has already written fails with a write-write conflict. None of its writes are kept, and the caller decides whether to retry.

Each call made outside `Update` that writes (such as `db.Str().Set`, `db.List().LPop`, `db.Set().Move` or `db.Key().Expire`) runs as one transaction, so a call that issues several statements keeps all of its writes or none of them, in either write mode. In `WriteModeConcurrent`, a call that meets a write-write conflict, whether with another call or with an open `Update`, is run again in a fresh transaction after a short, jittered, growing wait, up to 64 runs in all. A call that reads and then writes, such as `Incr`, `SetNX` or a pop, therefore fails and runs again instead of writing over a change made since it read: in the package tests, eight goroutines incrementing one counter 25 times each end at 200, eight `SetNX` calls on one new key have exactly one winner, and concurrent pops never return the same member twice. When the conflict outlasts those runs (for example, an `Update` holds the row for longer than about 0.2 to 0.4 seconds), the call returns the conflict error and keeps none of its writes. Calls that only read, such as `MGet` or `LRange`, run each of their statements on its own, so one call of several statements can read values committed at different moments.

```go
db, err := tursokv.Open("./data.db", &tursokv.Options{
    WriteMode: tursokv.WriteModeSerial, // one writer at a time
})
```

### Transactions

```go
err := db.Update(func(tx *tursokv.Tx) error {
    tx.Str().Set("balance:alice", "100")
    tx.Str().Set("balance:bob", "50")

    // Atomically transfer funds
    tx.Str().IncrBy("balance:alice", -25)
    tx.Str().IncrBy("balance:bob", 25)

    return nil // commit; any error rolls back
})
```

Inside `Update` and `View`, run every statement through `tx`. In `WriteModeSerial` the pool's only connection is the one `tx` holds, so a statement through `db` would wait forever. `View` is not enforced read-only: the engine has no read-only transaction.

A write-write conflict, or an `INSERT OR ROLLBACK` that hits a constraint, makes the engine end the transaction on the spot. From then on every statement through `tx` fails, and so does the commit, so a function that carries on after the failed statement (as the example above does, ignoring each error) still keeps nothing. When a failed statement leaves the transaction open, as a plain constraint failure does, the statements after it run in it as usual.

### Embedded Replica with Turso Cloud Sync

```go
db, err := tursokv.Open("./local-replica.db", &tursokv.Options{
    SyncURL:   "libsql://mydb-myorg.turso.io",
    AuthToken: "your-auth-token",

    // Background Sync period. 0 means the default of one minute;
    // a negative value turns the background loop off.
    SyncInterval: 30 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Writes land in the local file first.
db.Str().Set("session:abc", "user:42")

// Push local writes, then pull remote changes, without waiting for the loop.
db.Sync(context.Background())

// Move the WAL into the database file, keeping sync metadata.
db.Checkpoint(context.Background())
```

A new replica pulls the remote's state during `Open`. After that, `Sync` pushes local writes to the remote and then pulls the remote's changes. One background loop runs `Sync` every `SyncInterval`, and each failure is logged at warn level through `Options.Logger`. `Close` stops the loop, waiting for any round in progress.

A failed push makes `Sync` return its error without pulling.

**Conflict behaviour between replicas.** Measured with two replicas against a `tursodb` 0.7.2 `--sync-server`; Turso Cloud has not been measured.
- A row both replicas wrote holds the later push's value on every replica: when one replica counts a counter to 10 and the other to 3, and the second pushes last, both read 3.
- Each replica assigns `rkey` ids itself, so new keys created on two replicas between syncs can take the same id. The later push replaces the earlier key's row: that key and its value are gone on every replica, and no `Sync` reports an error.
- A change the server refuses, such as a new key the other replica pushed first (`rkey`'s UNIQUE key), fails that `Sync`. The next `Sync` returns nil and the refused change is discarded: this replica reads the other replica's value for the key, and the refused value row lands on whichever key holds its id on the server.

So two replicas that both create keys can lose one another's writes without any error; only a database with one writing replica avoids it.

`TestSyncConditionsLive` checks these conditions against a running sync server and is skipped unless `TURSOKV_SYNC_URL` names one:

```bash
tursodb server.db --sync-server 127.0.0.1:8080 &
TURSOKV_SYNC_URL=http://127.0.0.1:8080 go test -count=1 -run TestSyncConditionsLive .
```

A replica is always `WriteModeSerial` and is stored unencrypted: tursogo's sync API takes no encryption settings. `Open` therefore refuses `EncryptionKey` together with `SyncURL`, returning `tursokv.ErrEncryptionWithSync` before it creates any file or contacts the remote.

### Encryption at Rest (local files)

```go
db, err := tursokv.Open("./encrypted.db", &tursokv.Options{
    // 32-byte key, hex-encoded (64 hex characters), for AES-256-GCM.
    EncryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
})
```

An encrypted file refuses to open without its key.

### Full Configuration

```go
db, err := tursokv.Open("./data.db", &tursokv.Options{
    // Embedded replica
    SyncURL:      "libsql://mydb-myorg.turso.io",
    AuthToken:    "your-token",
    SyncInterval: time.Minute,

    // Local files only: Open returns ErrEncryptionWithSync when this is set
    // together with SyncURL.
    // EncryptionKey: "<64 hex characters>",

    // Writes
    WriteMode: tursokv.WriteModeAuto,

    // Connection lifetime is ten times this (file-backed databases only)
    Timeout: 5 * time.Second,

    // Receives background sync failures
    Logger: slog.Default(),
})
```

`Open` reads `opts` and never modifies it. It refuses `ReadOnly`, returning `tursokv.ErrReadOnlyUnsupported`: every open writes the journal mode and the schema, and the engine has no read-only open mode. `Cipher` may be empty or `"aes256gcm"`, the one cipher local files are encrypted with; any other name returns `tursokv.ErrUnsupportedCipher`, and a `Cipher` without `EncryptionKey` returns `tursokv.ErrCipherWithoutKey`. Each refusal comes before `Open` creates a directory or file or contacts the remote.

### Direct SQL

`db.SQL()` returns the underlying `*sql.DB`. Every connection it hands out has foreign keys on, and it refuses any statement that would turn them off. Open transactions on it with `BeginTx`, never with a `BEGIN` statement: a connection returned to the pool inside a transaction would carry later callers' writes into a transaction that nobody commits.

## API Reference

### Strings (`db.Str()`)

```go
Get(key string) (string, error)
GetBytes(key string) ([]byte, error)
Set(key, value string) error
SetBytes(key string, value []byte) error
SetEx(key, value string, ttl time.Duration) error
SetNX(key, value string) (bool, error)
MGet(keys ...string) ([]string, error)
MSet(pairs map[string]string) error
Incr(key string) (int64, error)
IncrBy(key string, delta int64) (int64, error)
Decr(key string) (int64, error)
Append(key, value string) (int, error)
Strlen(key string) (int, error)
```

### Hashes (`db.Hash()`)

```go
Get(key, field string) (string, error)
GetBytes(key, field string) ([]byte, error)
Set(key, field, value string) (bool, error)
SetBytes(key, field string, value []byte) (bool, error)
SetNX(key, field, value string) (bool, error)
MGet(key string, fields ...string) ([]string, error)
MSet(key string, pairs map[string]string) error
Del(key string, fields ...string) (int, error)
Exists(key, field string) (bool, error)
GetAll(key string) (map[string]string, error)
Keys(key string) ([]string, error)
Vals(key string) ([]string, error)
Len(key string) (int, error)
IncrBy(key, field string, delta int64) (int64, error)
IncrByFloat(key, field string, delta float64) (float64, error)
```

### Lists (`db.List()`)

```go
LPush(key string, values ...string) (int, error)
RPush(key string, values ...string) (int, error)
LPop(key string) (string, error)
RPop(key string) (string, error)
LIndex(key string, index int) (string, error)
LRange(key string, start, stop int) ([]string, error)
LLen(key string) (int, error)
LSet(key string, index int, value string) error
LTrim(key string, start, stop int) error
LInsertBefore(key, pivot, value string) (int, error)
LInsertAfter(key, pivot, value string) (int, error)
LRem(key string, count int, value string) (int, error)
```

### Sets (`db.Set()`)

```go
Add(key string, members ...string) (int, error)
Remove(key string, members ...string) (int, error)
IsMember(key, member string) (bool, error)
Members(key string) ([]string, error)
Card(key string) (int, error)
Pop(key string) (string, error)
RandMember(key string) (string, error)
RandMembers(key string, count int) ([]string, error)
Union(keys ...string) ([]string, error)
Inter(keys ...string) ([]string, error)
Diff(keys ...string) ([]string, error)
Move(src, dst, member string) (bool, error)
```

### Sorted Sets (`db.ZSet()`)

```go
Add(key string, members ...rzset.ScoreMember) (int, error)
AddWithOptions(key string, opts *rzset.ZAddOptions, members ...rzset.ScoreMember) (int, error)
Remove(key string, members ...string) (int, error)
Score(key, member string) (float64, bool, error)
Rank(key, member string) (int, bool, error)
RevRank(key, member string) (int, bool, error)
Card(key string) (int, error)
Count(key string, min, max float64) (int, error)
Range(key string, start, stop int) ([]string, error)
RangeWithScores(key string, start, stop int) ([]rzset.ScoreMember, error)
RevRange(key string, start, stop int) ([]string, error)
RevRangeWithScores(key string, start, stop int) ([]rzset.ScoreMember, error)
RangeByScore(key string, min, max float64) ([]string, error)
RangeByScoreWithLimit(key string, min, max float64, offset, count int) ([]string, error)
IncrBy(key, member string, delta float64) (float64, error)
PopMin(key string) (rzset.ScoreMember, bool, error)
PopMax(key string) (rzset.ScoreMember, bool, error)
RemRangeByRank(key string, start, stop int) (int, error)
RemRangeByScore(key string, min, max float64) (int, error)
```

### Keys (`db.Key()`)

```go
Get(key string) (rkey.Key, error) // type, version, expiry; Key.TTL() is the remaining time in ms
Exists(keys ...string) (int, error)
Delete(keys ...string) (int, error)
Expire(key string, ttl time.Duration) (bool, error)
ExpireAt(key string, at time.Time) (bool, error)
Persist(key string) (bool, error)
Keys(pattern string) ([]string, error)
```

## Schema

`sqlx.ApplySchema` creates TursoKV's tables and indexes in any `*sql.DB`, and `Open` calls it on every open. Every statement is `CREATE … IF NOT EXISTS`, so running it again adds only what is missing. It does not check the shape of a table that already exists; it fails only when a statement fails, such as an index naming a column the existing table lacks.

```go
db, err := sql.Open("turso", "scratch.db")
if err != nil {
    log.Fatal(err)
}
if err := sqlx.ApplySchema(ctx, db); err != nil {
    log.Fatal(err)
}
```

The DDL is the `schema` constant in `sqlx/schema.go`. It creates:

| Table | Holds | Primary key | Indexes |
|-------|-------|-------------|---------|
| `rkey` | One row per key: `key` (UNIQUE), `type` (1 string, 2 list, 3 set, 4 hash, 5 sorted set), `version`, `etime` (expiry in Unix ms, NULL for none), `mtime` (last write in Unix ms) | `id` | `rkeyEtime` on `etime` where it is set; `rkeyType` on `type` |
| `rstring` | A string key's `value` | `keyId` | none |
| `rlist` | List elements, ordered by `pos`, a REAL so an element can go between two others | `(keyId, pos)` | `rlistKey` |
| `rset` | Set members | `(keyId, member)` | `rsetKey` |
| `rhash` | Hash fields and values | `(keyId, field)` | `rhashKey` |
| `rzset` | Sorted-set members and scores | `(keyId, member)` | `rzsetKey`; `rzsetScore` on `(keyId, score)` |

Every value table's `keyId` references `rkey(id) ON DELETE CASCADE`, so deleting a key deletes its values, but only on a connection with foreign keys on. A caller that applies the schema to its own `*sql.DB` must turn them on for every connection.

The engine opens every connection with foreign keys off, so a one-time `PRAGMA foreign_keys=ON` would reach only one connection of the pool. `tursokv` therefore opens every pool through `policypool` with `ForeignKeysRequired`. Each physical connection sets and reads back `PRAGMA foreign_keys` when it opens and again each time it is reused; a connection that cannot prove it is discarded. For a replica, whose pool tursogo builds internally, the pool is held to one connection and each connection is proved the same way.

## Dependencies

- `github.com/bbmumford/policypool` (`sqlx`): per-connection foreign-key
  enforcement.
- `turso.tech/database/tursogo` v0.8.0-pre.12: the Turso Database engine and
  its sync client.

## Used by

No package in the ORBTR estate imports it.

## Platforms and build

No cgo at build time: tursogo embeds the Turso engine as a shared library and
loads it at run time through purego, so `CGO_ENABLED=0` builds work; the
runtime needs libc, the dynamic loader and a writable cache directory (see
Building and running). The module's
`go` directive is 1.25.0, and `go.mod` replaces `github.com/bbmumford/policypool`
and `github.com/bbmumford/filekind` (which policypool imports) with the sibling
checkouts `../policypool` and `../filekind`.

## Credits

Inspired by [Redka](https://github.com/nalgeon/redka) by Anton Zhiyanov.

## Licence

MIT. Copyright (c) 2026 Brandon Miller-Mumford. The full text is in
[LICENSE](LICENSE).

## Contributing

[AGENTS.md](AGENTS.md) holds the repository's two rules, for people and coding
agents alike: no temporary documents in the repository, and conventional commit
messages that stand on their own. [CLAUDE.md](CLAUDE.md) imports it for Claude
Code.
