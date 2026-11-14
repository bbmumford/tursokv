package sqlx

import (
	"context"
	"database/sql"
	"fmt"
)

// ApplySchema creates TursoKV's tables and indexes in db, running schema as
// one multi-statement Exec. Every statement is CREATE ... IF NOT EXISTS, so it
// runs on every open and leaves a database that already holds them unchanged;
// it does not compare an existing table's shape, and fails only when a
// statement does, such as an index naming a column the existing table lacks.
//
// Open calls it on the policy-proved pool it builds. A caller holding its own
// *sql.DB, such as a tool that dumps the schema from a scratch database, calls
// it directly. The value tables' ON DELETE CASCADE removes a key's values only
// on a connection with foreign keys on; Open's pool proves that on every
// connection, and a caller's db must do the same.
func ApplySchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	return nil
}

// schema is the TursoKV table layout ApplySchema applies. Every value table
// references rkey(id) ON DELETE CASCADE, which removes a key's values with the
// key only while foreign keys are enforced on the connection that deletes it.
const schema = `
-- Key metadata table
-- Tracks all keys with their types and TTL
CREATE TABLE IF NOT EXISTS rkey (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    type INTEGER NOT NULL DEFAULT 1,  -- 1=string, 2=list, 3=set, 4=hash, 5=zset
    version INTEGER NOT NULL DEFAULT 0,
    etime INTEGER,  -- Expiration time (Unix ms), NULL = no expiry
    mtime INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
);

CREATE INDEX IF NOT EXISTS rkeyEtime ON rkey(etime) WHERE etime IS NOT NULL;
CREATE INDEX IF NOT EXISTS rkeyType ON rkey(type);

-- String values table
CREATE TABLE IF NOT EXISTS rstring (
    keyId INTEGER PRIMARY KEY REFERENCES rkey(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- List values table (doubly-linked list in table form)
CREATE TABLE IF NOT EXISTS rlist (
    keyId INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    pos REAL NOT NULL,  -- Position for ordering (allows inserts between elements)
    value BLOB NOT NULL,
    PRIMARY KEY (keyId, pos)
);

CREATE INDEX IF NOT EXISTS rlistKey ON rlist(keyId);

-- Set values table
CREATE TABLE IF NOT EXISTS rset (
    keyId INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (keyId, member)
);

CREATE INDEX IF NOT EXISTS rsetKey ON rset(keyId);

-- Hash values table
CREATE TABLE IF NOT EXISTS rhash (
    keyId INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (keyId, field)
);

CREATE INDEX IF NOT EXISTS rhashKey ON rhash(keyId);

-- Sorted set values table
CREATE TABLE IF NOT EXISTS rzset (
    keyId INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (keyId, member)
);

CREATE INDEX IF NOT EXISTS rzsetKey ON rzset(keyId);
CREATE INDEX IF NOT EXISTS rzsetScore ON rzset(keyId, score);
`
