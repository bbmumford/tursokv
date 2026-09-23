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

CREATE INDEX IF NOT EXISTS rkey_etime ON rkey(etime) WHERE etime IS NOT NULL;
CREATE INDEX IF NOT EXISTS rkey_type ON rkey(type);

-- String values table
CREATE TABLE IF NOT EXISTS rstring (
    key_id INTEGER PRIMARY KEY REFERENCES rkey(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- List values table (doubly-linked list in table form)
CREATE TABLE IF NOT EXISTS rlist (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    pos REAL NOT NULL,  -- Position for ordering (allows inserts between elements)
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, pos)
);

CREATE INDEX IF NOT EXISTS rlist_key ON rlist(key_id);

-- Set values table
CREATE TABLE IF NOT EXISTS rset (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX IF NOT EXISTS rset_key ON rset(key_id);

-- Hash values table
CREATE TABLE IF NOT EXISTS rhash (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, field)
);

CREATE INDEX IF NOT EXISTS rhash_key ON rhash(key_id);

-- Sorted set values table
CREATE TABLE IF NOT EXISTS rzset (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX IF NOT EXISTS rzset_key ON rzset(key_id);
CREATE INDEX IF NOT EXISTS rzset_score ON rzset(key_id, score);
`
