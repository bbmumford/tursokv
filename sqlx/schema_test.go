package sqlx

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// schemaObject is one row of sqlite_schema.
type schemaObject struct{ Type, Name, SQL string }

// TestApplySchemaOnCallersDB applies the schema to a *sql.DB opened straight
// from the driver, not through Open: it creates the six TursoKV tables and
// their indexes, the same objects Open creates, and a second run succeeds and
// changes nothing. A database whose rkey table lacks the columns the indexes
// name makes it return an error instead of reporting success.
func TestApplySchemaOnCallersDB(t *testing.T) {
	ctx := context.Background()
	raw := openDriverDB(t, "fresh.db")
	if err := ApplySchema(ctx, raw); err != nil {
		t.Fatalf("ApplySchema on an empty database: %v", err)
	}
	first := schemaObjects(t, raw)
	want := []string{
		"index rhashKey", "index rkeyEtime", "index rkeyType", "index rlistKey",
		"index rsetKey", "index rzsetKey", "index rzsetScore",
		"table rhash", "table rkey", "table rlist", "table rset", "table rstring", "table rzset",
	}
	var got []string
	for _, o := range first {
		got = append(got, o.Type+" "+o.Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("objects after ApplySchema = %v, want %v", got, want)
	}

	if err := ApplySchema(ctx, raw); err != nil {
		t.Fatalf("second ApplySchema: %v", err)
	}
	if again := schemaObjects(t, raw); !reflect.DeepEqual(again, first) {
		t.Fatalf("second ApplySchema changed the schema:\n got %v\nwant %v", again, first)
	}

	opened, err := Open(filepath.Join(t.TempDir(), "opened.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if viaOpen := schemaObjects(t, opened.DB()); !reflect.DeepEqual(viaOpen, first) {
		t.Fatalf("Open created other objects than ApplySchema:\n Open %v\nApplySchema %v", viaOpen, first)
	}

	conflicting := openDriverDB(t, "conflicting.db")
	if _, err := conflicting.ExecContext(ctx, "CREATE TABLE rkey (id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE)"); err != nil {
		t.Fatal(err)
	}
	err = ApplySchema(ctx, conflicting)
	if err == nil {
		t.Fatal("ApplySchema reported success on an rkey table without the etime and type columns its indexes name")
	}
	t.Logf("ApplySchema on the conflicting rkey: %v", err)
}

// openDriverDB opens a file in the test's temp directory directly through the
// tursogo driver, bypassing Open.
func openDriverDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	db, err := sql.Open("turso", filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// declaredName reads the name a CREATE statement gives its object, as written.
var declaredName = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)`)

// schemaObjects lists db's tables, indexes, triggers and views, ordered by
// type and name. It leaves out the engine's own objects: the sqlite_ ones and
// the __turso_internal_ table the engine adds to a file in the MVCC journal
// mode, which Open sets for WriteModeConcurrent. Each Name is the spelling the
// object's CREATE statement declares: the tursogo engine records a table,
// index or trigger name lower-cased in sqlite_schema's name column and keeps
// the statement as written in its sql column, and an identifier differing
// only in case names the same object, so the declared spelling is the one a
// reader compares against the schema.
func schemaObjects(t *testing.T, db *sql.DB) []schemaObject {
	t.Helper()
	rows, err := db.Query("SELECT type, name, sql FROM sqlite_schema ORDER BY type, name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var objs []schemaObject
	for rows.Next() {
		var o schemaObject
		var ddl sql.NullString
		if err := rows.Scan(&o.Type, &o.Name, &ddl); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(o.Name, "sqlite_") || strings.HasPrefix(o.Name, "__turso_internal_") {
			continue
		}
		o.SQL = ddl.String
		if m := declaredName.FindStringSubmatch(o.SQL); m != nil && strings.EqualFold(m[1], o.Name) {
			o.Name = m[1]
		}
		objs = append(objs, o)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return objs
}
