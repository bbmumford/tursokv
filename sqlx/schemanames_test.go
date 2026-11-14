package sqlx

import (
	"context"
	"database/sql"
	"regexp"
	"sort"
	"testing"
)

// ruleName is the one spelling every identifier this store declares takes: lowerCamelCase
// with an acronym written as a word. Two adjacent capitals are an acronym kept in capitals;
// one trailing capital is a one-letter final word.
var ruleName = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[A-Z][a-z0-9]+)*[A-Z]?$`)

// The key table, the five value tables and their indexes are declared in that one
// spelling, read from the live schema ApplySchema leaves.
func TestTheSchemaDeclaresOnlyRuleNames(t *testing.T) {
	ctx := context.Background()
	db := openDriverDB(t, "names.db")
	if err := ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	var bad []string
	for _, o := range schemaObjects(t, db) {
		if !ruleName.MatchString(o.Name) {
			bad = append(bad, o.Name)
		}
		if o.Type != "table" {
			continue
		}
		for _, c := range tableColumns(t, db, o.Name) {
			if !ruleName.MatchString(c) {
				bad = append(bad, o.Name+"."+c)
			}
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("the schema declares %d names outside the rule: %q", len(bad), bad)
	}
}

// tableColumns answers a table's column names from PRAGMA table_info, whose second
// column is the name.
func tableColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		switch v := vals[1].(type) {
		case string:
			names = append(names, v)
		case []byte:
			names = append(names, string(v))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return names
}
