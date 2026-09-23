package rlist

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bbmumford/tursokv/internal/rkey"
	"github.com/bbmumford/tursokv/sqlx"
)

func TestLRemSelectBindsLimit(t *testing.T) {
	cases := []struct {
		count     int
		wantOrder string
		wantLimit any
	}{
		{0, "", nil},
		{1, "ASC", int64(1)},
		{9, "ASC", int64(9)},
		{10, "ASC", int64(10)},
		{123456, "ASC", int64(123456)},
		{-1, "DESC", int64(1)},
		{-10, "DESC", int64(10)},
		{math.MinInt, "DESC", int64(math.MaxInt64)},
	}
	for _, tc := range cases {
		query, args := lremSelect(7, tc.count, "x")
		if tc.wantOrder == "" {
			if strings.Contains(query, "LIMIT") || len(args) != 2 {
				t.Errorf("count 0: query %q args %v, want no limit", query, args)
			}
			continue
		}
		if !strings.HasSuffix(query, "ORDER BY pos "+tc.wantOrder+" LIMIT ?") {
			t.Errorf("count %d: query %q, want ORDER BY pos %s LIMIT ?", tc.count, query, tc.wantOrder)
		}
		if len(args) != 3 || args[2] != tc.wantLimit {
			t.Errorf("count %d: args %v, want limit %v", tc.count, args, tc.wantLimit)
		}
	}
}

// TestLRem removes occurrences of "x" from a list that interleaves twelve of
// them with other values, for counts on both sides of ten, past the number
// present, and in both directions.
func TestLRem(t *testing.T) {
	db, err := sqlx.Open(filepath.Join(t.TempDir(), "lrem.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lists := New(db, rkey.New(db))

	var start []string
	for i := range 12 {
		start = append(start, "x", fmt.Sprint(i))
	}
	keep := func(removeFirst, removeLast int) []string {
		var out []string
		seen, total := 0, 12
		for _, v := range start {
			if v == "x" {
				seen++
				if seen <= removeFirst || seen > total-removeLast {
					continue
				}
			}
			out = append(out, v)
		}
		return out
	}

	cases := []struct {
		count       int
		wantRemoved int
		want        []string
	}{
		{0, 12, keep(12, 0)},
		{1, 1, keep(1, 0)},
		{9, 9, keep(9, 0)},
		{10, 10, keep(10, 0)},
		{11, 11, keep(11, 0)},
		{12, 12, keep(12, 0)},
		{25, 12, keep(12, 0)},
		{-1, 1, keep(0, 1)},
		{-10, 10, keep(0, 10)},
		{-12, 12, keep(0, 12)},
		{-25, 12, keep(0, 12)},
		{math.MinInt, 12, keep(0, 12)},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(tc.count), func(t *testing.T) {
			key := fmt.Sprint("list", i)
			if _, err := lists.RPush(key, start...); err != nil {
				t.Fatal(err)
			}
			removed, err := lists.LRem(key, tc.count, "x")
			if err != nil {
				t.Fatalf("LRem(%d): %v", tc.count, err)
			}
			if removed != tc.wantRemoved {
				t.Fatalf("LRem(%d) removed %d, want %d", tc.count, removed, tc.wantRemoved)
			}
			got, err := lists.LRange(key, 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("after LRem(%d): %v, want %v", tc.count, got, tc.want)
			}
		})
	}
}
