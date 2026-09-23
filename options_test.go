package tursokv

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionOptionsSyncInterval(t *testing.T) {
	cases := []struct {
		name  string
		given time.Duration
		want  time.Duration
	}{
		{"zero takes the default", 0, defaultSyncInterval},
		{"positive is kept", 30 * time.Second, 30 * time.Second},
		{"negative turns the loop off", -1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := Options{SyncInterval: tc.given, WriteMode: WriteModeSerial}
			mergeOptions(&o, &defaultOptions)
			got := connectionOptions(o)
			if got.SyncInterval != tc.want {
				t.Fatalf("SyncInterval = %v, want %v", got.SyncInterval, tc.want)
			}
			if got.WriteMode != WriteModeSerial {
				t.Fatalf("WriteMode = %v, want serial", got.WriteMode)
			}
			if got.SyncErrors == nil {
				t.Fatal("SyncErrors is nil; background failures would be dropped")
			}
		})
	}
}

// TestOpenRefusesEncryptionKeyWithSyncURL opens through this package's Open,
// whose Options reach the connection layer only through connectionOptions. A
// key given with a SyncURL is refused with this package's
// ErrEncryptionWithSync before the remote sees a request. A key alone
// encrypts the local file: it reopens with the key and refuses to open
// without it.
func TestOpenRefusesEncryptionKeyWithSyncURL(t *testing.T) {
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	var requests atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "refused by test", http.StatusServiceUnavailable)
	}))
	defer remote.Close()

	db, err := Open(filepath.Join(t.TempDir(), "replica.db"), &Options{SyncURL: remote.URL, EncryptionKey: key})
	if err == nil {
		db.Close()
	}
	if !errors.Is(err, ErrEncryptionWithSync) {
		t.Fatalf("Open with EncryptionKey and SyncURL: err = %v, want %v", err, ErrEncryptionWithSync)
	}
	if n := requests.Load(); n != 0 {
		t.Fatalf("the refused Open sent %d requests to the remote", n)
	}

	path := filepath.Join(t.TempDir(), "encrypted.db")
	db, err = Open(path, &Options{EncryptionKey: key})
	if err != nil {
		t.Fatalf("Open with EncryptionKey alone: %v", err)
	}
	if err := db.Str().Set("secret", "kept"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(path, nil); err == nil {
		db.Close()
		t.Fatal("the file opened with EncryptionKey opened without it")
	}
	db, err = Open(path, &Options{EncryptionKey: key})
	if err != nil {
		t.Fatalf("reopen with the key: %v", err)
	}
	defer db.Close()
	if got, err := db.Str().Get("secret"); err != nil || got != "kept" {
		t.Fatalf("Get after reopen = %q, %v; want \"kept\"", got, err)
	}
}
