package tursokv

import (
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
