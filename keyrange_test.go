// Copyright (c) 2025 Visvasity LLC

package kvbadger

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"

	"github.com/visvasity/kv"
	"github.com/visvasity/kv/kvutil"
)

// Compile-time assertions that both the snapshot and the transaction types
// implement the optional kv.KeyRanger interface.
var (
	_ kv.KeyRanger = (*Snapshot)(nil)
	_ kv.KeyRanger = (*Transaction)(nil)
)

func TestAscendDescendKeys(t *testing.T) {
	ctx := context.Background()

	bopts := badger.DefaultOptions(t.TempDir())
	bdb, err := badger.Open(bopts)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()

	kvbdb := New(bdb)
	db := kv.DatabaseFrom(kvbdb)

	// Setup test data.
	err = kvutil.WithReadWriter(ctx, db, func(ctx context.Context, rw kv.ReadWriter) error {
		for _, k := range []string{"key1", "key2", "key3"} {
			if err := rw.Set(ctx, k, strings.NewReader("value-"+k)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	tests := []struct {
		name     string
		beg, end string
		ascend   []string
		descend  []string
		wantErr  bool
	}{
		{
			name:    "Full range (empty beg and end)",
			ascend:  []string{"key1", "key2", "key3"},
			descend: []string{"key3", "key2", "key1"},
		},
		{
			name:    "From key1 to key3 (inclusive-exclusive)",
			beg:     "key1",
			end:     "key3",
			ascend:  []string{"key1", "key2"},
			descend: []string{"key2", "key1"},
		},
		{
			name:    "From smallest key (empty beg)",
			end:     "key2",
			ascend:  []string{"key1"},
			descend: []string{"key1"},
		},
		{
			name:    "To largest key (empty end)",
			beg:     "key2",
			ascend:  []string{"key2", "key3"},
			descend: []string{"key3", "key2"},
		},
		{
			name:    "Invalid range (beg >= end)",
			beg:     "key3",
			end:     "key1",
			wantErr: true,
		},
	}

	// collect exercises the kv.KeyRanger methods against both the snapshot and
	// the transaction read paths.
	collect := func(t *testing.T, seqFor func(r kv.KeyRanger, errp *error) func(func(string) bool), wantErr bool, want []string) {
		t.Helper()

		run := func(r kv.KeyRanger) {
			var gotErr error
			var got []string
			for k := range seqFor(r, &gotErr) {
				got = append(got, k)
			}
			if wantErr && !errors.Is(gotErr, os.ErrInvalid) {
				t.Errorf("expected error os.ErrInvalid, got %v", gotErr)
			}
			if !wantErr && gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("keys = %v, want %v", got, want)
			}
		}

		snap, err := kvbdb.NewSnapshot(ctx)
		if err != nil {
			t.Fatalf("NewSnapshot failed: %v", err)
		}
		defer snap.Discard(ctx)
		run(snap)

		tx, err := kvbdb.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}
		defer tx.Rollback(ctx)
		run(tx)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collect(t, func(r kv.KeyRanger, errp *error) func(func(string) bool) {
				return r.AscendKeys(ctx, tt.beg, tt.end, errp)
			}, tt.wantErr, tt.ascend)

			collect(t, func(r kv.KeyRanger, errp *error) func(func(string) bool) {
				return r.DescendKeys(ctx, tt.beg, tt.end, errp)
			}, tt.wantErr, tt.descend)
		})
	}
}
