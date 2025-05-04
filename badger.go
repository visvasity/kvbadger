// Copyright (c) 2025 Visvasity LLC

// Package kvbadger implements Visvasity [Key-Value DB API] adapter for the [Badger Database].
//
// [Key-Value DB API]: https://pkg.go.dev/github.com/visvasity/kv
// [Badger Database]: https://pkg.go.dev/github.com/dgraph-io/badger/v4
package kvbadger

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"iter"
	"os"

	"github.com/dgraph-io/badger/v4"
)

type Database struct {
	db *badger.DB
}

// New returns a key-value database instance backed by the given badger
// database.
func New(db *badger.DB) *Database {
	return &Database{
		db: db,
	}
}

type Transaction struct {
	*txn
}

// NewTransaction returns a new read-write transaction.
func (d *Database) NewTransaction(ctx context.Context) (*Transaction, error) {
	return &Transaction{
		txn: &txn{
			db: d,
			tx: d.db.NewTransaction(true /* update */),
		},
	}, nil
}

// Rollback drops the transaction.
func (t *Transaction) Rollback(ctx context.Context) error {
	return t.txn.discard(ctx)
}

type Snapshot struct {
	*txn
}

// NewSnapshot returns a snapshot, which is just a read-only transaction.
func (d *Database) NewSnapshot(ctx context.Context) (*Snapshot, error) {
	return &Snapshot{
		txn: &txn{
			db: d,
			tx: d.db.NewTransaction(false /* update */),
		},
	}, nil
}

func (s *Snapshot) Discard(ctx context.Context) error {
	return s.txn.discard(ctx)
}

type txn struct {
	db *Database

	tx *badger.Txn
}

// Commit commits the transaction.
func (t *txn) Commit(ctx context.Context) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	t.db = nil
	return t.tx.Commit()
}

// discard drops the snapshot.
func (t *txn) discard(ctx context.Context) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	t.db = nil
	t.tx.Discard()
	return nil
}

// Get returns the value for a given key.
func (t *txn) Get(ctx context.Context, k string) (io.Reader, error) {
	item, err := t.tx.Get([]byte(k))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(v), nil
}

// Set stores a key-value pair.
func (t *txn) Set(ctx context.Context, k string, v io.Reader) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	data, err := io.ReadAll(v)
	if err != nil {
		return err
	}
	return t.tx.Set([]byte(k), data)
}

// Delete removes the key-value pair with the given key.
func (t *txn) Delete(ctx context.Context, k string) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	if err := t.tx.Delete([]byte(k)); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return os.ErrNotExist
		}
		return err
	}
	return nil
}

// Scan reads all keys in the database through the iterator, in no-particular
// order.
func (t *txn) Scan(ctx context.Context, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		it := t.tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.KeyCopy(nil))
			data, err := item.ValueCopy(nil)
			if err != nil {
				*errp = err
				return
			}
			if !yield(key, bytes.NewReader(data)) {
				return
			}
		}
	}
}

// Ascend returns key-value pairs in a given range through the iterator, in
// ascending order.
func (t *txn) Ascend(ctx context.Context, beg, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if beg > end && end != "" {
			*errp = os.ErrInvalid
			return
		}

		it := t.tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		it.Rewind()
		if beg != "" {
			it.Seek([]byte(beg))
		}

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.KeyCopy(nil))

			// Range includes begin and excludes end.
			if end != "" && key >= end {
				return
			}

			data, err := item.ValueCopy(nil)
			if err != nil {
				*errp = err
				return
			}
			if !yield(key, bytes.NewReader(data)) {
				return
			}
		}
	}
}

// Descend returns key-value pairs in a given range through the iterator, in
// descending order.
func (t *txn) Descend(ctx context.Context, beg, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		if beg > end && end != "" {
			*errp = os.ErrInvalid
			return
		}

		opts := badger.DefaultIteratorOptions
		opts.Reverse = true

		it := t.tx.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if end != "" {
			it.Seek([]byte(end))
		}

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.KeyCopy(nil))

			// Range includes begin and excludes end.
			if end != "" && key == end {
				continue
			}
			if beg != "" && key < beg {
				return
			}

			data, err := item.ValueCopy(nil)
			if err != nil {
				*errp = err
				return
			}
			if !yield(key, bytes.NewReader(data)) {
				return
			}
		}
	}
}
