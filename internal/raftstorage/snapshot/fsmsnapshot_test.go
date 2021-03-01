package snapshot_test

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/ankur-anand/dis-db/api/proto/v1/kv"
	"github.com/ankur-anand/dis-db/internal/raftstorage/snapshot"
	badger "github.com/dgraph-io/badger/v3"
)

// Database is a wrapper around a BadgerDB backend database
type Database struct {
	db *badger.DB // the underlying databse
}

// SnapshotItems provides a snapshot isolation of a transaction
// from the underyling database
func (d Database) SnapshotItems() <-chan *kv.SnapshotItem {
	// create a new no blocking channel
	ch := make(chan *kv.SnapshotItem, 1024)
	// generate items from snapshot to channel
	go d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		keyCount := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			ssi := &kv.SnapshotItem{Key: k, Value: v}
			copy(ssi.Key, k)
			keyCount = keyCount + 1
			ch <- ssi
			if err != nil {
				return err
			}
		}

		// just use nil to mark the end
		ssi := &kv.SnapshotItem{
			Key:   nil,
			Value: nil,
		}
		ch <- ssi

		return nil
	})

	// return channel to persist
	return ch
}

// Set attempts to store a value for a given key
func (d Database) Set(key string, val []byte) error {
	txn := d.db.NewTransaction(true)
	err := txn.Set([]byte(key), val)
	if err == badger.ErrTxnTooBig {
		_ = txn.Commit()
		txn = d.db.NewTransaction(true)
		err = txn.Set([]byte(key), val)
	}
	return txn.Commit()
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestPersit(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	bdb := Database{
		db: badgerDB,
	}

	rand.Seed(time.Now().UnixNano())
	keyValuePair := make(map[string][]byte)
	count := 10000
	// add 10 key value pair
	for i := 0; i < count; i++ {
		keyValuePair[randSeq(10)] = []byte(randSeq(30 + i))
	}

	// set some value
	for k, v := range keyValuePair {
		err := bdb.Set(k, v)
		if err != nil {
			t.Errorf("err set db %v", err)
		}
	}

	fsnap := snapshot.NewFSMSnapshot(bdb)
	sink := &inmemSnapshotSink{contents: &bytes.Buffer{}}
	err = fsnap.Persist(sink)
	if err != nil {
		t.Errorf("unexpected error during persist %v", err)
	}

	// total writes should be 2*count for wire framing
	if sink.totalWrites != 2*count {
		t.Errorf("expected writes %d got %d", 2*count, sink.totalWrites)
	}
}

// inmemSnapshotSink implements SnapshotSink in memory
type inmemSnapshotSink struct {
	totalWrites int
	size        int64
	id          string
	contents    *bytes.Buffer
}

// Write appends the given bytes to the snapshot contents
func (s *inmemSnapshotSink) Write(p []byte) (n int, err error) {
	written, err := s.contents.Write(p)
	s.size += int64(written)
	s.totalWrites++
	return written, err
}

// Close updates the Size and is otherwise a no-op
func (s *inmemSnapshotSink) Close() error {
	return nil
}

// ID returns the ID of the SnapshotMeta
func (s *inmemSnapshotSink) ID() string {
	return s.id
}

// Cancel returns successfully with a nil error
func (s *inmemSnapshotSink) Cancel() error {
	return nil
}
