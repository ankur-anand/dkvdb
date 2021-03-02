package kv_test

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/ankur-anand/dis-db/internal/raftstorage/kv"
)

func TestKVStorage(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	database, err := kv.NewDatabase(context.Background(), dataDir)
	if err != nil {
		t.Fatal(err)
	}

	defer database.Shutdown()
	rand.Seed(time.Now().UnixNano())
	keyValuePair := make(map[string][]byte)
	// add 10 key value pair
	for i := 10; i < 21; i++ {
		keyValuePair[randSeq(10)] = []byte(randSeq(30 + i))
	}

	// set some value
	for k, v := range keyValuePair {
		err := database.SetB([]byte(k), v)
		if err != nil {
			t.Errorf("err set db %v", err)
		}
	}

	// get some value
	for k, v := range keyValuePair {
		val, err := database.GetB([]byte(k))
		if err != nil {
			t.Errorf("err get db %v", err)
		}
		if !bytes.Equal(v, val) {
			t.Errorf("expected %s got %s", string(v), string(val))
		}
	}

	// del some value
	for k := range keyValuePair {
		err := database.DelB([]byte(k))
		if err != nil {
			t.Errorf("err sel db %v", err)
		}
	}

	for k := range keyValuePair {
		val, err := database.GetB([]byte(k))
		if err != nil {
			t.Errorf("err get db %v", err)
		}
		if val != nil {
			t.Errorf("expected nil value for deleted keys")
		}
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestSnaphot(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	database, err := kv.NewDatabase(context.Background(), dataDir)
	if err != nil {
		t.Fatal(err)
	}

	defer database.Shutdown()
	rand.Seed(time.Now().UnixNano())
	keyValuePair := make(map[string][]byte)
	count := 10000
	// add 10 key value pair
	for i := 0; i < count; i++ {
		keyValuePair[randSeq(10)] = []byte(randSeq(30 + i))
	}

	// set some value
	for k, v := range keyValuePair {
		err := database.SetB([]byte(k), v)
		if err != nil {
			t.Errorf("err set db %v", err)
		}
	}

	ch := database.SnapshotItems()

	keyCount := 0

	// read kv item from channel
	for {

		dataItem := <-ch
		if dataItem.Key == nil {
			break
		}

		keyCount = keyCount + 1
	}
	if keyCount != count {
		t.Errorf("expected keys in snapshot %d got %d", count, keyCount)
	}
}
