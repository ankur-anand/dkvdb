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
		err := database.Set(k, v)
		if err != nil {
			t.Errorf("err set db %v", err)
		}
	}

	// get some value
	for k, v := range keyValuePair {
		val, err := database.Get(k)
		if err != nil {
			t.Errorf("err get db %v", err)
		}
		if !bytes.Equal(v, val) {
			t.Errorf("expected %s got %s", string(v), string(val))
		}
	}

	// del some value
	for k := range keyValuePair {
		err := database.Del(k)
		if err != nil {
			t.Errorf("err sel db %v", err)
		}
	}

	for k := range keyValuePair {
		val, err := database.Get(k)
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
