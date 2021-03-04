package fsm_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ankur-anand/dkvdb/internal/raftstorage/api"
	"github.com/ankur-anand/dkvdb/internal/raftstorage/fsm"
	"github.com/ankur-anand/dkvdb/proto/v1/raftkv"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// not goroutine safe kv.
type kv struct {
	db map[string][]byte
}

func (kv *kv) SetB(key, val []byte) error {
	kv.db[string(key)] = val
	return nil
}

func (kv *kv) DelB(key []byte) error {
	delete(kv.db, string(key))
	return nil
}

func TestApply(t *testing.T) {
	t.Parallel()
	kvd := &kv{db: make(map[string][]byte, 10)}
	rFSM := fsm.NewRaftFSM(kvd, NewMockFSMSnapShot)

	cmd := &raftkv.LogEntryData{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Op:    raftkv.LogEntryData_SET,
	}

	pHelp := func(t *testing.T, m proto.Message) api.FSMApplyResponse {
		t.Helper()
		rLog := &raft.Log{}
		rLog.Type = raft.LogCommand
		data, err := proto.Marshal(m)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		rLog.Data = data
		ret := rFSM.Apply(rLog)
		res, ok := ret.(api.FSMApplyResponse)
		if !ok {
			t.Errorf("apply response type inconsistent")
		}
		return res
	}

	res := pHelp(t, cmd)
	if res.Error != nil {
		t.Errorf("expected nil error, got %v", res.Error)
	}
	if string(kvd.db["hello"]) != "world" {
		t.Errorf("Expected key value pair to be 'hello' and 'world'")
	}

	cmd = &raftkv.LogEntryData{
		Key: []byte("hello"),
		Op:  raftkv.LogEntryData_DELETE,
	}
	res = pHelp(t, cmd)
	if res.Error != nil {
		t.Errorf("expected nil error, got %v", res.Error)
	}
	if _, ok := kvd.db["hello"]; ok {
		t.Errorf("Expected key to be deleted")
	}

}

func TestRestore(t *testing.T) {
	t.Parallel()
	// generate snapshot.
	rand.Seed(time.Now().UnixNano())
	keyValuePair := make(map[string][]byte)
	count := 10000
	// add some key value pair
	// used for verification later
	for i := 0; i < count; i++ {
		keyValuePair[randSeq(10)] = []byte(randSeq(30 + i))
	}

	storage := &inmemSnapshotStorage{contents: &bytes.Buffer{}}

	// generate kv pair and write to store
	for k, v := range keyValuePair {
		dataItem := &raftkv.SnapshotItem{Key: []byte(k), Value: []byte(v)}
		msgLen := proto.Size(dataItem)
		lenBuf := make([]byte, 4) // limit size to 4gb.
		binary.BigEndian.PutUint32(lenBuf, uint32(msgLen))
		if _, err := storage.Write(lenBuf); err != nil {
			t.Errorf("error writing snapshot data %v", err)
			t.FailNow()
		}
		buff, err := proto.Marshal(dataItem)
		if err != nil {
			t.Errorf("error marshalling snapshot proto %v", err)
			t.FailNow()
		}
		if _, err := storage.Write(buff); err != nil {
			t.Errorf("error writing snapshot data %v", err)
			t.FailNow()
		}
	}

	restoreKV := &kv{db: make(map[string][]byte, count)}
	rFSM := fsm.NewRaftFSM(restoreKV, NewMockFSMSnapShot)
	rFSM.Restore(storage)

	if storage.totalReads != storage.totalWrites+1 {
		t.Errorf("expected total reads to be one greater than total writes")
	}

	snapshotKeys := make([]string, 0, count)
	for k := range keyValuePair {
		snapshotKeys = append(snapshotKeys, k)
	}

	restoredKeys := make([]string, 0, count)
	for k := range restoreKV.db {
		restoredKeys = append(restoredKeys, k)
	}

	sort.Strings(snapshotKeys)
	sort.Strings(restoredKeys)
	// both the keys slice should be deep equal.
	if !(reflect.DeepEqual(snapshotKeys, restoredKeys)) {
		t.Errorf("expected all keys from snapshots to be restored")
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

// inmemSnapshotStorage
type inmemSnapshotStorage struct {
	totalWrites int
	totalReads  int
	contents    *bytes.Buffer
}

func (s *inmemSnapshotStorage) Write(p []byte) (n int, err error) {
	s.totalWrites++
	return s.contents.Write(p)
}

func (s *inmemSnapshotStorage) Close() error {
	return nil
}

func (s *inmemSnapshotStorage) Read(p []byte) (n int, err error) {
	s.totalReads++
	return s.contents.Read(p)
}

func NewMockFSMSnapShot() (raft.FSMSnapshot, error) {
	return &raft.MockSnapshot{}, nil
}
