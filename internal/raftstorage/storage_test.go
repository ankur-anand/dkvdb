package raftstorage_test

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/dis-db/internal/raftstorage"
	"github.com/ankur-anand/dis-db/proto/v1/config"
	"github.com/hashicorp/raft"
)

func TestNewStorage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tcases := []struct {
		name string
		nc   *config.RaftStorageConfig
		err  bool
	}{
		{
			name: "missing DataPath should throw validation error",
			nc:   &config.RaftStorageConfig{DataPath: ""},
			err:  true,
		},
		{
			name: "missing RaftVolumeDir should throw validation error",
			nc:   &config.RaftStorageConfig{DataPath: dir},
			err:  true,
		},
		{
			name: "missing NodeID should throw validation error",
			nc:   &config.RaftStorageConfig{DataPath: dir, RaftVolumeDir: dir},
			err:  true,
		},
		{
			name: "should have sane default and no error",
			nc:   &config.RaftStorageConfig{DataPath: dir, RaftVolumeDir: dir, NodeID: "test1"},
			err:  false,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			ns, err := raftstorage.NewNodeStorage(context.Background(), tc.nc)
			if tc.err && err == nil {
				t.Errorf("expected validation error got nil")
			}
			if !tc.err && err != nil {
				t.Errorf("expected no validation error %v", ns)
			}
			// TODO: CHECK FOR SANE DEAFULT CASES
			if err != nil {

			}
		})
	}
}
func TestGetSetDel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	nc := &config.RaftStorageConfig{DataPath: dir, RaftVolumeDir: dir, NodeID: "test1"}
	ns, err := raftstorage.NewNodeStorage(context.Background(), nc)
	if err != nil {
		t.Errorf("expected nil error got %v", err)
	}
	defer ns.Shutdown()
	ns.WithTransport(raft.NewInmemTransport("127.0.0.1:7000"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = ns.StartRaft(ctx)
	if err != nil {
		t.Errorf("expected nil error got %v", err)
	}

	// Write Should not be allowed for follower
	err = ns.Set([]byte("hello"), []byte("world"))
	if err == nil {
		t.Errorf("expected write to return error on followers")
	}
	err = ns.Del([]byte("hello"))
	if err == nil {
		t.Errorf("expected write to return error on followers")
	}

	// Get should not have any effect
	_, err = ns.Get([]byte("hello"))
	if err != nil {
		t.Errorf("expected read to return no error on followers got %v", err)
	}

probeL1:
	for {
		select {
		case <-time.After(2 * time.Second):
			if ns.IsLeader() {
				t.Errorf("node should not transit into leader state")
				break probeL1
			}
			break probeL1
		}

	}
	//fmt.Println(ns.HasExistingState())
	err = ns.BootstrapThisNode()
	if err != nil {
		t.Errorf("expected cluster to boostrap without issue for clean state")
	}
	// watch to become leader
probeL2:
	for {
		select {
		case <-time.After(10 * time.Second):
			break probeL2
		default:
			if ns.IsLeader() {
				break probeL2
			}
		}

	}

	// Write Should not be allowed for follower
	err = ns.Set([]byte("hello"), []byte("world"))
	if err != nil {
		t.Errorf("expected write to return error on followers")
	}

	// Get should not have any effect
	res, err := ns.Get([]byte("hello"))
	if err != nil {
		t.Errorf("expected read to return no error on followers got %v", err)
	}
	if string(res) != "world" {
		t.Errorf("expected 'hello' key to have value 'world' got %v", string(res))
	}
	err = ns.Del([]byte("hello"))
	if err != nil {
		t.Errorf("expected write to return error on followers")
	}
}
