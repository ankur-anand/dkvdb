package raftstorage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/dis-db/internal/raftstorage/api"
	"github.com/ankur-anand/dis-db/internal/raftstorage/fsm"
	"github.com/ankur-anand/dis-db/internal/raftstorage/kv"
	"github.com/ankur-anand/dis-db/internal/raftstorage/snapshot"
	"github.com/ankur-anand/dis-db/proto/v1/config"
	pkv "github.com/ankur-anand/dis-db/proto/v1/raftkv"
	"github.com/hashicorp/raft"
	boltstore "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

var (
	errNotLeader      = errors.New("not the leader")
	errInternalServer = errors.New("internal server error")
)

const (
	isLeader    = int32(1)
	isNotLeader = int32(0)
)

const (
	defaultApplyTimeout     = 1000 // in millisecond
	logStoreFile            = "raftlogstore.bolt"
	stableStoreFile         = "stablestore.bolt"
	defaultSnapshotRetain   = 5
	deafultSnapshotInterval = 20
	defaultLogCacheSize     = 1024
	defaultRaftTCPmaxPool   = 3
	deafultRaftTCPIOTimeout = 30 * time.Second
)

// NodeStorage defines individual node of a kv database.
type NodeStorage struct {
	// store initialized raft
	kv            kv.Database
	fsm           fsm.RaftFSM
	raft          *raft.Raft
	nc            *config.RaftStorageConfig
	raftTransport raft.Transport
	localAddr     raft.ServerAddress
	serverID      raft.ServerID
	raftStorage   raftStorage
	isLeader      int32
}

// ValidateRequired Checks if the given required Field has been
// set with proper value or not.
func ValidateRequired(nc *config.RaftStorageConfig) error {
	if nc.DataPath == "" {
		return fmt.Errorf("mandatory path to persist kv missing")
	}
	if nc.RaftVolumeDir == "" {
		return fmt.Errorf("mandatory voulme to persist raft missing")
	}
	if nc.NodeID == "" {
		return fmt.Errorf("mandatory node for node is missing")
	}
	return nil
}

// ApplyDefaultState validates the given Config Parameter and Applies the
// sane default state if not present.
func ApplyDefaultState(nc *config.RaftStorageConfig) {
	if nc.ApplyTimeOutMS == 0 {
		nc.ApplyTimeOutMS = defaultApplyTimeout
	}
	if nc.SnapRetainNo == 0 {
		nc.SnapRetainNo = defaultSnapshotRetain
	}
	if nc.SnapshotIntervalSec == 0 {
		nc.SnapshotIntervalSec = deafultSnapshotInterval
	}
	if nc.LogCacheSize == 0 {
		nc.LogCacheSize = defaultLogCacheSize
	}
}

type raftStorage struct {
	snapshotStore *raft.FileSnapshotStore
	logCache      *raft.LogCache
	logStore      raft.LogStore
	stableStore   raft.StableStore
}

func newRaftStorage(nc *config.RaftStorageConfig) (raftStorage, error) {
	rs := raftStorage{}

	snapshotStore, err := raft.NewFileSnapshotStore(nc.RaftVolumeDir, int(nc.SnapRetainNo), os.Stdout)
	if err != nil {
		return rs, err
	}
	rs.snapshotStore = snapshotStore
	logStore, err := boltstore.NewBoltStore(filepath.Join(nc.RaftVolumeDir, logStoreFile))
	if err != nil {
		return rs, err
	}

	// Reduce Disk IO for Log
	cacheStore, err := raft.NewLogCache(int(nc.LogCacheSize), logStore)
	if err != nil {
		return rs, err
	}
	rs.logCache = cacheStore

	stableStore, err := boltstore.NewBoltStore(filepath.Join(nc.RaftVolumeDir, stableStoreFile))
	if err != nil {
		return rs, err
	}
	rs.stableStore = stableStore
	return rs, nil
}

// NewNodeStorage returns an initialized Node Storage,
func NewNodeStorage(ctx context.Context, nc *config.RaftStorageConfig) (*NodeStorage, error) {
	ns := &NodeStorage{}
	ns.nc = nc
	if err := ValidateRequired(nc); err != nil {
		return ns, err
	}

	ApplyDefaultState(nc)

	kv, err := kv.NewDatabase(ctx, nc.DataPath)
	if err != nil {
		return ns, err
	}
	ns.kv = kv

	fsm := fsm.NewRaftFSM(kv, func() (raft.FSMSnapshot, error) {
		return snapshot.NewFSMSnapshot(kv), nil
	})

	ns.fsm = fsm

	rs, err := newRaftStorage(nc)
	if err != nil {
		return ns, err
	}
	ns.raftStorage = rs

	return ns, nil
}

// StartRaft is used to construct a new Raft node.
func (ns *NodeStorage) StartRaft(ctx context.Context) error {
	if ns.raftTransport == nil {
		return fmt.Errorf("raft transport missing")
	}
	ns.serverID = raft.ServerID(ns.nc.NodeID)
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = ns.serverID
	raftConf.SnapshotInterval = time.Duration(ns.nc.SnapshotIntervalSec) * time.Second

	// for reliable leader notifications.
	raftNotifyCh := make(chan bool, 10)
	raftConf.NotifyCh = raftNotifyCh
	raft, err := raft.NewRaft(raftConf, ns.fsm, ns.raftStorage.logCache, ns.raftStorage.stableStore, ns.raftStorage.snapshotStore, ns.raftTransport)

	if err != nil {
		return err
	}
	ns.raft = raft
	go ns.monitorLeadership(ctx)
	return nil
}

// WithTransport attaches the supplied transport to raft cluster.
func (ns *NodeStorage) WithTransport(la raft.ServerAddress, rt raft.Transport) {
	ns.localAddr = rt.LocalAddr()
	ns.raftTransport = rt
}

// NewRaftTCPTransport returns a new initailzed raft TCP Network transport
func NewRaftTCPTransport(nc *config.RaftStorageConfig) (*raft.NetworkTransport, error) {
	advertiseAddr, err := net.ResolveTCPAddr("tcp", nc.RaftTCPBindAddr)
	if err != nil {
		return nil, err
	}

	return raft.NewTCPTransport(nc.RaftTCPBindAddr, advertiseAddr, defaultRaftTCPmaxPool, deafultRaftTCPIOTimeout, os.Stdout)
}

// Get returns the value of the given key in local db.
// The values will be eventaully consistent. So it can be stale value.
func (ns *NodeStorage) Get(key []byte) ([]byte, error) {
	// directly read from the underlying database.
	// reading for eventual consistency
	return ns.kv.GetB(key)
}

// Set tries to store the given key value pair from the database.
func (ns *NodeStorage) Set(key, val []byte) error {
	// if not leader should not accept any write.
	if !ns.IsLeader() {
		return errNotLeader
	}
	// try protobuff
	cmd := pkv.LogEntryData{Op: pkv.LogEntryData_SET, Key: key, Value: val}
	b, err := proto.Marshal(&cmd)
	if err != nil {
		return err
	}
	return ns.tryQuorum(b)
}

// Del tries to delete the provided key from the database.
func (ns *NodeStorage) Del(key []byte) error {
	// if not leader should not accept any write.
	if !ns.IsLeader() {
		return errNotLeader
	}
	// try protobuff
	cmd := pkv.LogEntryData{Op: pkv.LogEntryData_DELETE, Key: key}
	b, err := proto.Marshal(&cmd)
	if err != nil {
		return err
	}
	return ns.tryQuorum(b)
}

func (ns *NodeStorage) tryQuorum(cmd []byte) error {
	// Apply is async.
	af := ns.raft.Apply(cmd, time.Duration(ns.nc.ApplyTimeOutMS)*time.Millisecond)
	// poll over future.
	if err := af.Error(); err != nil {
		return fmt.Errorf("error persisting data in raft cluster: %s", err.Error())
	}

	// check the error response
	res, ok := af.Response().(api.FSMApplyResponse)
	if !ok {
		return errInternalServer
	}

	return res.Error
}

// BootstrapThisNode will try to bootsrap this server.
// Check for the state before Bootstraping
// A cluster can only be bootstrapped once from a single participating Voter server.
// Any further attempts to bootstrap will return an error that can be safely ignored.
func (ns *NodeStorage) BootstrapThisNode() error {
	log.Println("Bootstarpping raft node")
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      ns.serverID,
				Address: ns.localAddr,
			},
		},
	}
	rf := ns.raft.BootstrapCluster(configuration)
	return rf.Error()
}

// IsLeader resports whether the given node is in leader state.
func (ns *NodeStorage) IsLeader() bool {
	return atomic.LoadInt32(&ns.isLeader) == isLeader
}

func (ns *NodeStorage) setLeaderFlag(flag bool) {
	if flag {
		atomic.StoreInt32(&ns.isLeader, isLeader)
	} else {
		atomic.StoreInt32(&ns.isLeader, isNotLeader)
	}
}

func (ns *NodeStorage) monitorLeadership(ctx context.Context) {
	// monitor leadership
	for {
		select {
		case leader := <-ns.raft.LeaderCh():
			if leader {
				log.Println("become leader, except write")
				ns.setLeaderFlag(true)
			} else {
				log.Println("become follower, close write")
				ns.setLeaderFlag(false)
			}
		case <-ctx.Done():
			return
		default:
		}
	}
}

// Shutdown this raft node.
func (ns *NodeStorage) Shutdown() {
	fs := ns.raft.Shutdown()
	if fs.Error() != nil {
		log.Printf("raft shutdown failed %v", fs.Error())
	}
	err := ns.kv.Shutdown()
	if err != nil {
		log.Printf("db shutdown failed %v", err)
	}
}
