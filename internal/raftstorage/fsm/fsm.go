package fsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/ankur-anand/dis-db/internal/raftstorage/api"
	"github.com/ankur-anand/dis-db/proto/v1/raftkv"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// KV interface defines the contracts between FSM
// and underlying storage for write operation.
type KV interface {
	SetB([]byte, []byte) error
	DelB([]byte) error
}

// RaftFSM provides the state machine implementation for raft
type RaftFSM struct {
	kv                     KV
	fsmSnapshotConstructor func() (raft.FSMSnapshot, error)
}

// NewRaftFSM returns an initialized state machine for raft
func NewRaftFSM(kv KV, fsmc func() (raft.FSMSnapshot, error)) RaftFSM {
	return RaftFSM{kv: kv, fsmSnapshotConstructor: fsmc}
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (r RaftFSM) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		cmd := raftkv.LogEntryData{}
		err := proto.Unmarshal(log.Data, &cmd)
		if err != nil {
			return api.FSMApplyResponse{
				Error: err,
			}
		}

		switch cmd.GetOp() {
		case raftkv.LogEntryData_SET:
			return api.FSMApplyResponse{
				Error: r.kv.SetB(cmd.GetKey(), cmd.GetValue()),
			}
		case raftkv.LogEntryData_DELETE:
			return api.FSMApplyResponse{
				Error: r.kv.DelB(cmd.GetKey()),
			}
		}
		return api.FSMApplyResponse{
			Error: api.ErrUnprocessableEntity,
		}

	}

	return api.FSMApplyResponse{
		Error: api.ErrInternalServer,
	}

}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (r RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return r.fsmSnapshotConstructor()
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (r RaftFSM) Restore(irc io.ReadCloser) error {
	defer irc.Close()

	// read file
	hob := make([]byte, 4)
	// Recommended max Protobuf message size (1 MB)
	msgBuf := make([]byte, 1024*1024*1) // 1 mb
	for {
		if _, err := io.ReadFull(irc, hob); err != nil {
			if err == io.EOF {
				break
			}
			log.Println("snapshot cannot be restored", err)
			return err
		}

		msgLen := int(binary.BigEndian.Uint32(hob))

		if msgLen < 0 {
			return fmt.Errorf("snapshot cannot be restored")
		}

		if msgLen > len(msgBuf) {
			msgBuf = make([]byte, msgLen)
		}

		_, err := io.ReadFull(irc, msgBuf[:msgLen])
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		item := &raftkv.SnapshotItem{}
		err = proto.Unmarshal(msgBuf[:msgLen], item)

		if err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}

		// apply item to the underlying db.
		r.kv.SetB(item.Key, item.Value)
	}

	return nil
}
