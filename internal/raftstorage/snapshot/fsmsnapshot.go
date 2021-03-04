package snapshot

import (
	"encoding/binary"
	"log"

	"github.com/ankur-anand/dkvdb/proto/v1/raftkv"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type snapshotter interface {
	SnapshotItems() <-chan *raftkv.SnapshotItem
}

// FSMSnapshot implements the raft snapshot persistance layer
type FSMSnapshot struct {
	snap snapshotter
}

// NewFSMSnapshot returns a new initialized FSMSnapshot Implementation
func NewFSMSnapshot(snap snapshotter) *FSMSnapshot {
	return &FSMSnapshot{snap: snap}
}

// Persist data in specific type
// kv item serialize in google protubuf
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close() // call close on completion

	ch := f.snap.SnapshotItems()

	keyCount := 0

	// read kv item from channel
	for {

		dataItem := <-ch
		if dataItem.Key == nil {
			break
		}
		// length prefix framing protobuf.
		msgLen := proto.Size(dataItem)
		lenBuf := make([]byte, 4) // limit size to 4gb.
		binary.BigEndian.PutUint32(lenBuf, uint32(msgLen))
		keyCount = keyCount + 1
		// send high order bytes first.
		if _, err := sink.Write(lenBuf); err != nil {
			sink.Cancel()
			return err
		}
		// encode message
		// if large happens we can use buffer pool.
		buff, err := proto.Marshal(dataItem)
		if err != nil {
			sink.Cancel()
			return err
		}
		if _, err := sink.Write(buff); err != nil {
			sink.Cancel()
			return err
		}

	}
	log.Printf("Total keys persisted %d", keyCount)

	return nil
}

// Release is invoked when we are finished with the snapshot.
func (f *FSMSnapshot) Release() {
	log.Printf("Snapshot finished")
}
