package dkvdb

import (
	"fmt"

	"github.com/tidwall/redcon"
)

// RedisKV wraps the provided distributed kv storage
// over redis protocol.
type RedisKV struct {
	mux *redcon.ServeMux
	ns  *NodeStorage
}

func (rr *RedisKV) sethandler() {
	rr.mux.HandleFunc("ping", rr.Ping)
	rr.mux.HandleFunc("quit", rr.Quit)
	rr.mux.HandleFunc("set", rr.Set)
	rr.mux.HandleFunc("get", rr.Get)
	rr.mux.HandleFunc("del", rr.Delete)
}

// NewRedisKVStorage returns an initialized Redis Handler
// over the distributed Node storage.
func NewRedisKVStorage(ns *NodeStorage) (*RedisKV, error) {
	if ns == nil {
		return nil, fmt.Errorf("provided nodestorage is nil")
	}
	return &RedisKV{mux: redcon.NewServeMux(), ns: ns}, nil
}

// Ping responds to ping redis protocol
func (rr *RedisKV) Ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

// Quit closes the corrent TCP redis onn
func (rr *RedisKV) Quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

// Set the key value from redis conn to the database
func (rr *RedisKV) Set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	err := rr.ns.Set(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

// Get returns the value if any for the given key to redis conn
func (rr *RedisKV) Get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	val, err := rr.ns.Get(cmd.Args[1])

	if err != nil {
		conn.WriteError(err.Error())
		return

	}

	if len(val) == 0 {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(val)
}

// Delete the key from redis conn from the database.
func (rr *RedisKV) Delete(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	val, err := rr.ns.Get(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return

	}
	err = rr.ns.Del(cmd.Args[1])

	if err != nil {
		conn.WriteError(err.Error())
		return

	}

	if len(val) == 0 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}
