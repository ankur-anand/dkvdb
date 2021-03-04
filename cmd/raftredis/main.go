package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ankur-anand/dkvdb"
	"github.com/ankur-anand/dkvdb/proto/v1/config"
	"github.com/tidwall/redcon"
)

var daddr = ":6380"

// All of the command lime flags.

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetPrefix("dkvdb ")
	// redis options
	addr := flag.String("addr", daddr, "address on which redis server should listen")
	joinAddress := flag.String("join", "127.0.0.1:7000", "join address for raft cluster")
	node := flag.String("node", "", "raft node name")
	bootstrap := flag.Bool("bootstrap", false, "start as raft cluster")
	datapath := flag.String("volumedir", "", "volume path to persist raft and kv data")

	flag.Parse()
	nc := config.RaftStorageConfig{
		NodeID:          *node,
		DataPath:        *datapath,
		RaftVolumeDir:   *datapath,
		RaftTCPBindAddr: *joinAddress,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	ns := start(ctx, *bootstrap, &nc)

	mux, err := dkvdb.NewRedisKVStorage(ns)
	check(err)

	go func() {
		log.Printf("started redis server at %s", *addr)

		err = redcon.ListenAndServe(*addr,
			mux.MUX.ServeRESP,
			func(conn redcon.Conn) bool {
				// use this function to accept or deny the connection.
				log.Printf("accept: %s", conn.RemoteAddr())
				return true
			},
			func(conn redcon.Conn, err error) {
				// this is called when the connection has been closed
				log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
			},
		)
		if err != nil {
			check(err)
		}

	}()
	<-c
	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	log.Println("server closing")
	go ns.Shutdown()
	<-ctx.Done()
}

func start(ctx context.Context, bootstrap bool, nc *config.RaftStorageConfig) *dkvdb.NodeStorage {
	ns, err := dkvdb.NewNodeStorage(ctx, nc)
	check(err)
	trans, err := dkvdb.NewRaftTCPTransport(nc)
	check(err)
	ns.WithTransport(trans.LocalAddr(), trans)
	err = ns.StartRaft(ctx)
	check(err)
	if bootstrap {
		// safe to bootstrap.
		err := ns.BootstrapThisNode()
		if err != nil {
			log.Println("error while bootstraping, if clean bootstrap this is fatal")
		}
	}
	return ns
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
