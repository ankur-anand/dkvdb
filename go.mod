module github.com/ankur-anand/dkvdb

go 1.15

replace github.com/hashicorp/raft-boltdb => github.com/ankur-anand/raft-boltdb v0.0.0-20210304100614-ce7523396a29

require (
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/golang/protobuf v1.4.1
	github.com/hashicorp/raft v1.2.0
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
	github.com/tidwall/redcon v1.4.0
	google.golang.org/protobuf v1.25.0
)
