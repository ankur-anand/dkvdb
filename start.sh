#!/bin/bash
mkdir -p /tmp/dkdv/
node_number=$1
#rm -rf /tmp/dkdv/
function usage() {
    echo "Usage:"
    echo "   run.sh nodeNumber(1-5)"
}

raft_addr=$((6999 + node_number))
redis_addr=$((6379 + node_number))
node="node${node_number}"
volumedir="/tmp/dkdv/node${node_number}"

args="${args} -addr 127.0.0.1:${redis_addr}"
args="${args} -join 127.0.0.1:${raft_addr}"
args="${args} -node ${node}"
args="${args} -volumedir ${volumedir}"

if [ "${node_number}" == 1 ] ; then
    args="${args} -bootstrap true"
fi

echo${args}
go run ./cmd/raftredis/main.go ${args}