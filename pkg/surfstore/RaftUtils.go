package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}
	consistentHashRing := NewConsistentHashRing(config.BlockAddrs)

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs, consistentHashRing),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		id:             id,
		raftAddrs:      config.RaftAddrs,
		blockAddrs:     config.BlockAddrs,
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int64, len(config.RaftAddrs)),
		matchIndex:     make([]int64, len(config.RaftAddrs)),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {

	listener, err := net.Listen("tcp", server.raftAddrs[server.id])
	if err != nil {
		log.Printf("Error listening: %s\n", err.Error())
		return err
	}
	defer listener.Close()
	grpcserver := grpc.NewServer()

	RegisterRaftSurfstoreServer(grpcserver, server)
	if err := grpcserver.Serve(listener); err != nil {
		log.Printf("failed to serve: %v", err)
	}

	return err
}
