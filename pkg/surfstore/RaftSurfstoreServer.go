package surfstore

import (
	context "context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	id            int64

	metaStore *MetaStore

	raftAddrs   []string
	blockAddrs  []string
	commitIndex int64
	lastApplied int64
	nextIndex   []int64
	matchIndex  []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isLeader {
		if !s.isCrashed {
			for {
				succ, err := s.SendHeartbeat(ctx, empty)
				checkError(err)
				if succ.Flag {
					break
				}
				// majority of servers are crashed
			}
			metaMap, err := s.metaStore.GetFileInfoMap(ctx, empty)
			checkError(err)
			//fmt.Printf("Made it here\n")
			return metaMap, ctx.Err()
		} else { // leader is crashed
			//fmt.Printf("%d is crashed\n", s.id)
			return nil, ERR_SERVER_CRASHED
		}
	} else {
		//fmt.Printf("%d is not the leader\n", s.id)
	}
	return nil, ERR_NOT_LEADER
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if s.isLeader {
		if !s.isCrashed {
			var empty *emptypb.Empty
			for {
				succ, err := s.SendHeartbeat(ctx, empty)
				checkError(err)
				if succ.Flag {
					break
				}
			}

			var blockStoreMap = BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}
			for _, blockHash := range hashes.Hashes {
				responsibleServer := s.metaStore.ConsistentHashRing.GetResponsibleServer(blockHash)
				responsibleServer = strings.Replace(responsibleServer, "blockstore", "", -1)

				blockHashesForServer := blockStoreMap.BlockStoreMap[responsibleServer]
				if blockHashesForServer == nil {
					var temp = BlockHashes{Hashes: make([]string, 0)}
					blockHashesForServer = &temp
				}
				blockHashesForServer.Hashes = append(blockHashesForServer.Hashes, blockHash)
				blockStoreMap.BlockStoreMap[responsibleServer] = blockHashesForServer
			}
			return &blockStoreMap, ctx.Err()
		} else { // leader is crashed
			return nil, ERR_SERVER_CRASHED
		}
	}
	return nil, ERR_NOT_LEADER
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if s.isLeader {
		if !s.isCrashed {
			for {
				succ, err := s.SendHeartbeat(ctx, empty)
				checkError(err)
				if succ.Flag {
					break
				}
			}
			var blockStoreAddrs = BlockStoreAddrs{BlockStoreAddrs: s.metaStore.BlockStoreAddrs}
			return &blockStoreAddrs, ctx.Err()
		} else { // leader is crashed
			return nil, ERR_SERVER_CRASHED
		}
	}
	return nil, ERR_NOT_LEADER
}

func print_state(s *RaftSurfstore) {
	fmt.Printf("id: %d, isLeader: %t, term: %d, log len: %d,\n raftAddrs len: %d, blockAddrs len: %d, commit index: %d, last applied idx: %d,\nnext index: %v, match index: %v\n", s.id, s.isLeader, s.term, len(s.log), len(s.raftAddrs), len(s.blockAddrs), s.commitIndex, s.lastApplied, s.nextIndex, s.matchIndex)
	meta, exist := s.metaStore.FileMetaMap["multi_file1.txt"]

	if !exist {
		fmt.Printf("id: %d. exist: %t\n", s.id, exist)
	} else {
		fmt.Printf("id: %d. exist: %t. meta: %v\n", s.id, exist, meta)
	}
	fmt.Printf("\n")
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isLeader {
		if !s.isCrashed {
			fmt.Printf("%d. Recieved update meta: %v\n", s.id, filemeta)
			var empty *emptypb.Empty
			for { // loop until a majority of the servers are not crashed
				succ, err := s.SendHeartbeat(ctx, empty)
				checkError(err)
				if succ.Flag {
					break
				}
			}
			if s.metaStore.FileMetaMap == nil {
				s.metaStore.FileMetaMap = make(map[string]*FileMetaData)
			}

			//fmt.Printf("%d. RaftServer UpdateFile() finished heartbeat\n", s.id)

			var version = Version{Version: -1}

			// check for hash existance
			remoteFileInfo, remoteFileExist := s.metaStore.FileMetaMap[filemeta.Filename]
			//fmt.Printf("%d. RaftServer UpdateFile() remoteFileExist: %t\n", s.id, remoteFileExist)

			// valid update
			if !remoteFileExist || (remoteFileExist && filemeta.Version == remoteFileInfo.Version+1) {
				var updateOperation = UpdateOperation{Term: s.term, FileMetaData: filemeta}
				s.log = append(s.log, &updateOperation) // apply to state machine
				s.commitIndex = int64(len(s.log) - 1)
				//fmt.Printf("%d. RaftServer UpdateFile() s.log.len: %d: \n", s.id, len(s.log))
			} else { // invalid update
				return &version, ctx.Err()
			}

			// issue 2-phase commit to followers
			for {
				appendSuccesses := 1
				for idx, raftServerIp := range s.raftAddrs {
					if raftServerIp == s.raftAddrs[s.id] {
						continue
					}

					// connect to the other raft server
					conn, err := grpc.Dial(raftServerIp, grpc.WithInsecure())
					if err != nil {
						return nil, err
					}
					c := NewRaftSurfstoreClient(conn)

					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()

					prevLogIndex := s.lastApplied
					prevLogTerm := s.log[s.lastApplied].Term
					//fmt.Printf("%d. RaftServer UpdateFile() start 2-phase commit to %d.\n", s.id, idx)

					//print_state(s)

					for {
						var appendEntryInput = AppendEntryInput{Term: s.term, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
							Entries: s.log, LeaderCommit: s.commitIndex}

						appendEntryResponse, err := c.AppendEntries(ctx, &appendEntryInput)
						if err != nil {
							break
						}
						//checkError(err)
						if appendEntryResponse.Success {
							appendSuccesses++
							s.matchIndex[idx] = appendEntryResponse.MatchedIndex
							s.nextIndex[idx] = int64(len(s.log))
							//fmt.Printf("%d. RaftServer UpdateFile() applied appendEntry to %d successfully\n", s.id, idx)
							break
						} else {
							if prevLogIndex > 0 {
								prevLogIndex--
								prevLogTerm = s.log[prevLogIndex].Term
								s.nextIndex[idx] = int64(prevLogIndex)
							} else {
								break
							}
						}
					}
				}

				// Commit update if majority of servers agreed
				if appendSuccesses >= int(math.Ceil(float64(len(s.raftAddrs))/2.0)) {
					//fmt.Printf("%d. Apply commit to log. appendSuccesses: %d\n", s.id, appendSuccesses)
					// log update in local log
					s.metaStore.FileMetaMap[filemeta.Filename] = filemeta
					version.Version = filemeta.Version
					s.lastApplied = int64(len(s.log) - 1)
					//s.SendHeartbeat(ctx, empty)
					return &version, ctx.Err()
				} // otherwise restart and try to get a majority again
			}
		} else { // leader is crashed
			return nil, ERR_SERVER_CRASHED
		}
	}
	return nil, ERR_NOT_LEADER
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	var output = AppendEntryOutput{Term: s.term, Success: true}

	if s.isCrashed {
		output.Success = false
		//fmt.Printf("%d append entries false 1\n", s.id)
		return &output, ERR_SERVER_CRASHED
	}

	// is heartbeat
	/*if len(input.Entries) == 0 {
		if input.Term > s.term {
			s.term = input.Term
			if s.isLeader {
				s.isLeader = false
				//fmt.Printf("%d stepping down from the leader for term %d\n", s.id, s.term)
			}
		}


		print_state(s)
	} else */

	if !s.isCrashed { // Apply commits
		if input.Term > s.term {
			s.term = input.Term
			if s.isLeader {
				s.isLeader = false
				//fmt.Printf("%d stepping down from the leader for term %d\n", s.id, s.term)
			}
		}

		for {
			if s.commitIndex > s.lastApplied || (s.commitIndex == 0 && s.lastApplied == 0 && len(s.log) == 1) { //|| (len(s.log) == 1 && s.commitIndex == 0 && first_iter) {
				if s.lastApplied > 0 || s.commitIndex > 0 {
					s.lastApplied = s.lastApplied + 1
				}
				fmt.Printf("%d. Applied commit to log. s.commitIndex: %d. s.lastApplied: %d. len(s.log): %d\n", s.id, s.commitIndex, s.lastApplied, len(s.log))
				filemeta := s.log[s.lastApplied].FileMetaData
				fmt.Printf("%d. Log commit (%d) meta: %v\n", s.id, s.lastApplied, filemeta)
				s.metaStore.FileMetaMap[filemeta.Filename] = filemeta

				if s.lastApplied == 0 {
					break
				}
			} else {
				break
			}
		}
	}

	if len(input.Entries) != 0 && !s.isCrashed {
		if input.Term > s.term {
			s.term = input.Term
		}

		if input.Term < s.term {
			output.Success = false
			//fmt.Printf("%d append entries false 2\n", s.id)
		} else if len(s.log) < int(input.PrevLogIndex) {
			output.Success = false
			//fmt.Printf("%d append entries false 3\n", s.id)
		} else if len(s.log) > 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			output.Success = false
			//fmt.Printf("%d append entries false 4\n", s.id)
		} else {
			for idx := input.PrevLogIndex; idx < int64(len(input.Entries)); idx++ {
				inputItem := input.Entries[idx]
				if int64(len(s.log)) <= idx {
					s.log = append(s.log, inputItem)
					//fmt.Printf("server: %d. appending log. New length: %d\n", s.id, len(s.log))
					output.MatchedIndex = idx
				} else if s.log[idx] != inputItem {
					s.log[idx] = inputItem
					output.MatchedIndex = idx
					//fmt.Printf("server: %d. Changed log. New length: %d\n", s.id, len(s.log))
				}
			}
		}
	}

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = input.LeaderCommit

		if int64(len(s.log)-1) < input.LeaderCommit {
			s.commitIndex = int64(len(s.log) - 1)
		}
		//		fmt.Printf("server: %d. Commit index changed: %d\n", s.id, s.commitIndex)
	}

	/*if s.isCrashed {
		return &output, ERR_SERVER_CRASHED
	}*/

	//(s)
	//fmt.Printf("\n")
	return &output, ctx.Err()
}

func (s *RaftSurfstore) check_if_need_to_increment_term() bool {
	increment := false

	if s.term == 0 {
		increment = true
	} else {
		if len(s.log) > 0 {
			for _, log := range s.log {
				if (*log).Term == s.term {
					increment = true
				}
			}
		}
	}

	return increment
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, empty *emptypb.Empty) (*Success, error) {
	//if s.check_if_need_to_increment_term() {
	s.term++
	//}
	s.isLeader = true
	s.nextIndex = make([]int64, len(s.raftAddrs))
	s.matchIndex = make([]int64, len(s.raftAddrs)) // defaults to 0

	for idx := range s.nextIndex {
		s.nextIndex[idx] = s.lastApplied + 1
	}

	//_, err := s.SendHeartbeat(ctx, empty)
	//checkError(err)
	//fmt.Printf("%d is now the leader for term %d\n", s.id, s.term)
	return &Success{Flag: true}, ctx.Err()
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	print_state(s)
	//var appendEntryInput = AppendEntryInput{Term: s.term, Entries: make([]*UpdateOperation, 0)}

	respondedServers := 1 // automatically call self
	flag := false

	for idx, raftServerIp := range s.raftAddrs {
		if raftServerIp == s.raftAddrs[s.id] {
			continue
		}

		conn, err := grpc.Dial(raftServerIp, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		prevLogIndex := int64(0)
		prevLogTerm := int64(0)
		if len(s.log) > 0 {
			prevLogIndex = s.lastApplied
			prevLogTerm = s.log[s.lastApplied].Term
		}

		for {
			var appendEntryInput = AppendEntryInput{Term: s.term, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
				Entries: s.log, LeaderCommit: s.commitIndex}

			appendEntryResponse, err := c.AppendEntries(ctx, &appendEntryInput)
			if err != nil {
				break
			}
			//checkError(err)
			if appendEntryResponse.Success {
				respondedServers++
				s.matchIndex[idx] = appendEntryResponse.MatchedIndex
				s.nextIndex[idx] = int64(len(s.log))
				//fmt.Printf("%d. RaftServer UpdateFile() applied appendEntry to %d successfully\n", s.id, idx)
				break
			} else {
				if prevLogIndex > 0 {
					prevLogIndex--
					prevLogTerm = s.log[prevLogIndex].Term
					s.nextIndex[idx] = int64(prevLogIndex)
				} else {
					break
				}
			}
		}

		conn.Close() // close the connection
	}
	if respondedServers >= int(math.Ceil(float64(len(s.raftAddrs))/2.0)) {
		flag = true
	}

	return &Success{Flag: flag}, ctx.Err()
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
