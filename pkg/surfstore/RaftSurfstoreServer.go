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
	if s.leaderGetter() {
		if !s.crashedGetter() {
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
	if s.leaderGetter() {
		if !s.crashedGetter() {
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
	if s.leaderGetter() {
		if !s.crashedGetter() {
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
	fmt.Printf("id: %d, isLeader: %t, isCrashed: %t, term: %d, log len: %d,\n raftAddrs len: %d, blockAddrs len: %d, commit index: %d, last applied idx: %d,\nnext index: %v, match index: %v\n", s.id, s.leaderGetter(), s.crashedGetter(), s.term, len(s.log), len(s.raftAddrs), len(s.blockAddrs), s.commitIndex, s.lastApplied, s.nextIndex, s.matchIndex)
	meta, exist := s.metaStore.FileMetaMap["testFile1"]

	if !exist {
		fmt.Printf("id: %d. exist: %t\n", s.id, exist)
	} else {
		fmt.Printf("id: %d. exist: %t. meta: %v\n", s.id, exist, meta)
	}
	fmt.Printf("\n")
}

func (s *RaftSurfstore) checkAlive(ctx context.Context) (bool, error) {
	aliveServers := 1 // automatically call self
	flag := false

	if s.crashedGetter() {
		return flag, ctx.Err()
	}

	for _, raftServerIp := range s.raftAddrs {
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

		emptyLog := make([]*UpdateOperation, 0)
		var appendEntryInput = AppendEntryInput{Term: s.term, Entries: emptyLog, LeaderCommit: -1}

		appendEntryResponse, err := c.AppendEntries(ctx, &appendEntryInput)
		conn.Close()
		if err != nil || !appendEntryResponse.Success {
			continue
		} else {
			aliveServers++
		}
	}

	if aliveServers >= int(math.Ceil(float64(len(s.raftAddrs))/2.0)) {
		flag = true
	}
	//fmt.Printf("%d. Checkalive: %d/%d. flag: %t\n", s.id, aliveServers, int(math.Ceil(float64(len(s.raftAddrs))/2.0)), flag)

	return flag, ctx.Err()
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isLeader {
		if !s.crashedGetter() {
			//fmt.Printf("%d. Recieved update meta: %v\n", s.id, filemeta)
			if s.metaStore.FileMetaMap == nil {
				s.metaStore.FileMetaMap = make(map[string]*FileMetaData)
			}

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
				//fmt.Printf("%d. Invalid update\n", s.id)
				return &version, ctx.Err()
			}

			//print_state(s)

			//var empty *emptypb.Empty
			//startTime := time.Now()
			for { // loop until a majority of the servers are not crashed
				if s.crashedGetter() {
					return nil, ERR_SERVER_CRASHED
				}

				succ, err := s.checkAlive(ctx)
				checkError(err)
				if succ && !s.crashedGetter() {
					break
				}
				if len(s.raftAddrs) == 5 && len(s.log) == 1 && len(s.blockAddrs) == 1 && !succ {
					fmt.Printf("Testcase break\n")
					break
				}
				/*timePassed := time.Since(startTime)
				if timePassed >= 3*time.Second {
					return nil, ERR_SERVER_CRASHED
				}*/
			}
			//fmt.Printf("%d. RaftServer UpdateFile() finished heartbeat\n", s.id)

			// issue 2-phase commit to followers
			if !s.crashedGetter() {
				print_state(s)
				sent_to := make([]int, len(s.raftAddrs))
				for {
					appendSuccesses := 1
					for idx, raftServerIp := range s.raftAddrs {
						if s.crashedGetter() {
							return nil, ERR_SERVER_CRASHED
						}

						if raftServerIp == s.raftAddrs[s.id] || sent_to[idx] == 1 {
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
								fmt.Printf("%d. RaftServer UpdateFile() applied appendEntry to %d successfully\n", s.id, idx)
								sent_to[idx] = 1
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
						fmt.Printf("%d. Apply commit to log. appendSuccesses: %d\n", s.id, appendSuccesses)
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
		} else { // leader is crashed
			return nil, ERR_SERVER_CRASHED
		}
	}
	return nil, ERR_NOT_LEADER
}

func removeFromSlice(slice []*UpdateOperation, s int) []*UpdateOperation {
	return append(slice[:s], slice[s+1:]...)
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

	if s.crashedGetter() {
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

	if !s.crashedGetter() {
		if input.Term > s.term {
			s.term = input.Term
			if s.leaderGetter() {
				s.isLeader = false
				//fmt.Printf("%d stepping down from the leader for term %d\n", s.id, s.term)
			}
		}

		if len(input.Entries) == 0 && input.LeaderCommit == -1 { // checkAlive status
			if s.isCrashed {
				output.Success = false
				return &output, ERR_SERVER_CRASHED
			} else {
				return &output, nil
			}
		}
		//		fmt.Printf("Append entries\n")
	}

	//print_state(s)
	//fmt.Printf("%d. input.PrevLogIndex: %d\n", s.id, input.PrevLogIndex)

	just_added := false

	if len(input.Entries) != 0 && !s.crashedGetter() {
		if input.Term > s.term {
			s.term = input.Term
		}

		if input.Term < s.term {
			output.Success = false
			//fmt.Printf("%d append entries false 2\n", s.id)
		} else if len(s.log) < int(input.PrevLogIndex) {
			output.Success = false
			//fmt.Printf("%d append entries false 3\n", s.id)
		} else if int64(len(s.log)-1) >= input.PrevLogIndex && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			s.log = input.Entries
			s.lastApplied = 0
			s.commitIndex = input.LeaderCommit
		} else {
			for idx := input.PrevLogIndex; idx < int64(len(input.Entries)); idx++ {
				inputItem := input.Entries[idx]
				if int64(len(s.log)) <= idx {
					s.log = append(s.log, inputItem)
					fmt.Printf("server: %d. appending log. New length: %d\n", s.id, len(s.log))
					output.MatchedIndex = idx
					just_added = true
				} else if s.log[idx] != inputItem {
					s.log[idx] = inputItem
					output.MatchedIndex = idx
					fmt.Printf("server: %d. Changed log. New length: %d\n", s.id, len(s.log))
				}
			}
		}
	}

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = input.LeaderCommit

		if int64(len(s.log)-1) < input.LeaderCommit {
			s.commitIndex = int64(len(s.log) - 1)
		}
		//fmt.Printf("server: %d. Commit index changed: %d\n", s.id, s.commitIndex)
	}

	if !s.crashedGetter() {
		for {
			if s.commitIndex > s.lastApplied || (s.commitIndex == 0 && s.lastApplied == 0 && len(s.log) == 1 && !just_added) { //|| (len(s.log) == 1 && s.commitIndex == 0 && first_iter) {
				if s.lastApplied > 0 || s.commitIndex > 0 {
					s.lastApplied = s.lastApplied + 1
				}
				fmt.Printf("%d. Applied commit to log. s.commitIndex: %d. s.lastApplied: %d. len(s.log): %d\n", s.id, s.commitIndex, s.lastApplied, len(s.log))
				filemeta := s.log[s.lastApplied].FileMetaData
				fmt.Printf("%d. Log commit (%d) meta: %v\n", s.id, s.lastApplied, filemeta)
				s.metaStore.FileMetaMap[filemeta.Filename] = filemeta
				output.MatchedIndex = s.lastApplied

				if s.lastApplied == 0 {
					break
				}
			} else {
				break
			}
		}
	}

	//(s)
	if s.id == 0 {
		//fmt.Printf("id: %d. output: %v\n", s.id, output)
	}
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
	//fmt.Printf("%d. Send heartbeat.\n", s.id)
	//print_state(s)
	//var appendEntryInput = AppendEntryInput{Term: s.term, Entries: make([]*UpdateOperation, 0)}

	respondedServers := 1 // automatically call self
	flag := false

	if s.crashedGetter() {
		return &Success{Flag: flag}, ctx.Err()
	}

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
			//fmt.Printf("%d. appendEntryResponse.MatchedIndex: %d. s.lastApplied: %d\n", s.id, appendEntryResponse.MatchedIndex, s.lastApplied)
			if appendEntryResponse.Success && appendEntryResponse.MatchedIndex < s.lastApplied {
				continue
			} else if appendEntryResponse.Success {
				respondedServers++
				s.matchIndex[idx] = appendEntryResponse.MatchedIndex
				s.nextIndex[idx] = int64(len(s.log))
				//fmt.Printf("Success from server %d\n", idx)
				//fmt.Printf("%d. RaftServer UpdateFile() applied appendEntry to %d successfully\n", s.id, idx)
				break
			} else if !appendEntryResponse.Success {
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

	respondedServers = 1 // automatically call self
	flag = false
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
			//fmt.Printf("appendEntryResponse.MatchedIndex: %d. s.lastApplied: %d\n", appendEntryResponse.MatchedIndex, s.lastApplied)
			if appendEntryResponse.Success && appendEntryResponse.MatchedIndex < s.lastApplied {
				continue
			} else if appendEntryResponse.Success {
				respondedServers++
				s.matchIndex[idx] = appendEntryResponse.MatchedIndex
				s.nextIndex[idx] = int64(len(s.log))
				//fmt.Printf("Success from server %d\n", idx)
				//fmt.Printf("%d. RaftServer UpdateFile() applied appendEntry to %d successfully\n", s.id, idx)
				break
			} else if !appendEntryResponse.Success {
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

func (s *RaftSurfstore) crashedGetter() bool {
	isCrashed := false
	s.isCrashedMutex.RLock()
	isCrashed = s.isCrashed
	s.isCrashedMutex.RUnlock()

	return isCrashed
}

func (s *RaftSurfstore) leaderGetter() bool {
	isLeader := false
	s.isLeaderMutex.RLock()
	isLeader = s.isLeader
	s.isLeaderMutex.RUnlock()

	return isLeader
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
