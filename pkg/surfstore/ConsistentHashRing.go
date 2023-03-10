package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//	fmt.Printf("blockId: %s\n", blockId)
	//	blockHash := c.Hash(blockId)
	responsibleServerHash := ""
	serverHashes := make([]string, 0)

	//	fmt.Printf("blockHash: %s\n", blockId)
	//	fmt.Printf("c.ServerMap: %v\n", c.ServerMap)

	for k, _ := range c.ServerMap {
		serverHashes = append(serverHashes, k)
	}
	//	fmt.Printf("serverHashes: %s\n", serverHashes)

	sort.Strings(serverHashes)

	for _, serverHash := range serverHashes {
		if serverHash > blockId {
			responsibleServerHash = serverHash
			//			fmt.Printf("responsable server: %s\n", responsibleServerHash)
			break
		}
	}
	if responsibleServerHash == "" && len(serverHashes) > 0 {
		responsibleServerHash = serverHashes[0]
	}
	//	fmt.Printf("responsibleServerHash == %s\n", responsibleServerHash)
	//	fmt.Printf("\n")

	/*minServerHash := ""
	minServerAddr := ""
	serverHashDirectAfterBlock := ""
	serverAddrDirectAfterBlock := ""

	for serverHash, addr := range c.ServerMap {
		if serverHash > blockHash {
			if serverHashDirectAfterBlock == "" || serverHash < serverHashDirectAfterBlock {
				// found new server hash directly after the block hash
				serverHashDirectAfterBlock = serverHash
				serverAddrDirectAfterBlock = addr
			}
		}

		if minServerHash == "" || serverHash < minServerHash {
			minServerHash = serverHash
			minServerAddr = addr
		}
	}

	if serverAddrDirectAfterBlock == "" {
		responsibleServerAddr = minServerAddr
	} else {
		responsibleServerAddr = serverAddrDirectAfterBlock
	}*/
	return c.ServerMap[responsibleServerHash]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	var consistentHashRing = ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, serverAddr := range serverAddrs {
		serverHash := consistentHashRing.Hash("blockstore" + serverAddr)
		consistentHashRing.ServerMap[serverHash] = serverAddr
	}
	return &consistentHashRing
}
