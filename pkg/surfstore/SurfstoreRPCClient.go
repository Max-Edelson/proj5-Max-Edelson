package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, raftServerAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(raftServerAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			conn.Close()
			if err == ERR_NOT_LEADER || err == ERR_SERVER_CRASHED || strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed") {
				continue
			} else {
				return ERR_SERVER_CRASHED
			}
		} else {
			temp := make(map[string][]string)
			for k, v := range blockMap.BlockStoreMap {
				temp[k] = v.Hashes
			}
			*blockStoreMap = temp
			// close the connection
			return conn.Close()
		}
	}
	return ERR_SERVER_CRASHED // all servers crashed
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, raftServerAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(raftServerAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var empty emptypb.Empty
		addrs, err := c.GetBlockStoreAddrs(ctx, &empty)
		if err != nil {
			conn.Close()
			if err == ERR_NOT_LEADER || err == ERR_SERVER_CRASHED || strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed") {
				continue
			} else {
				return ERR_SERVER_CRASHED
			}
		} else {
			*blockStoreAddrs = addrs.BlockStoreAddrs
			return conn.Close()
		}
	}
	return ERR_SERVER_CRASHED // all servers are crashed
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	addr := blockStoreAddr
	if strings.Contains(blockStoreAddr, "blockstore") {
		addr = strings.Replace(blockStoreAddr, "blockstore", "", -1)
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var empty emptypb.Empty
	hashes, err := c.GetBlockHashes(ctx, &empty)
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = hashes.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	addr := blockStoreAddr
	if strings.Contains(blockStoreAddr, "blockstore") {
		addr = strings.Replace(blockStoreAddr, "blockstore", "", -1)
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	addr := blockStoreAddr
	if strings.Contains(blockStoreAddr, "blockstore") {
		addr = strings.Replace(blockStoreAddr, "blockstore", "", -1)
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	success, err := c.PutBlock(ctx, block)
	*succ = success.Flag
	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	addr := blockStoreAddr
	if strings.Contains(blockStoreAddr, "blockstore") {
		addr = strings.Replace(blockStoreAddr, "blockstore", "", -1)
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var blockHashes BlockHashes
	blockHashes.Hashes = blockHashesIn
	returnedBlockHashes, err := c.HasBlocks(ctx, &blockHashes)
	blockHashesOut = &returnedBlockHashes.Hashes

	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	var empty emptypb.Empty
	//fmt.Printf("RPC FileInfoMap\n")
	for _, raftServerAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		//fmt.Printf("Connect to %s\n", raftServerAddr)
		conn, err := grpc.Dial(raftServerAddr, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("Err: %s\n", err.Error())
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &empty)

		if err != nil {
			conn.Close()
			if err == ERR_NOT_LEADER || err == ERR_SERVER_CRASHED || strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed") {
				//fmt.Printf("Continue\n")
				continue
			} else {
				//fmt.Printf("Returning. Err: %s\n", err.Error())
				return ERR_SERVER_CRASHED
			}
		} else {
			//fmt.Printf("Found not crashed leader\n")
			*serverFileInfoMap = fileInfoMap.FileInfoMap
			return conn.Close()
		}
	}
	//fmt.Printf("All servers crashed\n")
	return ERR_SERVER_CRASHED // all servers crashed
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, raftServerAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(raftServerAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		version, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			conn.Close()
			if err == ERR_NOT_LEADER || err == ERR_SERVER_CRASHED || strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed") {
				continue
			} else {
				return ERR_SERVER_CRASHED
			}
		} else {
			latestVersion = &version.Version
			return conn.Close()
		}
	}
	return ERR_SERVER_CRASHED // all servers crashed
}

/*func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var empty emptypb.Empty
	blockAddr, err := c.GetBlockStoreAddr(ctx, &empty)
	blockStoreAddr = &blockAddr.Addr

	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}*/

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
