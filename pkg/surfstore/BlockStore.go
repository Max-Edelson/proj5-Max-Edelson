package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	blockVal, errorHash := bs.BlockMap[blockHash.Hash]

	// Hash not in map
	if !errorHash {
		var block = Block{BlockData: []byte("")}
		return &block, ctx.Err()
	}

	return blockVal, ctx.Err()
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashList := make([]string, 0)
	for blockHash, _ := range bs.BlockMap {
		blockHashList = append(blockHashList, blockHash)
	}
	return &BlockHashes{Hashes: blockHashList}, ctx.Err()
}

// How would this function fail?
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	if bs.BlockMap == nil {
		bs.BlockMap = make(map[string]*Block)
	}

	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block

	//fmt.Printf("computed hash: %s = len: %d\n", hash, block.BlockSize)

	var success Success
	success.Flag = true

	if ctx.Err() != nil {
		success.Flag = false
	}

	return &success, ctx.Err()
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	stored := make([]string, 0)

	for _, hash := range blockHashesIn.Hashes {
		_, hashFound := bs.BlockMap[hash]
		if hashFound {
			stored = append(stored, hash)
		}
	}

	var storedBlockHashes = BlockHashes{Hashes: stored}
	return &storedBlockHashes, ctx.Err()
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
