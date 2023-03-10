package surfstore

import (
	context "context"
	"strings"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	if m.FileMetaMap == nil {
		m.FileMetaMap = make(map[string]*FileMetaData)
	}

	var fileInfoMap = FileInfoMap{FileInfoMap: m.FileMetaMap}
	return &fileInfoMap, ctx.Err()
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	if m.FileMetaMap == nil {
		m.FileMetaMap = make(map[string]*FileMetaData)
	}

	var version = Version{Version: -1}

	// check for hash existance
	remoteFileInfo, remoteFileExist := m.FileMetaMap[fileMetaData.Filename]

	// wrong version number as parameter
	if !remoteFileExist || (remoteFileExist && fileMetaData.Version == remoteFileInfo.Version+1) {
		// update file or create file. Double check that we should create the file
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		version.Version = fileMetaData.Version
	}
	return &version, ctx.Err()
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	var blockStoreMap = BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}
	for _, blockHash := range blockHashesIn.Hashes {
		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(blockHash)
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
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, ctx.Err()
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string, consistentHashRing *ConsistentHashRing) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: consistentHashRing,
	}
}
