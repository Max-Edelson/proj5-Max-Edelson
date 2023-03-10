package surfstore

import (
	context "context"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	baseDirPath := "./"
	if client.BaseDir != "." {
		if client.BaseDir[len(client.BaseDir)-1:] != "/" {
			baseDirPath = client.BaseDir + "/"
		} else {
			baseDirPath = client.BaseDir
		}
	}
	//	fmt.Printf("baseDirPath: %s, client.BaseDir: %s\n", baseDirPath, client.BaseDir)

	/* Start updating local index.db file with local changes */
	localFileMetaMap := make(map[string]*FileMetaData)
	err := filepath.Walk(client.BaseDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() {
			filename := filepath.Base(path)
			//			fmt.Printf("visited file: %q\n", filename)
			if strings.Contains(filename, DEFAULT_META_FILENAME) ||
				strings.Contains(filename, CONFIG_DELIMITER) ||
				strings.Contains(filename, "/") {
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			var metadata = FileMetaData{Filename: filename, BlockHashList: make([]string, 0)}

			fileInfo, _ := file.Stat()
			var fileSize int64 = fileInfo.Size()
			numChunks := uint64(math.Ceil(float64(fileSize) / float64(client.BlockSize)))

			for i := uint64(0); i < numChunks; i++ {
				chunkSize := client.BlockSize
				if i == numChunks-1 { // last chunk may not be full blocksize
					chunkSize = int(fileSize) % client.BlockSize
				}

				buf := make([]byte, chunkSize)
				bytesRead, err := file.Read(buf)
				if err != nil {
					fmt.Printf("Error reading %s\n", path)
					continue
				}

				chunk := buf[:bytesRead]
				hash := GetBlockHashString(chunk)
				//				fmt.Printf("Hash: %s\n", hash)
				metadata.BlockHashList = append(metadata.BlockHashList, hash)
			}
			localFileMetaMap[filename] = &metadata
			//			metadata.Version = 1
			//			fmt.Printf("list: %s\n", localFileMetaMap[filename].BlockHashList)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
	//fmt.Printf("Done updating local index.db file with local changes\n")

	indexFileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	checkError(err)

	// Check if any files were deleted
	for indexFilename, indexMetadata := range indexFileMetaMap {
		_, filenameExistsInLocal := localFileMetaMap[indexFilename]
		if !filenameExistsInLocal && !wasDeleted(*indexMetadata) { // File was deleted
			indexMetadata.BlockHashList = []string{TOMBSTONE_HASHVALUE}
			localFileMetaMap[indexFilename] = indexMetadata
			//			fmt.Printf("%s was deleted from local system.\n", indexFilename)
		} else if filenameExistsInLocal {
			localFileMetaMap[indexFilename].Version = indexMetadata.Version
		}
	}
	/* Done updating local index.db file with local changes */
	//fmt.Printf("Done searching for deleted files on local\n")

	// perform the call
	var empty emptypb.Empty
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Download remote fileInfoMap
	// connect to the server
	//	raftStoreConn, err := grpc.Dial(client.MetaStoreAddr, grpc.WithInsecure())
	//	checkError(err)
	//	defer metaStoreConn.Close()
	//	metaStoreC := NewMetaStoreClient(metaStoreConn)
	// Open raftSuftStore connection
	//	blockStoreAddrs, err := metaStoreC.GetBlockStoreAddrs(ctx, &empty)
	//	checkError(err)

	/*raftSurfConn, err := grpc.Dial(client.MetaStoreAddrs[2], grpc.WithInsecure())
	checkError(err)
	defer raftSurfConn.Close()
	raftServerClient := NewRaftSurfstoreClient(raftSurfConn)
	fmt.Printf("Done connecting to raft surfstore client\n")*/

	remoteFileMetaMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteFileMetaMap)
	checkError(err)
	/*fmt.Printf("Done retrieving getFileInfoMap\n")
	remoteFileMetaMap := *remoteFileInfoMap.FileInfoMap*/

	// Open blockStoreClient connection
	blockStoreAddrs := make([]string, 0)
	err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	checkError(err)
	var blockStoreMap = make(map[string]BlockStoreClient) // takes in addr
	for _, addr := range blockStoreAddrs {
		if strings.Contains(addr, "blockstore") {
			addr = strings.Replace(addr, "blockstore", "", -1)
		}

		blockStoreConn, err := grpc.Dial(addr, grpc.WithInsecure())
		checkError(err)
		defer blockStoreConn.Close()
		blockStoreC := NewBlockStoreClient(blockStoreConn)
		blockStoreMap[addr] = blockStoreC
	}
	//fmt.Printf("Done getting BlockStoreClients\n")

	// fmt.Printf("\n")
	for localFilename, localMetadata := range localFileMetaMap {
		//		fmt.Printf("\n")
		remoteMetaData, filenameExistsInRemote := remoteFileMetaMap[localFilename]
		var localHashes = make([]string, 0)
		var localBlocks = make([]Block, 0)
		if !wasDeleted(*localMetadata) {
			getLocalHashesAndBlocks(baseDirPath+localFilename, client.BlockSize, &localHashes, &localBlocks)
		} else {
			localHashes = append(localHashes, "0")
		}
		localModification := !reflect.DeepEqual(localHashes, localMetadata.BlockHashList)
		if (filenameExistsInRemote && wasDeleted(*remoteMetaData)) || wasDeleted(*localMetadata) {
			localModification = true
		}
		if localModification {
			localMetadata.BlockHashList = localHashes
		}

		differFromRemote := true
		if filenameExistsInRemote {
			differFromRemote = !reflect.DeepEqual(localHashes, remoteMetaData.BlockHashList)
			//			fmt.Printf("localHashes: %s, remoteMetaData.BlockHashList: %s\n", localHashes, remoteMetaData.BlockHashList)
		}
		//		fmt.Printf("localModification: %t. differFromRemote: %t\n", localModification, differFromRemote)

		// Local file not in remote. Upload it
		if !filenameExistsInRemote {
			//fmt.Printf("%s does not exist in remote\n", localFilename)
			localFileMetaMap[localFilename].Version = 1

			// Upload blocks
			if !uploadBlocks(localHashes, localBlocks, &blockStoreMap, client, ctx) {
				// handle error
				log.Fatal("Had an error\n")
			} else { // Try to upload metadata
				//fmt.Printf("Update metaStoreC with localMetadata (%d): %v\n", len(localMetadata.BlockHashList), localMetadata)
				//sort.Strings(localMetadata.BlockHashList)
				//				fmt.Println("Sorted hashList: ", localMetadata.BlockHashList)
				returnVersion := localMetadata.Version
				err = client.UpdateFile(localMetadata, &returnVersion)
				checkError(err)
				//PrintNumOnEachServer(&blockStoreMap, ctx, empty)
				if returnVersion == -1 { // Someone uploaded newer version of this file. Handle conflict.
					/*updatedRemoteMeta := */ handleNewerVersionOnServer(baseDirPath+localFilename, client.BlockSize, &blockStoreMap, client, ctx, *remoteMetaData, indexFileMetaMap, localHashes, empty)
					//indexFileMetaMap[localFilename] = &updatedRemoteMeta
				}
			}
			//			fmt.Printf("%s version num: %d\n", localFilename, localMetadata.Version)
			//uploadFile(client.BaseDir+localFilename, client.BlockSize, blockStoreC, metaStoreC, ctx, localMetadata.Version, *remoteMetaData, indexFileMetaMap, empty)
		} else if remoteMetaData.Version > localMetadata.Version && wasDeleted(*remoteMetaData) { // File was deleted from the remote system, but still present on local
			//fmt.Printf("%s was deleted in remote, but not on local\n", localFilename)
			localFileMetaMap[localFilename] = remoteMetaData
			err = os.Remove(baseDirPath + localFilename)
			checkError(err)
		} else if remoteMetaData.Version > localMetadata.Version {
			/* The remote file is a higher version than the local version, bring the local version up to date with the remote
			by downloading any necessary blocks. */
			//fmt.Printf("%s has higher version number in remote than in local\n", localFilename)
			bringLocalFileUpToDateWithRemote(baseDirPath+localFilename, client.BlockSize, &blockStoreMap, client, ctx, *remoteMetaData, localHashes)
			err = client.GetFileInfoMap(&remoteFileMetaMap)
			checkError(err)
			localFileMetaMap[localFilename] = remoteFileMetaMap[localFilename]
		} else if remoteMetaData.Version <= localMetadata.Version && (localModification || differFromRemote) { // Remote version should never be less than local
			/* The local version has local modifications while both the remote and the local are the same version. Upload the
			differing blocks onto the remote server. */
			//fmt.Printf("%s has modifications on local and has same version in remote\n", localFilename)
			success := true
			if !wasDeleted(*localMetadata) {
				remoteMissingHashes := getMissingHashesFromLocalAndRemote(localHashes, remoteMetaData, client, ctx) // hashes missing from remote
				var blocksToUpload = make([]Block, 0)
				getBlocksFromHashes(remoteMissingHashes, localBlocks, &blocksToUpload) // get blocks corresponding to hashes
				success = uploadBlocks(localHashes, blocksToUpload, &blockStoreMap, client, ctx)
			}
			if !success {
				log.Fatal("Errored uploading blocks")
			}
			//			fmt.Printf("%s localMetadata.Version: %d\n", localFilename, localMetadata.Version)
			localMetadata.Version += 1
			returnVersion := localMetadata.Version
			err = client.UpdateFile(localMetadata, &returnVersion)
			//fmt.Printf("%s version num: %d\n", localFilename, localMetadata.Version)
			checkError(err)
			if returnVersion == -1 { // Someone uploaded newer version of this file. Handle conflict.
				/*updatedRemoteMeta := */ handleNewerVersionOnServer(baseDirPath+localFilename, client.BlockSize, &blockStoreMap, client, ctx, *remoteMetaData, localFileMetaMap, localHashes, empty)
				//indexFileMetaMap[localFilename] = &updatedRemoteMeta
			}
		} /* else {
			fmt.Printf("%s has no detected changes on local or remote.\n", localFilename)
		}*/

		//fmt.Printf("%v\n", localMetadata)
	}
	//	fmt.Printf("\nDone running through local and remote comparison\n")

	// check for remote files not present on local system
	for remoteFilename, remoteMetadata := range remoteFileMetaMap {
		_, filenameExistsInLocal := localFileMetaMap[remoteFilename]
		//localHashes, localBlocks := getLocalHashesAndBlocks(client.BaseDir+localFilename, client.BlockSize)
		//localModification := !reflect.DeepEqual(localHashes, localMetadata.BlockHashList)

		if !filenameExistsInLocal { // Download remote file to local
			if !wasDeleted(*remoteMetadata) {
				//				fmt.Printf("%s doesn't exist on local\n", remoteFilename)
				bringLocalFileUpToDateWithRemote(baseDirPath+remoteFilename, client.BlockSize, &blockStoreMap, client, ctx, *remoteMetadata, []string{})
			}
			localFileMetaMap[remoteFilename] = remoteMetadata
		}
	}
	//	fmt.Printf("Done running through remote and local comparison\n")

	err = WriteMetaFile(localFileMetaMap, baseDirPath)
	checkError(err)
}

func PrintNumOnEachServer(blockStoreMap *map[string]BlockStoreClient, ctx context.Context, empty emptypb.Empty) {
	for k, v := range *blockStoreMap {
		blockHashes, err := v.GetBlockHashes(ctx, &empty)
		checkError(err)
		fmt.Printf("%s: %d hashes\n", k, len(blockHashes.Hashes))
	}
}

func wasDeleted(localMetadata FileMetaData) bool {
	deleted := false
	if len(localMetadata.BlockHashList) == 1 && localMetadata.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		deleted = true
		//		fmt.Printf("%s was deleted\n", localMetadata.Filename)
	}
	return deleted
}

func handleNewerVersionOnServer(path string, blockSize int, blockStoreMap *map[string]BlockStoreClient, client RPCClient, ctx context.Context, remoteMetaData FileMetaData, indexFileMetaMap map[string]*FileMetaData, localHashes []string, empty emptypb.Empty) {
	bringLocalFileUpToDateWithRemote(path, blockSize, blockStoreMap, client, ctx, remoteMetaData, localHashes)
	remoteFileMetaMap := make(map[string]*FileMetaData)
	err := client.GetFileInfoMap(&remoteFileMetaMap)
	indexFileMetaMap[filepath.Base(path)] = remoteFileMetaMap[filepath.Base(path)]
	checkError(err)
}

func getBlocksFromHashes(targetHashes []string, localBlocks []Block, blockToUpload *[]Block) {
	for _, block := range localBlocks {
		blockhash := GetBlockHashString(block.BlockData)
		if hashInHashList(targetHashes, blockhash) {
			*blockToUpload = append(*blockToUpload, block)
		}
	}
}

// For use when uploading the different blocks to the server
func getMissingHashesFromLocalAndRemote(localHashes []string, remoteMetaData *FileMetaData, client RPCClient, ctx context.Context) []string {
	var missingHashes []string = make([]string, 0)
	for _, hash := range localHashes {
		if !hashInHashList(remoteMetaData.BlockHashList, hash) {
			missingHashes = append(missingHashes, hash)
		}
	}
	return missingHashes
}

func uploadBlocks(hashes []string, blocks []Block, blockStoreMap *map[string]BlockStoreClient, client RPCClient, ctx context.Context) bool {
	// returns map of addr => hashes
	responsibleServers := make(map[string][]string)
	err := client.GetBlockStoreMap(hashes, &responsibleServers)
	checkError(err)

	flag := true
	for addr, blockHashes := range responsibleServers {
		//	fmt.Printf("uploadBlocks[%d]: %s\n", idx, GetBlockHashString(block.BlockData))

		blockStoreC := (*blockStoreMap)[addr]
		for _, blockHash := range blockHashes {
			blockIdx := getHashIndex(hashes, blockHash)
			if blockIdx == -1 {
				log.Fatal("Invalid block index in uploadBlocks()")
			}

			succ, err := blockStoreC.PutBlock(ctx, &blocks[blockIdx])
			checkError(err)
			if !succ.Flag {
				flag = false
				break
			}
		}
		if !flag {
			break
		}
	}

	return flag
}

func getHashIndex(hashes []string, target string) int {
	idx := -1

	for i, hash := range hashes {
		if hash == target {
			idx = i
			break
		}
	}
	return idx
}

func returnServerAddrForHash(responsibleServers map[string][]string, targetHash string) string {
	addr := ""

	for k, v := range responsibleServers {
		hashesInServer := v

		if hashInHashList(hashesInServer, targetHash) {
			addr = k
			break
		}
	}
	return addr
}

func bringLocalFileUpToDateWithRemote(path string, blockSize int, blockStoreMap *map[string]BlockStoreClient, client RPCClient, ctx context.Context, remoteMetaData FileMetaData, localHashes []string) {
	var reconstitutedFile []byte = make([]byte, 0)
	responsibleServers := make(map[string][]string)
	err := client.GetBlockStoreMap(remoteMetaData.BlockHashList, &responsibleServers)
	checkError(err)

	for idx, remoteHash := range remoteMetaData.BlockHashList {
		var chunk []byte
		succ := true
		if !hashInHashList(localHashes, remoteHash) {
			blockStoreAddr := returnServerAddrForHash(responsibleServers, remoteHash)
			if blockStoreAddr == "" {
				log.Fatal("Invalid addr in bringLocalFileUpToDateWithRemote\n")
			}

			blockStoreC := (*blockStoreMap)[blockStoreAddr]
			block, err := blockStoreC.GetBlock(ctx, &BlockHash{Hash: remoteHash})
			checkError(err)
			chunk = block.BlockData
		} else {
			//			fmt.Printf("hashInHashList(localHashes (%d), remoteHash): %t\n", len(localHashes), hashInHashList(localHashes, remoteHash))
			chunk, succ = readChunk(path, blockSize, idx)
		}
		if !succ {
			log.Fatal("Failed to read chunk\n")
		}
		reconstitutedFile = append(reconstitutedFile, chunk...)
	}

	// Write file back to local
	err = os.WriteFile(path, reconstitutedFile, 0644)
	checkError(err)
}

// return true for match, false for differ
func getLocalHashesAndBlocks(path string, blockSize int, localHashes *[]string, localBlocks *[]Block) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	numChunks := uint64(math.Ceil(float64(fileSize) / float64(blockSize)))

	buf := make([]byte, blockSize)

	for i := uint64(0); i < numChunks; i++ {
		chunkSize := blockSize
		if i == numChunks-1 { // last chunk may not be full blocksize
			chunkSize = int(fileSize) % blockSize
		}

		if len(buf) != chunkSize {
			buf = make([]byte, chunkSize)
		}

		bytesRead, err := file.Read(buf)
		checkError(err)

		chunk := buf[:bytesRead]
		hash := GetBlockHashString(chunk)
		//	fmt.Printf("getLocalHash hash: %s\n", hash)
		*localHashes = append(*localHashes, hash)

		chunkData := make([]byte, chunkSize)
		copy(chunkData, chunk)

		var block = Block{BlockData: chunkData, BlockSize: int32(chunkSize)}

		*localBlocks = append(*localBlocks, block)
		//		fmt.Printf("getLocalHash block len: %d\n", len(block.BlockData))
	}
}

func readChunk(path string, blockSize int, chunk int) ([]byte, bool) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	numChunks := uint64(math.Ceil(float64(fileSize) / float64(blockSize)))
	buf := make([]byte, blockSize)

	for i := 0; i < int(numChunks); i++ {
		chunkSize := blockSize
		if uint64(i) == numChunks-1 { // last chunk may not be full blocksize
			chunkSize = int(fileSize) % blockSize
		}

		if len(buf) != chunkSize {
			buf = make([]byte, chunkSize)
		}

		bytesRead, err := file.Read(buf)
		checkError(err)
		if i == chunk {
			return buf[:bytesRead], true
		}
	}
	return []byte(""), false
}

func hashInHashList(hashList []string, targetHash string) bool {
	found := false
	for _, hash := range hashList {
		if hash == targetHash {
			found = true
			break
		}
	}
	return found
}

func checkError(err error) {
	if err != nil {
		log.Fatal("Error: %s\n", err.Error())
	}
}
