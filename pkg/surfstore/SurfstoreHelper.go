package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

func writeToBaseDir(path string, content []byte) error {
	return os.WriteFile(path, content, 0644)
}

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	statement, err = db.Prepare(insertTuple)
	for fileName, metadata := range fileMetas {
		//		fmt.Printf("key[%s] value[%s]\n", k, v)
		idx := 0
		for _, hash := range metadata.BlockHashList {
			statement.Exec(fileName, metadata.Version, idx, hash)
			idx += 1
		}
	}

	return err
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = ``

const getUniqueFilenames = "select fileName, version from indexes order by fileName;"
const getHashListForFile = "select hashValue from indexes where fileName = ? AND version = ? order by hashIndex;"

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}

	// create the table if need be
	statement, err := db.Prepare(createTable)
	if err != nil {
		fmt.Println(err.Error())
	}
	statement.Exec()

	// retrieve all cse courses tuples in ascending order by code.
	rows, err := db.Query(getUniqueFilenames)
	if err != nil {
		fmt.Println(err.Error())
	}
	var uniqueFilename string
	var uniqueVersion int
	for rows.Next() {
		rows.Scan(&uniqueFilename, &uniqueVersion)
		fullFileRows, err := db.Query(getHashListForFile, uniqueFilename, uniqueVersion)
		if err != nil {
			fmt.Println(err.Error())
		}

		var fileMetaData FileMetaData
		fileMetaData.BlockHashList = make([]string, 0)
		fileMetaData.Filename = uniqueFilename
		fileMetaData.Version = int32(uniqueVersion)

		var hash string
		for fullFileRows.Next() {
			fullFileRows.Scan(&hash)
			fileMetaData.BlockHashList = append(fileMetaData.BlockHashList, hash)
			//			fmt.Println(department + strconv.Itoa(code) + ": " + description)
		}
		fileMetaMap[uniqueFilename] = &fileMetaData
	}
	return fileMetaMap, err
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
