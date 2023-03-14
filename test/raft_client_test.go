package SurfTest

import (
	"fmt"
	"os"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

func TestSyncTwoClientsOneUpdates(t *testing.T) {
	t.Logf("client1 syncs with a file, modifies, syncs again. client2 syncs.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	fmt.Printf("Set leader to server 0\n")
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Printf("End heartbeat\n")

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	fmt.Printf("Start Sync\n")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	fmt.Printf("End Sync\n")

	fmt.Printf("Start heartbeat1\n")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Printf("End heartbeat1\n")

	err = worker1.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}
	fmt.Printf("Start Sync2\n")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed: %s\n", err.Error())
	}
	fmt.Printf("End Sync2\n")

	fmt.Printf("Start heartbeat\n")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Printf("End heartbeat\n")

	//client1 syncs
	fmt.Printf("Start Sync3\n")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	fmt.Printf("End Sync3\n")

	//fmt.Printf("Start heartbeat\n")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Printf("End heartbeat\n")

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1 == nil || fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2 == nil || fileMeta2[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", workingDir+"/test0/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	fmt.Printf("Set leader to server 0\n")
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Printf("End heartbeat\n")

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	//fmt.Printf("Start Sync\n")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	//fmt.Printf("End Sync\n")

	//fmt.Printf("Start heartbeat\n")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Printf("End heartbeat\n")
	//fmt.Printf("Crash server 0\n")
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})

	//fmt.Printf("Start heartbeat\n")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Printf("End heartbeat\n")

	//client2 syncs
	//fmt.Printf("Start Sync\n")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed: %s\n", err.Error())
	}
	//fmt.Printf("End Sync\n")

	//fmt.Printf("Start heartbeat\n")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Printf("End heartbeat\n")

	//client1 syncs
	//fmt.Printf("Start Sync\n")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	//fmt.Printf("End Sync\n")

	//fmt.Printf("Start heartbeat\n")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Printf("End heartbeat\n")
	//fmt.Printf("Start heartbeat\n")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//fmt.Printf("End heartbeat\n")

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}
