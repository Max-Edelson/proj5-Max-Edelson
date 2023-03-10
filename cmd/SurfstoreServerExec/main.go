package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}
	if len(args) >= 1 {
		blockStoreAddrs = args
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func startBlockStoreServer(block *surfstore.BlockStore, hostAddr string) error {
	//	fmt.Printf("Making blockstore server on %s\n", hostAddr)
	//	fmt.Printf("1\n")
	listener, err := net.Listen("tcp", hostAddr)
	//	fmt.Printf("2\n")
	if err != nil {
		//		fmt.Printf("3\n")
		log.Printf("Error listening: %s\n", err.Error())
		return err
	}
	//	fmt.Printf("4\n")
	defer listener.Close()

	server := grpc.NewServer()
	surfstore.RegisterBlockStoreServer(server, block)
	if err := server.Serve(listener); err != nil {
		log.Printf("failed to serve: %v", err)
	}
	return err
}

func startServer_deprecated(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	//	fmt.Printf("serviceType: %s\n", serviceType)

	/*if serviceType == "meta" || serviceType == "both" {
		consistentHashRing := surfstore.NewConsistentHashRing(blockStoreAddrs)

		serverB := surfstore.NewMetaStore(blockStoreAddrs, consistentHashRing)
		//		fmt.Printf("serverB blockStoreAddrs: %v\n", serverB.BlockStoreAddrs)
		if serviceType == "meta" {
			return startMetaStoreServer(serverB, hostAddr)
		} else { // serviceType == "both"
			go startMetaStoreServer(serverB, hostAddr)
		}
	}*/

	if serviceType == "block" || serviceType == "both" {
		return startBlockStoreServer(surfstore.NewBlockStore(), hostAddr)
	}

	/*for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error connecting: ", err.Error())
			return err
		}
		//		fmt.Println("Client " + conn.RemoteAddr().String() + " connected, spawning goroutine to handle connection")

		// Spawn a new goroutine to handle this client's connections
		// and go back to listening for additional connections
		go handleClientConnection(conn, s.VirtualHosts)
	}*/
	return nil
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	//	fmt.Printf("serviceType: %s\n", serviceType)

	listener, err := net.Listen("tcp", hostAddr)
	if err != nil {
		log.Printf("Error listening: %s\n", err.Error())
		return err
	}
	defer listener.Close()
	server := grpc.NewServer()

	if serviceType == "meta" {
		consistentHashRing := surfstore.NewConsistentHashRing(blockStoreAddrs)

		//		fmt.Printf("blockStoreAddrs: %s\n", blockStoreAddrs)
		metaServer := surfstore.NewMetaStore(blockStoreAddrs, consistentHashRing)
		surfstore.RegisterMetaStoreServer(server, metaServer)
		if err := server.Serve(listener); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	} else if serviceType == "block" {
		surfstore.RegisterBlockStoreServer(server, surfstore.NewBlockStore())
		if err := server.Serve(listener); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}

	return err
}
