package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/Miracle-6785/mfs/generate/proto"
	"github.com/Miracle-6785/mfs/internal/config"
	"github.com/Miracle-6785/mfs/internal/datanode"
	"github.com/Miracle-6785/mfs/internal/namenode"
	"google.golang.org/grpc"
)

func main() {
	// Start NameNode gRPC server
	go func() {
		if err := startNameNode(":50051"); err != nil {
			log.Fatalf("failed to start NameNode: %v", err)
		}
	}()

	// Give it a moment to start
	time.Sleep(1000 * time.Millisecond)

	go func() {
		if err := startDataNode(":5051", "localhost:50051"); err != nil {
			log.Fatalf("failed to start DataNode: %v", err)
		}
	}()

	select {}
}

// startNameNode starts the gRPC server for the NameNode on the specified address/port
func startNameNode(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	server := grpc.NewServer()

	// Create a NameNode instance
	nn := namenode.NewNameNode(config.BLOCK_SIZE, 1)

	// Register the NameNodeService
	proto.RegisterNameNodeServiceServer(server, nn)

	// Register the DataNodeRegistryService
	proto.RegisterDataNodeRegistryServiceServer(server, nn)

	log.Printf("starting NameNode on %s", addr)
	return server.Serve(lis)
}

func startDataNode(addr, nameNodeAddr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	server := grpc.NewServer()
	dn, err := datanode.NewDataNode("dn1", "/tmp/dn1", nameNodeAddr, "5051")
	if err != nil {
		return err
	}

	proto.RegisterDataNodeServiceServer(server, dn)

	// ✅ Attempt to register with NameNode
	go func() {
		time.Sleep(2 * time.Second)
		if err := dn.RegisterWithNameNode(); err != nil {
			log.Fatalf("failed to register DataNode with NameNode: %v", err)
		} else {
			log.Println("DataNode successfully registered with NameNode")
			// dn.StartHeartbeat(5 * time.Second)
		}
	}()

	log.Printf("Starting DataNode on %s", addr)
	return server.Serve(lis)
}
