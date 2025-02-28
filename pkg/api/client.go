package api

import (
	"log"

	"github.com/Miracle-6785/mfs/generate/proto"
	"google.golang.org/grpc"
)

var NameNodeClient proto.NameNodeServiceClient

func InitGRPCClients() {
	// Connect to NameNode gRPC server
	nameNodeConn, err := grpc.Dial(":50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal("Failed to connect to NameNode gRPC server: %v", err)
	}
	NameNodeClient = proto.NewNameNodeServiceClient(nameNodeConn)
}

func InitDataNodeClient(addr string) proto.DataNodeServiceClient {
	dataNodeConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to DataNode gRPC server at %s: %v", addr, err)
	}

	client := proto.NewDataNodeServiceClient(dataNodeConn)
	log.Printf("Connected to DataNode at %s\n", addr)
	return client
}
