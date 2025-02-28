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
