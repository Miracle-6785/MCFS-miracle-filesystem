package datanode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Miracle-6785/mfs/generate/proto"
	"github.com/Miracle-6785/mfs/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StorageInfo struct {
	capacity   int64
	used       int64
	storageDir string
	blockCount int
}

// Type DataNode represents a data node in the cluster
type DataNode struct {
	proto.UnimplementedDataNodeServiceServer

	id           string
	nameNodeAddr string
	Port         string
	StorageInfo
}

func NewDataNode(id, storageDir, nameNodeAddr, port string) (*DataNode, error) {
	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, err
	}

	// Calculate capacity and used space
	// This would be a real implementation to get disk stats

	storageInfo := &StorageInfo{
		capacity:   10 * 1024 * 1024 * 1024, // 10GB
		used:       1000,
		storageDir: storageDir,
		blockCount: 0,
	}

	return &DataNode{
		id:           id,
		nameNodeAddr: nameNodeAddr,
		StorageInfo:  *storageInfo,
		Port:         port,
	}, nil
}

func (dn *DataNode) RegisterWithNameNode() error {
	// Get the local IP address of the DataNode
	localIP, err := common.GetLocalIP()
	if err != nil {
		return fmt.Errorf("failed to get local IP: %v", err)
	}

	// Connect to NameNode
	conn, err := grpc.Dial(dn.nameNodeAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create a client for the DataNodeRegistrationService
	client := proto.NewDataNodeRegistryServiceClient(conn)

	// Prepare the request
	req := &proto.RegisterDataNodeRequest{
		DatanodeId: dn.id,
		Address: &proto.Address{
			Ip:   localIP,
			Port: dn.Port,
		},
		StorageInfo: &proto.StorageInfo{
			Capacity: dn.capacity,
			Used:     dn.used,
		},
	}

	// Make the RPC call
	resp, err := client.RegisterDataNode(context.Background(), req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("Registration failed: %s", resp.Message)
	}

	// Cloud log or print the message
	fmt.Println("DataNode registered with NameNode")
	return nil
}

// StartHeartbeat starts sending periodic heartbeats to the NameNode
func (dn *DataNode) StartHeartbeat(interval time.Duration) {
	// Connect to NameNode
	conn, _ := grpc.Dial(dn.nameNodeAddr, grpc.WithInsecure())
	defer conn.Close()

	// Create a client for the DataNodeRegistrationService
	client := proto.NewDataNodeRegistryServiceClient(conn)

	// Send periodic heartbeats
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		<-ticker.C

		// Prepare the request
		req := &proto.HeartbeatRequest{
			DatanodeId: dn.id,
			StorageInfo: &proto.StorageInfo{
				Capacity: dn.capacity,
				Used:     dn.used,
			},
		}

		// Send the heartbeat RPC
		resp, err := client.Heartbeat(context.Background(), req)
		if err != nil {
			fmt.Printf("❌ Heartbeat failed: %v\n", err)
		} else if resp.Success {
			fmt.Println("✅ Heartbeat sent successfully")
		} else {
			fmt.Println("⚠️ Heartbeat response received but not successful")
		}
	}
}

func (dn *DataNode) WriteBlock(stream proto.DataNodeService_WriteBlockServer) error {
	var (
		file              *os.File
		blockID           string
		totalBytesWritten int64
		opened            bool
	)

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	// 1. Loop over incoming messages
	for {
		req, err := stream.Recv()
		if err != nil {
			// Client closed the stream, or an error occurred
			break
		}

		if err != nil {
			return status.Errorf(codes.Unknown, "Failed to receive a message: %v", err)
		}

		switch payload := req.Payload.(type) {
		case *proto.WriteBlockRequest_Metadata:
			if opened {
				// If we already have a file open, something went wrong
				return stream.SendAndClose(&proto.WriteBlockResponse{
					Success:      false,
					ErrorCode:    proto.ErrorCode_UNKNOWN,
					ErrorMessage: "Received metadata twice for the same block",
				})
			}

			blockID = payload.Metadata.BlockId
			blockSize := payload.Metadata.Size

			fmt.Printf("Received metadata for block %s, size: %d\n", blockID, blockSize)

			// 3. Open a local file for writing
			path := filepath.Join(dn.StorageInfo.storageDir, blockID)
			f, err := os.Create(path)
			if err != nil {
				return stream.SendAndClose(&proto.WriteBlockResponse{
					Success:      false,
					ErrorCode:    proto.ErrorCode_UNKNOWN,
					ErrorMessage: fmt.Sprintf("Failed to create file: %v", err),
				})
			}

			file = f
			opened = true
		case *proto.WriteBlockRequest_DataChunk:
			if !opened {
				// Data arrived before metadata
				return stream.SendAndClose(&proto.WriteBlockResponse{
					Success:      false,
					ErrorCode:    proto.ErrorCode_UNKNOWN,
					ErrorMessage: "Received data chunk before metadata",
				})
			}

			chunk := payload.DataChunk
			n, writeErr := file.Write(chunk)
			if writeErr != nil {
				return stream.SendAndClose(&proto.WriteBlockResponse{
					Success:      false,
					ErrorCode:    proto.ErrorCode_UNKNOWN,
					ErrorMessage: fmt.Sprintf("Failed to write chunk: %v", writeErr),
				})
			}

			totalBytesWritten += int64(n)
		default:
			// Unrecognized oneof
			return stream.SendAndClose(&proto.WriteBlockResponse{
				Success:      false,
				ErrorCode:    proto.ErrorCode_UNKNOWN,
				ErrorMessage: "Unknown payload type",
			})
		}

	}
	// 5. Stream ended normally (client closed the stream)
	if file != nil {
		if err := file.Close(); err != nil {
			return status.Errorf(codes.Unknown, "Failed to close file: %v", err)
		}
	}

	fmt.Printf("Wrote %d bytes for block %s\n", totalBytesWritten, blockID)

	// 6. Return final success response
	return stream.SendAndClose(&proto.WriteBlockResponse{
		Success:      true,
		ErrorCode:    proto.ErrorCode_NONE,
		ErrorMessage: fmt.Sprintf("Wrote %d bytes for block %s", totalBytesWritten, blockID),
	})
}
