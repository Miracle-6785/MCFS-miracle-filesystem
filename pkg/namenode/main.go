package namenode

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Miracle-6785/mfs/generate/proto"
	"github.com/Miracle-6785/mfs/pkg/common"
)

// FileSystemNode represents an entry in the filesystem tree
type FileSystemNode struct {
	Name        string
	IsDirectory bool
	Size        int64
	ModTime     int64
	Children    map[string]*FileSystemNode
	Block       []*proto.BlockInfo
}

// NameNode manages the file system namespace and block mapping
type NameNode struct {
	proto.UnimplementedNameNodeServiceServer
	proto.UnimplementedDataNodeRegistryServiceServer

	mu            sync.RWMutex
	root          *FileSystemNode
	dataNodes     map[string]*DataNodeInfo
	blockSize     int64
	replicaFactor int
}

type DataNodeInfo struct {
	ID          string
	Address     string
	LastSeen    time.Time
	Capacity    int64
	Used        int64
	BlockStored map[string]bool
}

func NewNameNode(blockSize int64, replicaFactor int) *NameNode {
	return &NameNode{
		root: &FileSystemNode{
			Name:        "/tmp/mfs",
			IsDirectory: true,
			Children:    make(map[string]*FileSystemNode),
		},
		dataNodes:     make(map[string]*DataNodeInfo),
		blockSize:     blockSize,
		replicaFactor: replicaFactor,
	}
}

func (nn *NameNode) RegisterDataNode(ctx context.Context, req *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	// Record the DataNode in NameNode's datanodes map
	nn.dataNodes[req.DatanodeId] = &DataNodeInfo{
		ID:          req.DatanodeId,
		Address:     fmt.Sprintf("%s:%s", req.Address.GetIp(), req.Address.GetPort()),
		LastSeen:    time.Now(),
		Capacity:    req.StorageInfo.Capacity,
		Used:        req.StorageInfo.Used,
		BlockStored: make(map[string]bool),
	}

	// Log the ip address of the DataNode
	fmt.Printf("DataNode registered: %+v", nn.dataNodes[req.DatanodeId])

	return &proto.RegisterDataNodeResponse{
		Success: true,
		Message: "DataNode registered successfully",
	}, nil
}

func (nn *NameNode) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	// Update the last seen time of the DataNode
	nn.dataNodes[req.DatanodeId].LastSeen = time.Now()

	var storageUtilization float64

	if req.StorageInfo.Used == 0 {
		storageUtilization = 0
	} else {
		storageUtilization = float64(req.StorageInfo.Used) / float64(req.StorageInfo.Capacity)
	}

	log.Printf("Heartbeat from DataNode: %s with storage utilization: %f", req.DatanodeId,
		storageUtilization)

	return &proto.HeartbeatResponse{
		Success: true,
	}, nil
}

// CreateFile creates a new file in the filesystem
func (nn *NameNode) CreateFile(ctx context.Context, req *proto.CreateFileRequest) (*proto.CreateFileResponse, error) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	// Parse the input path (e.g: "/dir/subdir/file.txt")
	path := req.Path
	if path == "" || path == "/" {
		return &proto.CreateFileResponse{
				Success: false,
				Message: "Invalid file path",
			},
			nil
	}

	// Normalize any leading slashes
	// e.g. "/dir/subdir/file.txt" -> ["dir", "subdir", "file.txt"]
	parts := common.SplitPath(path)

	// Traverse the filesystem tree to find (or create) the parent directory
	parent, err := nn.traverseToParent(parts[:len(parts)-1], true)
	if err != nil {
		return &proto.CreateFileResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to traverse path: %v", err),
		}, nil
	}

	if !parent.IsDirectory {
		return &proto.CreateFileResponse{
			Success: false,
			Message: "Parent is not a directory",
		}, nil
	}

	// Check if a child with the same name already exists
	filename := parts[len(parts)-1]
	if _, exists := parent.Children[filename]; exists {
		return &proto.CreateFileResponse{
			Success: false,
			Message: "File already exists",
		}, nil
	}

	// Create a new FileSystemNode for the file
	newNode := &FileSystemNode{
		Name:        filename,
		IsDirectory: false,
		ModTime:     time.Now().Unix(),
		Children:    make(map[string]*FileSystemNode),
		Block:       nil,
	}

	// Insert the new node into the parent children map
	parent.Children[filename] = newNode

	return &proto.CreateFileResponse{
		Success: true,
		Message: "File created successfully",
	}, nil
}

// traverseToParent navigates the tree to the parent directory node of a path.
// If createDir is true, it will create intermediate directories if they do not exist.
// Paths should be the path components EXCEPT the last one.
func (nn *NameNode) traverseToParent(parts []string, createDirs bool) (*FileSystemNode, error) {
	current := nn.root

	for _, part := range parts {
		child, exists := current.Children[part]

		if !exists {
			if createDirs {
				// Create a directory node if it does not exist
				child = &FileSystemNode{
					Name:        part,
					IsDirectory: true,
					Children:    make(map[string]*FileSystemNode),
				}
				current.Children[part] = child
			} else {
				return nil, fmt.Errorf("Directory %q does not exist", part)
			}
		}

		if !child.IsDirectory {
			return nil, fmt.Errorf("Path component %q is not a directory", part)
		}

		current = child
	}

	return current, nil
}
