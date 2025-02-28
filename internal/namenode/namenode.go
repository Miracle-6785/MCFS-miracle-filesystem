package namenode

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/Miracle-6785/mfs/generate/proto"
	"github.com/Miracle-6785/mfs/internal/config"
	"github.com/Miracle-6785/mfs/pkg/common"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	Port        string
	LastSeen    time.Time
	Capacity    int64
	Used        int64
	BlockStored map[string]bool
}

func NewNameNode(blockSize int64, replicaFactor int) *NameNode {
	return &NameNode{
		root: &FileSystemNode{
			Name:        config.NODENAME_ROOT_DIR,
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
		Address:     req.Address.GetIp(),
		Port:        req.Address.GetPort(),
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

// Return all tailor DataNodes for the block
func (nn *NameNode) AllocateBlock(ctx context.Context, req *proto.AllocateBlockRequest) (*proto.AllocateBlockResponse, error) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	// Find the tailored DataNodes for the block
	tailoredDataNodes, err := nn.getTailoredDataNodes(req.Size)

	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "Failed to find DataNodes: %v", err)
	}

	resNodes := make([]*proto.DataNodeInfo, 0, len(tailoredDataNodes))

	for _, node := range tailoredDataNodes {
		resNodes = append(resNodes, &proto.DataNodeInfo{
			DatanodeId: node.ID,
			Ip:         node.Address,
			Port:       node.Port,
		})
	}

	// Generate a new block ID
	blockID := uuid.New()
	return &proto.AllocateBlockResponse{
		BlockId:   blockID.String(),
		Datanodes: resNodes,
	}, nil
}

func (nn *NameNode) getTailoredDataNodes(demandSize int64) ([]*DataNodeInfo, error) {
	nodes := make([]*DataNodeInfo, 0, len(nn.dataNodes))

	for _, node := range nn.dataNodes {
		nodes = append(nodes, node)
	}
	// Sort DataNode by their storage utilization
	slices.SortFunc(nodes, func(a, b *DataNodeInfo) int {
		// Calculate utilization
		utilA := float64(a.Used) / float64(a.Capacity)
		utilB := float64(b.Used) / float64(b.Capacity)

		// Return -1, 0, or 1 for sorting
		switch {
		case utilA < utilB:
			return -1
		case utilA > utilB:
			return 1
		default:
			return 0
		}
	})
	// Filter out DataNodes that do not have enough space
	filterNodes := make([]*DataNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if node.Capacity-node.Used >= demandSize {
			filterNodes = append(filterNodes, node)
		}
	}

	// Return the number of DataNodes equal to the replica factor
	return nodes[:nn.replicaFactor], nil
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
