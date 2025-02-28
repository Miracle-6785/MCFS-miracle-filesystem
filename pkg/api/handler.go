package api

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/Miracle-6785/mfs/generate/proto"
	"github.com/Miracle-6785/mfs/internal/config"
	"github.com/gin-gonic/gin"
)

// UploadFileHandler handles the file upload from the user via multipart form data.
func UploadFileHandler(c *gin.Context) {
	// 1. Parse the form-data to get the file and DFS file path
	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No 'file' part in request"})
		return
	}

	dfsFilePath := c.PostForm("file_path")
	if dfsFilePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File path missing (use form field 'file_path')"})
		return
	}

	// 2. Open the uploaded file for reading
	uploadedFile, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to open uploaded file"})
		return
	}
	defer uploadedFile.Close()

	// 3. Create the file metadata in the NameNode
	createResp, err := NameNodeClient.CreateFile(context.Background(), &proto.CreateFileRequest{
		Path: dfsFilePath,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("CreateFile RPC failed: %v", err)})
		return
	}
	if !createResp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": createResp.Message})
		return
	}

	// 4. Chunk the file (64MB per chunk here; pick your block size)
	blockSize := int64(config.BLOCK_SIZE) // 64MB
	err = uploadFileInChunks(uploadedFile, blockSize, dfsFilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 5. Optionally, call a "CompleteFile" RPC if your design requires finalizing
	// (Not shown here; depends on your architecture.)

	c.JSON(http.StatusOK, gin.H{"message": "File uploaded successfully"})
}

func uploadFileInChunks(file io.Reader, blockSize int64, dfsFilePath string) error {
	buf := make([]byte, blockSize)

	for {
		// Read up to 'blockSize' bytes from the uploaded file
		n, readErr := file.Read(buf)
		if n > 0 {
			chunkData := buf[:n]

			// (a) Ask NameNode for a new block allocation
			allocResp, allocErr := NameNodeClient.AllocateBlock(context.Background(), &proto.AllocateBlockRequest{
				FilePath: dfsFilePath,
				Size:     int64(n), // letting NameNode know how large this block is
			})
			if allocErr != nil {
				return fmt.Errorf("AllocateBlock failed: %v", allocErr)
			}

			// Ensure there's at least one DataNode to store this block
			if len(allocResp.Datanodes) == 0 {
				return fmt.Errorf("no DataNodes available for block allocation")
			}

			// (b) Write chunkData to the first DataNode in the list
			//     (If replication factor > 1, you might replicate to others, or do pipeline replication.)
			dataNodeAddr := allocResp.Datanodes[0].Ip + ":" + allocResp.Datanodes[0].Port
			err := writeChunkToDataNode(allocResp.BlockId, chunkData, dataNodeAddr)
			if err != nil {
				return fmt.Errorf("writeChunkToDataNode failed: %v", err)
			}
		}

		// If we hit EOF, we are done reading the file
		if readErr == io.EOF {
			break
		}
		// If there's some other read error, return it
		if readErr != nil {
			return fmt.Errorf("error reading file chunk: %v", readErr)
		}
	}
	return nil
}

func writeChunkToDataNode(blockID string, chunkData []byte, dataNodeAddr string) error {
	// 1. Create a gRPC client for the DataNode
	dataNodeClient := InitDataNodeClient(dataNodeAddr)

	// 2. Start a client-streaming call
	stream, err := dataNodeClient.WriteBlock(context.Background())
	if err != nil {
		return fmt.Errorf("failed to open WriteBlock stream: %v", err)
	}

	// 3. First message: block metadata
	metaMsg := &proto.WriteBlockRequest{
		Payload: &proto.WriteBlockRequest_Metadata{
			Metadata: &proto.BlockMetadata{
				BlockId: blockID,
				Size:    int64(len(chunkData)), // or the entire block length
			},
		},
	}
	if err := stream.Send(metaMsg); err != nil {
		return fmt.Errorf("failed to send metadata: %v", err)
	}

	// 4. Second message: the actual data chunk
	dataMsg := &proto.WriteBlockRequest{
		Payload: &proto.WriteBlockRequest_DataChunk{
			DataChunk: chunkData,
		},
	}
	if err := stream.Send(dataMsg); err != nil {
		return fmt.Errorf("failed to send data chunk: %v", err)
	}

	// 5. Close the sending side, wait for server's response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("WriteBlock CloseAndRecv failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("WriteBlock unsuccessful: %s (error_code=%v)",
			resp.ErrorMessage, resp.ErrorCode)
	}

	return nil
}
