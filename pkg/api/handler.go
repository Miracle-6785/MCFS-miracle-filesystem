package api

import (
	"context"
	"net/http"

	"github.com/Miracle-6785/mfs/generate/proto"
	"github.com/gin-gonic/gin"
)

func CreateFileHandler(c *gin.Context) {
	var req struct {
		FileName string `json:"file_name"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Call gRPC NameNode service
	resp, err := NameNodeClient.CreateFile(context.Background(), &proto.CreateFileRequest{
		Path: req.FileName,
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create file"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": resp.Message})
}
