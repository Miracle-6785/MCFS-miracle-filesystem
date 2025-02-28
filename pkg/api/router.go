package api

import "github.com/gin-gonic/gin"

func SetupRouter() *gin.Engine {
	r := gin.Default()

	r.POST("/upload", UploadFileHandler)

	return r
}
