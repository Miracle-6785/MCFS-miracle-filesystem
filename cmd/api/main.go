package main

import (
	"log"

	"github.com/Miracle-6785/mfs/pkg/api"
)

func main() {
	api.InitGRPCClients()

	r := api.SetupRouter()

	log.Println("Starting API server on :8080")
	r.Run(":8080")
}
