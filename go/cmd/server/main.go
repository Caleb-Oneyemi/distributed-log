package main

import (
	"log"

	"github.com/Caleb-Oneyemi/distributed-log/internal/server"
)

func main() {
	srv := server.NewHttpServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
