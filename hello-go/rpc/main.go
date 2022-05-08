package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	fmt.Println("vim-go")
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Failed to listen on port 9000: %v", err)
	}

	grpcServer := grpc.NewServer()
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve on port 9000: %v", err)
	}
}
