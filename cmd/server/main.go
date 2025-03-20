package main

import (
    "flag"
    "log"
    "net"

    "Distributed-Lock-Manager/internal/server"
    pb "Distributed-Lock-Manager/proto"

    "google.golang.org/grpc"
)

func main() {
    // Define a flag for the address with a default value of ":50051"
    address := flag.String("address", ":50051", "Address to listen on")
    flag.Parse()

    // Initialize the files
    server.CreateFiles()

    // Set up TCP listener using the specified address
    lis, err := net.Listen("tcp", *address)
    if err != nil {
        log.Fatalf("Failed to listen on %s: %v", *address, err)
    }

    // Create gRPC server
    s := grpc.NewServer()
    pb.RegisterLockServiceServer(s, server.NewLockServer())

    // Log the address the server is listening on
    log.Printf("Server listening at %v", lis.Addr())
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}