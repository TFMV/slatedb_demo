package main

import (
	"context"
	"log"
	"time"

	pb "github.com/TFMV/slatedb_demo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect to the gRPC server
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewSlateDBClient(conn)

	// Example 1: Put a key-value pair
	putResp, err := client.Put(ctx, &pb.PutRequest{
		Key:   "exampleKey",
		Value: "exampleValue",
	})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	log.Printf("Put response: %s", putResp.Message)

	// Example 2: Get the value for a key
	getResp, err := client.Get(ctx, &pb.GetRequest{
		Key: "exampleKey",
	})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	log.Printf("Get response: Key = %s, Value = %s", "exampleKey", getResp.Value)

	// Example 3: Delete a key
	delResp, err := client.Delete(ctx, &pb.DeleteRequest{
		Key: "exampleKey",
	})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	log.Printf("Delete response: %s", delResp.Message)

	// Attempt to get the deleted key
	getResp, err = client.Get(ctx, &pb.GetRequest{
		Key: "exampleKey",
	})
	if err != nil {
		log.Printf("Get after delete failed (expected): %v", err)
	} else {
		log.Printf("Unexpected Get response after delete: %s", getResp.Value)
	}
}
