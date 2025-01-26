package main

import (
	"context"
	"log"
	"net"

	pb "github.com/TFMV/slatedb_demo/proto"

	"github.com/slatedb/slatedb-go/slatedb"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc"
)

type SlateDBServer struct {
	pb.UnimplementedSlateDBServer
	db *slatedb.DB
}

func NewSlateDBServer(db *slatedb.DB) *SlateDBServer {
	return &SlateDBServer{db: db}
}

func (s *SlateDBServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	key := []byte(req.GetKey())
	value := []byte(req.GetValue())

	s.db.Put(key, value)
	return &pb.PutResponse{Message: "Successfully put key"}, nil
}

func (s *SlateDBServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	key := []byte(req.GetKey())

	value, err := s.db.Get(key)
	if err != nil {
		return &pb.GetResponse{Message: "Key not found"}, nil
	}

	return &pb.GetResponse{Value: string(value), Message: "Successfully retrieved key"}, nil
}

func (s *SlateDBServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	key := []byte(req.GetKey())

	s.db.Delete(key)
	return &pb.DeleteResponse{Message: "Successfully deleted key"}, nil
}

func main() {
	// Set up SlateDB
	bucket := objstore.NewInMemBucket()

	db, err := slatedb.Open("/tmp/testDB", bucket)
	if err != nil {
		log.Fatalf("failed to open SlateDB: %v", err)
	}
	defer db.Close()

	// Start gRPC server
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSlateDBServer(grpcServer, NewSlateDBServer(db))

	log.Println("Starting gRPC server on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
