package main

import (
	"context"
	"log"
	"net"
	"os"

	pb "github.com/TFMV/slatedb_demo/proto"
	golog "github.com/go-kit/log"
	"github.com/slatedb/slatedb-go/slatedb"
	"github.com/thanos-io/objstore/providers/gcs"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
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
		if err.Error() == "key not found" {
			return &pb.GetResponse{Message: "Key not found"}, nil
		}
		return nil, err
	}

	return &pb.GetResponse{Value: string(value), Message: "Successfully retrieved key"}, nil
}

func (s *SlateDBServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	key := []byte(req.GetKey())

	s.db.Delete(key)
	return &pb.DeleteResponse{Message: "Successfully deleted key"}, nil
}

func main() {
	// Context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Logger setup
	logger := golog.NewLogfmtLogger(golog.NewSyncWriter(os.Stderr))

	// Read the service account key file
	serviceAccountKey, err := os.ReadFile("/Users/thomasmcgeehan/slatedb_demo/slatedb_demo/sa.json")
	if err != nil {
		log.Fatalf("failed to read service account key file: %v", err)
	}

	// GCS configuration
	bucketConfig := gcs.Config{
		Bucket:         "slate_demo",              // Replace with your actual bucket name
		ServiceAccount: string(serviceAccountKey), // Pass the JSON content
		UseGRPC:        true,                      // Adjust based on your GCS setup
	}

	// Serialize the configuration to YAML
	configBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		log.Fatalf("failed to marshal GCS config: %v", err)
	}

	// Create the GCS bucket
	bucket, err := gcs.NewBucket(ctx, logger, configBytes, "gcs")
	if err != nil {
		log.Fatalf("Failed to initialize GCS bucket: %v", err)
	}

	// Initialize SlateDB
	db, err := slatedb.Open("slate_demo", bucket)
	if err != nil {
		log.Fatalf("Failed to open SlateDB: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing SlateDB: %v", err)
		}
	}()

	// Start gRPC server
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSlateDBServer(grpcServer, NewSlateDBServer(db))

	log.Println("gRPC server started on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
