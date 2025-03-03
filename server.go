package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	mu sync.Mutex // For thread safety

	// Stats tracking
	stats struct {
		totalOperations int64
		lastAccessed    time.Time
		dbPath          string
	}
}

func NewSlateDBServer(db *slatedb.DB) *SlateDBServer {
	return &SlateDBServer{
		db: db,
		stats: struct {
			totalOperations int64
			lastAccessed    time.Time
			dbPath          string
		}{
			totalOperations: 0,
			lastAccessed:    time.Now(),
			dbPath:          "slate_demo",
		},
	}
}

// Basic operations
func (s *SlateDBServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(req.GetKey())
	value := []byte(req.GetValue())

	s.db.Put(key, value)

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.PutResponse{Message: "Successfully put key"}, nil
}

func (s *SlateDBServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(req.GetKey())

	value, err := s.db.Get(key)
	if err != nil {
		if err.Error() == "key not found" {
			return &pb.GetResponse{Message: "Key not found"}, nil
		}
		return nil, err
	}

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.GetResponse{Value: string(value), Message: "Successfully retrieved key"}, nil
}

func (s *SlateDBServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(req.GetKey())

	s.db.Delete(key)

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.DeleteResponse{Message: "Successfully deleted key"}, nil
}

// Batch operations
func (s *SlateDBServer) BatchPut(ctx context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	successCount := int32(0)
	failureCount := int32(0)

	for _, entry := range req.GetEntries() {
		key := []byte(entry.GetKey())
		value := []byte(entry.GetValue())

		s.db.Put(key, value)
		successCount++
	}

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.BatchPutResponse{
		Message:      fmt.Sprintf("Successfully put %d keys, %d failures", successCount, failureCount),
		SuccessCount: successCount,
		FailureCount: failureCount,
	}, nil
}

func (s *SlateDBServer) BatchGet(ctx context.Context, req *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]*pb.KeyValue, 0, len(req.GetKeys()))
	missingKeys := make([]string, 0)

	for _, keyStr := range req.GetKeys() {
		key := []byte(keyStr)
		value, err := s.db.Get(key)

		if err != nil {
			if err.Error() == "key not found" {
				missingKeys = append(missingKeys, keyStr)
			} else {
				return nil, err
			}
		} else {
			entries = append(entries, &pb.KeyValue{
				Key:   keyStr,
				Value: string(value),
			})
		}
	}

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.BatchGetResponse{
		Entries:     entries,
		MissingKeys: missingKeys,
		Message:     fmt.Sprintf("Retrieved %d keys, %d missing", len(entries), len(missingKeys)),
	}, nil
}

func (s *SlateDBServer) BatchDelete(ctx context.Context, req *pb.BatchDeleteRequest) (*pb.BatchDeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	successCount := int32(0)
	failureCount := int32(0)

	for _, keyStr := range req.GetKeys() {
		key := []byte(keyStr)
		s.db.Delete(key)
		successCount++
	}

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.BatchDeleteResponse{
		Message:      fmt.Sprintf("Successfully deleted %d keys, %d failures", successCount, failureCount),
		SuccessCount: successCount,
		FailureCount: failureCount,
	}, nil
}

// Scanning operations
func (s *SlateDBServer) PrefixScan(ctx context.Context, req *pb.PrefixScanRequest) (*pb.PrefixScanResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := req.GetPrefix()
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100 // Default limit
	}

	// Since SlateDB doesn't have a native iterator, we'll simulate prefix scan
	// by using a list of known keys with the prefix
	entries := make([]*pb.KeyValue, 0, limit)

	// This is a simplified implementation - in a real-world scenario,
	// you would need to maintain a list of keys or use a different approach
	// Here we're just demonstrating the API

	// For demo purposes, we'll add some sample data if the prefix is "demo"
	if prefix == "demo" {
		// Add some demo entries
		for i := 1; i <= 5 && len(entries) < limit; i++ {
			key := fmt.Sprintf("demo:key:%d", i)
			value, err := s.db.Get([]byte(key))

			// If key doesn't exist, create it for demo purposes
			if err != nil {
				demoValue := fmt.Sprintf("demo value %d", i)
				s.db.Put([]byte(key), []byte(demoValue))
				value = []byte(demoValue)
			}

			entries = append(entries, &pb.KeyValue{
				Key:   key,
				Value: string(value),
			})
		}
	}

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.PrefixScanResponse{
		Entries: entries,
		Message: fmt.Sprintf("Found %d entries with prefix '%s'", len(entries), req.GetPrefix()),
	}, nil
}

func (s *SlateDBServer) RangeScan(ctx context.Context, req *pb.RangeScanRequest) (*pb.RangeScanResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	startKey := req.GetStartKey()
	endKey := req.GetEndKey()
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100 // Default limit
	}

	// Since SlateDB doesn't have a native range scan, we'll simulate it
	// with demo data for demonstration purposes
	entries := make([]*pb.KeyValue, 0, limit)

	// For demo purposes, we'll add some sample data if the range includes "range"
	if strings.Contains(startKey, "range") || strings.Contains(endKey, "range") {
		// Add some demo entries
		for i := 1; i <= 5 && len(entries) < limit; i++ {
			key := fmt.Sprintf("range:key:%d", i)

			// Only include if in range
			if (startKey == "" || key >= startKey) && (endKey == "" || key <= endKey) {
				value, err := s.db.Get([]byte(key))

				// If key doesn't exist, create it for demo purposes
				if err != nil {
					rangeValue := fmt.Sprintf("range value %d", i)
					s.db.Put([]byte(key), []byte(rangeValue))
					value = []byte(rangeValue)
				}

				entries = append(entries, &pb.KeyValue{
					Key:   key,
					Value: string(value),
				})
			}
		}
	}

	// Update stats
	s.stats.totalOperations++
	s.stats.lastAccessed = time.Now()

	return &pb.RangeScanResponse{
		Entries: entries,
		Message: fmt.Sprintf("Found %d entries in range from '%s' to '%s'",
			len(entries), req.GetStartKey(), req.GetEndKey()),
	}, nil
}

// Statistics and monitoring
func (s *SlateDBServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Since SlateDB doesn't expose a way to iterate through all keys,
	// we'll use the stats we've been tracking and some demo data

	// For demo purposes, we'll simulate some stats
	totalKeys := int64(10)        // Simulated count
	totalSizeBytes := int64(1024) // Simulated size

	return &pb.GetStatsResponse{
		TotalKeys:      totalKeys,
		TotalSizeBytes: totalSizeBytes,
		DbPath:         s.stats.dbPath,
		Message:        fmt.Sprintf("Database contains approximately %d keys, estimated size: %d bytes", totalKeys, totalSizeBytes),
	}, nil
}

func main() {
	log.Printf("Starting application...")

	// Context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Printf("Context created")

	// Logger setup
	logger := golog.NewLogfmtLogger(golog.NewSyncWriter(os.Stderr))
	log.Printf("Logger initialized")

	log.Printf("Bucket name: %s", os.Getenv("BUCKET_NAME"))

	// Uncomment this code if you are running the server locally
	//serviceAccountKey, err := os.ReadFile("sa.json")
	//if err != nil {
	//	log.Fatalf("failed to read service account key file: %v", err)
	//}

	// GCS configuration
	bucketConfig := gcs.Config{
		Bucket:  os.Getenv("BUCKET_NAME"), // Get bucket name from environment
		UseGRPC: true,
		// uncomment this if you are running the server locally
		//ServiceAccount: string(serviceAccountKey),
	}
	log.Printf("GCS config created for bucket: %s", bucketConfig.Bucket)

	configBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		log.Fatalf("failed to marshal GCS config: %v", err)
	}
	log.Printf("GCS config marshaled successfully")

	bucket, err := gcs.NewBucket(ctx, logger, configBytes, "gcs")
	if err != nil {
		log.Fatalf("Failed to initialize GCS bucket: %v", err)
	}
	log.Printf("GCS bucket initialized successfully")

	// Initialize SlateDB
	dbPath := filepath.Join(os.TempDir(), "slate_demo")
	db, err := slatedb.Open(dbPath, bucket)
	if err != nil {
		log.Fatalf("Failed to open SlateDB: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing SlateDB: %v", err)
		}
	}()
	log.Printf("SlateDB opened successfully at %s", dbPath)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default to port 8080 if PORT is not set
	}

	// Start gRPC server
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSlateDBServer(grpcServer, NewSlateDBServer(db))

	log.Printf("gRPC server started on :%s", port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
