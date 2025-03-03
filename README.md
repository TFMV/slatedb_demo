# SlateDB Demo

A demonstration of a high-performance key-value store using SlateDB with gRPC interface.

## Overview

This project implements a key-value store service using SlateDB as the storage engine and gRPC for client-server communication. It supports various operations including:

- Basic operations (Put, Get, Delete)
- Batch operations (BatchPut, BatchGet, BatchDelete)
- Scanning operations (PrefixScan, RangeScan)
- Statistics and monitoring

## Project Structure

```text
slatedb_demo/
├── proto/
│   └── slatedb.proto    # Protocol buffer definitions
├── example/
│   └── client.go        # Simple gRPC client
├── cmd/
│   └── fancy_cli/       # Fancy CLI demo
│       ├── main.go      # CLI implementation
│       └── README.md    # CLI documentation
├── server.go            # gRPC server implementation
└── README.md            # This file
```

## Getting Started

1. Install dependencies:

```bash
go mod tidy
```

2. Start the server:

```bash
go run server.go
```

3. Run the simple client (in a separate terminal):

```bash
go run example/client.go
```

4. Or try the fancy CLI demo (in a separate terminal):

```bash
cd cmd/fancy_cli
go run main.go
```

## Server Configuration

The SlateDB server can be configured using the following environment variables:

- `PORT`: The port on which the server will listen (default: "5423")
- `BUCKET_NAME`: The name of the GCS bucket to use for storage (required)

Example:

```bash
PORT=5423 BUCKET_NAME=slate_demo_local go run server.go
```

## Fancy CLI Demo

The SlateDB Fancy CLI provides an interactive and colorful interface for exploring SlateDB features. It includes:

- Interactive menus for different operations
- Colorful output for better readability
- Tabular display of key-value data
- Structured demo scenario

### Running the CLI Demo

The CLI can be configured using the following environment variable:

- `SERVER_ADDR`: The address of the SlateDB server (default: "localhost:5423")

To run the CLI demo:

```bash
# Navigate to the CLI directory
cd cmd/fancy_cli

# Run the CLI (make sure the server is running)
go run main.go

# Or specify a custom server address
SERVER_ADDR=localhost:8080 go run main.go
```

For more details, see the [CLI README](cmd/fancy_cli/README.md).

## API Reference

### Basic Operations

- `Put(key, value)`: Store a key-value pair
- `Get(key)`: Retrieve a value by key
- `Delete(key)`: Remove a key-value pair

### Batch Operations

- `BatchPut(entries)`: Store multiple key-value pairs
- `BatchGet(keys)`: Retrieve multiple values by keys
- `BatchDelete(keys)`: Remove multiple keys

### Scanning Operations

- `PrefixScan(prefix, limit)`: Find all keys with a specific prefix
- `RangeScan(startKey, endKey, limit)`: Find all keys within a specific range

### Statistics

- `GetStats()`: Get database statistics (key count, size, etc.)

## Dependencies

- github.com/slatedb/slatedb-go
- github.com/thanos-io/objstore
- google.golang.org/grpc
- github.com/fatih/color (for fancy CLI)
- github.com/rodaine/table (for fancy CLI)
