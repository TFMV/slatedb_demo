# SlateDB Demo

A simple demonstration of a key-value store using SlateDB with gRPC interface.

## Overview

This project implements a basic key-value store service using SlateDB as the storage engine and gRPC for client-server communication. It supports three basic operations:

- Put: Store a key-value pair
- Get: Retrieve a value by key
- Delete: Remove a key-value pair

## Project Structure

## Server Configuration

The server runs on port 50051 by default and uses an in-memory bucket for storage. The data is stored in `/tmp/testDB`.

## Dependencies

- github.com/slatedb/slatedb-go
- github.com/thanos-io/objstore
- google.golang.org/grpc
