# Use an official Go image for building the binary
FROM golang:1.23 AS builder

WORKDIR /app

# Copy dependency files and download modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code and build the application
COPY . .
RUN CGO_ENABLED=0 go build -o main .

# Use a minimal base image for the runtime
FROM gcr.io/distroless/static

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy the service account JSON file if you are running the server locally
# COPY sa.json .

# Expose the default port
EXPOSE 8080

# Command to run the application
CMD ["./main"]
