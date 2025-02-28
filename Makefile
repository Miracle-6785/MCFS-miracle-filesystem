# Define variables
BINARY_NAME=bin/mfs
API_BINARY_NAME=bin/api
GO_FILES=$(shell find . -type f -name '*.go' -not -path "./vendor/*")

.PHONY: all build api-build run api-run test clean fmt lint

# Default target
all: build

generate:
	@echo "Generating protobuf..."
	@protoc --go_out=. --go-grpc_out=. pkg/proto/*.proto

# Build the main MFS binary
build:
	@echo "Building $(BINARY_NAME)..."
	@go build -o $(BINARY_NAME) ./main.go
	@echo "Build complete."

# Build the API service
api-build:
	@echo "Building API service..."
	@go build -o $(API_BINARY_NAME) ./cmd/api/main.go
	@echo "API build complete."

# Run the main MFS binary
run: build
	@echo "Running $(BINARY_NAME)..."
	@./$(BINARY_NAME)

# Run the API service
api-run: api-build
	@echo "Running API service on port 8080..."
	@./$(API_BINARY_NAME)

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Format Go code
fmt:
	@echo "Formatting code..."
	@find . -name '*.go' -not -path "./vendor/*" -exec go fmt {} +

# Lint Go code
lint:
	@echo "Linting code..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo >&2 "golangci-lint not installed. Run 'go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest'"; exit 1; }
	@golangci-lint run || echo "Linting found issues."

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	@rm -f $(BINARY_NAME) $(API_BINARY_NAME)
	@echo "Cleanup complete."
