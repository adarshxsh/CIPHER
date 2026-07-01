FROM golang:alpine

WORKDIR /app

# Disable CGO for building statically linked binary
ENV CGO_ENABLED=0

# Copy go.mod and go.sum and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the peer binary
RUN go build -o peer cmd/peer/main.go

# Start the peer by default
ENTRYPOINT ["/app/peer"]
