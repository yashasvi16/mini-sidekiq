.PHONY: build test test-race test-integration docker-up docker-down run-server run-dashboard fmt vet clean

build:
	go build -o bin/server ./cmd/server
	go build -o bin/dashboard ./cmd/dashboard

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

# Broker tests hit real Redis/Postgres via docker-compose - bring those up first.
test-integration: docker-up
	go test -race -count=1 ./internal/broker/...

docker-up:
	docker compose up -d

docker-down:
	docker compose down

run-server: docker-up
	go run ./cmd/server

run-dashboard: docker-up
	go run ./cmd/dashboard

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -rf bin/
