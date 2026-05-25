MODULE := dash0.com/otlp-metric-store

.PHONY: build run test test-integration test-all fmt vet lint tidy clean local-up local-down local-logs send-metrics demo-up demo-down

build:
	go build ./...

run:
	go run ./cmd/

test:
	go test -count=1 ./...

test-integration:
	go test -tags integration -count=1 -v ./...

test-all: test test-integration

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: vet
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

tidy:
	go mod tidy

clean:
	go clean ./...

local-up:
	docker compose up -d --wait

local-down:
	docker compose down

local-logs:
	docker compose logs -f

send-metrics:
	docker compose --profile send-metrics run --rm telemetrygen

demo-up: local-up
	-@lsof -ti :4317 -ti :13133 | xargs kill -9 2>/dev/null; true
	go run ./cmd/ &
	@echo "Service started. Run 'make send-metrics' to generate data, 'make demo-down' to stop."

demo-down:
	-pkill -f "go run ./cmd/" 2>/dev/null
	-pkill -f "otlp-log-processor-backend" 2>/dev/null
	$(MAKE) local-down
