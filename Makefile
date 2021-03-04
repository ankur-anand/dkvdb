default:
	@echo "Specify command to run"

# Run Quick unit tests.
test:
	@echo "[go test] running unit tests"
	@-rm -r $(COVERAGE_DIR)
	@mkdir $(COVERAGE_DIR)
	@go test -v ./...

# set COVERAGE_DIR If not set
COVERAGE_DIR ?= .coverage
.PHONY: testr
testr:
	@echo "[go test] running unit tests with race flag and collecting coverage metrics"
	@-rm -r $(COVERAGE_DIR)
	@mkdir $(COVERAGE_DIR)
	@go test -v -race -covermode atomic -coverprofile $(COVERAGE_DIR)/combined.txt ./...

# get the html coverage
coverage:
	@go tool cover -html=$(COVERAGE_DIR)/combined.txt

# Run all lint
.PHONY: lint
lint: lint-check-deps
	@echo "[golangci-lint] linting sources"
	@golangci-lint run \
		-E misspell \
		-E golint \
		-E gofmt \
		-E unconvert \
		--exclude-use-default=false \
		./...

# Install the lint dependencies
.PHONY: lint-check-deps
lint-check-deps:
	@if [ -z `which golangci-lint` ]; then \
		echo "[go get] installing golangci-lint";\
		go get -u github.com/golangci/golangci-lint/cmd/golangci-lint;\
	fi

# go mod tidy
modtidy:
	@go mod  tidy

# Generate protobuff from compiler. 
protoc:
	# `-I.` resolve all proto imports path relative to the current directory.
	# compiled using protov2 API
	@protoc proto/raftkv/*.proto -I. --go_out=:.
	@protoc proto/config/*.proto -I. --go_out=:.

