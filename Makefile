PROXY_OLD=$(shell go env GOPROXY)
PROXY=https://goproxy.cn,direct
.PHONY:vet lint upgrade
vet:
	@echo "Running go vet"
	@go env -w GOPROXY=$(PROXY)
	@go mod tidy
	@go vet ./...
	@go env -w GOPROXY=$(PROXY_OLD)
lint:vet
	@echo "Running golangci-lint"
	@hash golangci-lint > /dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run
upgrade:
	@echo "Upgrading..."
	@go env -w GOPROXY=$(PROXY)
	@go mod tidy
	@go get -d -u -v ./...
	@go mod tidy
	@go env -w GOPROXY=$(PROXY_OLD)
	@echo "Upgrade complete."