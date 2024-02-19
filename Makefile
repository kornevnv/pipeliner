PROJECT_NAME=pipeliner
CURRENT_DIR?=$(shell pwd)


test:
	@docker run --name $(PROJECT_NAME)-test --rm -i -v "$(CURRENT_DIR):/$(PROJECT_NAME)" -w "/$(PROJECT_NAME)" golang:1.20 bash -c "go test -failfast ./... -coverprofile=../../coverage.out > ../../test-report.txt && go tool cover -func=../../coverage.out >> ../../test-report.txt || (cat ../../test-report.txt && exit 2)"

coverage:
	@docker run --name $(PROJECT_NAME)-coverage --rm -i -v "$(CURRENT_DIR):/$(PROJECT_NAME)" -w "/$(PROJECT_NAME)" test-coverage:latest bash -c "test-coverage -report=../../test-report.txt -limit=0"

lint:
	@docker run --name $(PROJECT_NAME)-lint --rm -i -v "$(CURRENT_DIR):/$(PROJECT_NAME)" -w "/$(PROJECT_NAME)" golangci/golangci-lint:v1.52-alpine golangci-lint run ./... -E gofmt -E bodyclose -E gosec -E goconst -E unconvert -E gocritic -E nestif -E asciicheck --skip-dirs=./example --timeout=10m
