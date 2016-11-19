#!/bin/sh

set -eu # exit on error; error on undefined variables
set -x  # echo 

# embed templates
go generate -x ./templates

# compile and install test packages and dependencies (faster future tests)
go test -v -i ./...
go test -race -v -i ./...
go test -race -v ./... || (echo "FAILED" && exit 1)
go vet ./...
go build bqcost.go

# currently too noisy TODO: enable
#golint ./...

echo "PASSED"
