#!/bin/sh

set -eu # exit on error; error on undefined variables
set -x  # echo 


# compile and install test packages and depenencies (faster future tests)
go test -race -v -i ./...
go test -race -v ./... || (echo "FAILED" && exit 1)
go vet ./...
go build bqcost.go

# currently too noisy TODO: enable
#golint ./...

echo "PASSED"
