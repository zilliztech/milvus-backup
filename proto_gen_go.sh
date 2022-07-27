#!/usr/bin/env bash

PWD 	  := $(shell pwd)

PROGRAM=${PWD}
GOPATH=$(go env GOPATH)
PROTO_DIR=$PROGRAM/proto/

echo ${PROGRAM}
export protoc=${PROGRAM}/build/thirdparty/protobuf-build/protoc
which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && cd /tmp && go install github.com/golang/protobuf/protoc-gen-go@v1.3.2)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

export PATH=${GOPATH}/bin:$PATH
echo `which protoc-gen-go`

pushd ${PROTO_DIR}

mkdir -p backuppb

${protoc} --proto_path=. --go_out=plugins=grpc,paths=source_relative:./backuppb backup.proto

popd
