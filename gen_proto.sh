#!/usr/bin/env bash

PWD 	  := $(shell pwd)

PROGRAM=${PWD}
GOPATH=$(go env GOPATH)
BACK_PROTO_DIR=$PROGRAM/core/proto/
GOOGLE_PROTO_DIR=${PROGRAM}/build/thirdparty/protobuf-src/src/

echo ${PROGRAM}
export protoc=${PROGRAM}/build/thirdparty/protobuf-build/protoc
echo `${protoc} --version`
which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && cd /tmp && go install github.com/golang/protobuf/protoc-gen-go@v1.3.2)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

export PATH=${GOPATH}/bin:$PATH
echo `which protoc-gen-go`
echo ${BACK_PROTO_DIR}

pushd ${BACK_PROTO_DIR}

mkdir -p backuppb

${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./backuppb backup.proto

# remove has_index omitempty
sed -i "" -e "s/has_index,omitempty/has_index/g" ./backuppb/backup.pb.go
# remove data omitempty
sed -i "" -e "s/data,omitempty/data/g" ./backuppb/backup.pb.go
# remove size omitempty
sed -i "" -e "s/size,omitempty/size/g" ./backuppb/backup.pb.go

# to make db_collections field compatible to both json and string
#sed -i "" -e "s/*_struct.Value/interface{}/g" ./backuppb/backup.pb.go
#sed -i "" '/_struct "github.com\/golang\/protobuf\/ptypes\/struct"/d' ./backuppb/backup.pb.go

popd
