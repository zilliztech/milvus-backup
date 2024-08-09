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

sed -i "" -e "s/google.protobuf.Value db_collections/string db_collections/g" backup.proto
sed -i "" -e "s/import \"google\/protobuf\/struct.proto\"; \/\/ proto define/\/\/ proto define/g" backup.proto

${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./backuppb backup.proto

# remove has_index omitempty
sed -i "" -e "s/has_index,omitempty/has_index/g" ./backuppb/backup.pb.go
# remove data omitempty
sed -i "" -e "s/data,omitempty/data/g" ./backuppb/backup.pb.go
# remove size omitempty
sed -i "" -e "s/size,omitempty/size/g" ./backuppb/backup.pb.go
# remove progress omitempty
sed -i "" -e "s/progress,omitempty/progress/g" ./backuppb/backup.pb.go
# remove state_code omitempty
sed -i "" -e "s/state_code,omitempty/state_code/g" ./backuppb/backup.pb.go

popd

swag init

pushd ${BACK_PROTO_DIR}

sed -i "" -e "s/string db_collections/google.protobuf.Value db_collections/g" backup.proto
sed -i "" -e "s/\/\/ proto define/import \"google\/protobuf\/struct.proto\"; \/\/ proto define/g" backup.proto

${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./backuppb backup.proto

popd