#!/usr/bin/env bash

PWD 	  := $(shell pwd)

PROGRAM=${PWD}
GOPATH=$(go env GOPATH)
BACK_PROTO_DIR=$PROGRAM/core/proto/
GOOGLE_PROTO_DIR=${PROGRAM}/build/thirdparty/protobuf-src/src/

echo ${PROGRAM}
export protoc=${PROGRAM}/build/thirdparty/protobuf-build/protoc
which protoc-gen-go 1>/dev/null || (echo "Installing protoc-gen-go" && cd /tmp && go install github.com/golang/protobuf/protoc-gen-go@v1.3.2)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi

export PATH=${GOPATH}/bin:$PATH
echo `which protoc-gen-go`

pushd ${BACK_PROTO_DIR}

mkdir -p backuppb

${protoc} --proto_path=. --go_out=plugins=grpc,paths=source_relative:./backuppb backup.proto

popd

MILVUS_PROTO_DIR=${PROGRAM}/internal/proto/

pushd ${MILVUS_PROTO_DIR}

mkdir -p commonpb
mkdir -p schemapb
mkdir -p etcdpb
mkdir -p indexcgopb

mkdir -p internalpb
mkdir -p milvuspb
mkdir -p rootcoordpb

mkdir -p segcorepb
mkdir -p proxypb

mkdir -p indexpb
mkdir -p datapb
mkdir -p querypb
mkdir -p planpb

mkdir -p querypbv2

${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./commonpb common.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./schemapb schema.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./etcdpb etcd_meta.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./indexcgopb index_cgo_msg.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./rootcoordpb root_coord.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./internalpb internal.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./milvuspb milvus.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./proxypb proxy.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./indexpb index_coord.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./datapb data_coord.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./querypb query_coord.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./planpb plan.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./segcorepb segcore.proto
${protoc} --proto_path="${GOOGLE_PROTO_DIR}" --proto_path=. --go_out=plugins=grpc,paths=source_relative:./querypbv2 query_coordv2.proto

popd
