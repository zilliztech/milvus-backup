#!/usr/bin/env bash

PROGRAM=${PWD}
BACK_PROTO_DIR=$PROGRAM/core/proto/

echo ${PROGRAM}
if ! command -v protoc 1>/dev/null; then
    echo "Error: protoc is required but not installed. Install it with:" > /dev/stderr
    echo "  macOS:         brew install protobuf" > /dev/stderr
    echo "  Debian/Ubuntu: apt install protobuf-compiler" > /dev/stderr
    exit 1
fi
protoc --version

# The committed backup.pb.go is v1.3-style, and the artifact JSON contract
# was born with that shape, so the generator is pinned: whatever
# protoc-gen-go happens to sit in GOPATH/bin would silently rewrite the whole
# file in the modern style, and every generator since v1.4 emits that style,
# so v1.3.2 is the last one that reproduces the file. Install it into a
# scratch bin and put only that on PATH.
TMP_BIN=$(mktemp -d)
GOBIN=${TMP_BIN} go install github.com/golang/protobuf/protoc-gen-go@v1.3.2 || exit 1
export PATH=${TMP_BIN}:$PATH

pushd ${BACK_PROTO_DIR}

mkdir -p backuppb

protoc --proto_path=. --go_out=paths=source_relative:./backuppb backup.proto

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
