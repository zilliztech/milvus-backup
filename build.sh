go get
./scripts/gen_swag.sh
./scripts/gen_proto.sh
go build -ldflags "-X main.commit=$(git rev-parse --short HEAD) -X main.date=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" -o milvus-backup
