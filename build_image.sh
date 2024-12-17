
VERSION=$(git describe --tags --always)
COMMIT=$(git rev-parse --short HEAD)
DATE=$(date +'%Y-%m-%dT%H:%M:%SZ')

docker build -t milvusdb/milvus-backup:$VERSION --build-arg VERSION=$VERSION --build-arg COMMIT=$COMMIT --build-arg DATE=$DATE .
docker tag milvusdb/milvus-backup:$VERSION milvusdb/milvus-backup:latest