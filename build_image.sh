
VERSION=$(git describe --tags --always)
COMMIT=$(git rev-parse --short HEAD)
DATA=$(date +'%Y-%m-%dT%H:%M:%S')

docker build -t milvusdb/milvus-backup:$VERSION --build-arg VERSION=$VERSION --build-arg COMMIT=$COMMIT --build-arg DATA=$DATA .
docker tag milvusdb/milvus-backup:$VERSION milvusdb/milvus-backup:latest