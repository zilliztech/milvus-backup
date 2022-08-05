module github.com/zilliztech/milvus-backup

go 1.16

require (
	github.com/gin-gonic/gin v1.7.7
	github.com/go-basic/ipv4 v1.0.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/milvus-io/milvus-sdk-go/v2 v2.1.0
	github.com/pkg/errors v0.9.1
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/spf13/cast v1.3.1
	github.com/spf13/viper v1.8.1
	github.com/streamnative/pulsarctl v0.5.0
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.etcd.io/etcd/client/v3 v3.5.0
	go.etcd.io/etcd/server/v3 v3.5.0
	go.uber.org/zap v1.17.0
	google.golang.org/grpc v1.46.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace (
	github.com/apache/pulsar-client-go => github.com/milvus-io/pulsar-client-go v0.6.8
	github.com/milvus-io/milvus-sdk-go/v2 => github.com/wayblink/milvus-sdk-go/v2 v2.1.1
	github.com/streamnative/pulsarctl => github.com/xiaofan-luan/pulsarctl v0.5.1
)
