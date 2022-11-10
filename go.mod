module github.com/zilliztech/milvus-backup

go 1.16

require (
	github.com/gin-gonic/gin v1.7.7
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	//github.com/milvus-io/milvus-proto/go-api v0.0.0-20221019080323-84e9fa2f9e45
	github.com/milvus-io/milvus-sdk-go/v2 v2.1.0
	github.com/minio/minio-go/v7 v7.0.17
	github.com/pkg/errors v0.9.1
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.5.0
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.8.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.etcd.io/etcd/client/v3 v3.5.0
	go.uber.org/zap v1.17.0
	google.golang.org/grpc v1.48.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/kr/text v0.2.0 // indirect
	github.com/lingdor/stackerror v0.0.0-20191119040541-976d8885ed76
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6
	golang.org/x/oauth2 v0.0.0-20210402161424-2e8d93401602
)

replace (
	github.com/apache/pulsar-client-go => github.com/milvus-io/pulsar-client-go v0.6.8
	github.com/milvus-io/milvus-sdk-go/v2 => github.com/wayblink/milvus-sdk-go/v2 v2.2.2
	github.com/streamnative/pulsarctl => github.com/xiaofan-luan/pulsarctl v0.5.1
)
