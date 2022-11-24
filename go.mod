module github.com/zilliztech/milvus-backup

go 1.16

require (
	github.com/gin-gonic/gin v1.8.1
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/milvus-io/milvus-sdk-go/v2 v2.1.0
	github.com/minio/minio-go/v7 v7.0.17
	github.com/pkg/errors v0.9.1
	github.com/sony/sonyflake v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.5.0
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.8.1
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.uber.org/zap v1.17.0
	google.golang.org/grpc v1.48.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/PuerkitoBio/purell v1.2.0 // indirect
	github.com/go-openapi/spec v0.20.7 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-playground/validator/v10 v10.11.1 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lingdor/stackerror v0.0.0-20191119040541-976d8885ed76
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/swaggo/files v0.0.0-20220728132757-551d4a08d97a // indirect
	github.com/swaggo/gin-swagger v1.5.3 // indirect
	github.com/swaggo/swag v1.8.8 // indirect
	github.com/urfave/cli/v2 v2.23.5 // indirect
	golang.org/x/crypto v0.3.0 // indirect
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6
	golang.org/x/oauth2 v0.0.0-20210402161424-2e8d93401602
	golang.org/x/tools v0.3.0 // indirect
)

replace (
	github.com/apache/pulsar-client-go => github.com/milvus-io/pulsar-client-go v0.6.8
	github.com/milvus-io/milvus-sdk-go/v2 => github.com/wayblink/milvus-sdk-go/v2 v2.2.4
	github.com/streamnative/pulsarctl => github.com/xiaofan-luan/pulsarctl v0.5.1
)
