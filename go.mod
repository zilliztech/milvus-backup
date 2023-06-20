module github.com/zilliztech/milvus-backup

go 1.16

require (
	github.com/blang/semver/v4 v4.0.0
	github.com/gin-gonic/gin v1.8.1
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/milvus-io/milvus-sdk-go/v2 v2.2.0
	github.com/minio/minio-go/v7 v7.0.17
	github.com/pkg/errors v0.9.1
	github.com/sony/sonyflake v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.5.0
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.8.1
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.etcd.io/etcd/client/v3 v3.5.0
	go.uber.org/zap v1.17.0
	google.golang.org/grpc v1.53.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/BurntSushi/toml v1.2.1 // indirect
	github.com/aliyun/credentials-go v1.3.0
	github.com/cockroachdb/errors v1.10.0
	github.com/go-openapi/spec v0.20.7 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/google/uuid v1.3.0
	github.com/lingdor/stackerror v0.0.0-20191119040541-976d8885ed76
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/swaggo/files v0.0.0-20220728132757-551d4a08d97a
	github.com/swaggo/gin-swagger v1.5.3
	github.com/swaggo/swag v1.8.8
	golang.org/x/crypto v0.3.0 // indirect
	golang.org/x/exp v0.0.0-20220827204233-334a2380cb91
	golang.org/x/oauth2 v0.5.0
	golang.org/x/sync v0.1.0
	golang.org/x/time v0.1.0
)

replace github.com/milvus-io/milvus-sdk-go/v2 => github.com/wayblink/milvus-sdk-go/v2 v2.2.16
