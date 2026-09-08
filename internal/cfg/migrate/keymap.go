package migrate

// This file holds the machine-readable form of the v1 key space: one entry per
// logical v1 parameter, with its config keys in schema declaration order (the
// first hit wins, matching v1 resolution) and the environment variables that
// set it (same rule). It is the single source of truth the translator walks,
// and the list the legacy-environment scan enumerates.
//
// v2/legacy.go keeps a parallel table of the same renames written as prose
// for the "you used a v1 name in a v2 file" warnings; the two serve different
// readers and are both derived from the retired v1 schema.

// v1Field describes one logical v1 parameter and how its value lands in the
// v2 key space.
type v1Field struct {
	// keys are the v1 config file keys, lower-cased, in the order the v1
	// schema declared them; the first one present wins.
	keys []string
	// env is the v1 environment variables that set the parameter, in the
	// order the v1 schema declared them.
	env []string

	// v2 is the target v2 config key for a plain rename. Fields whose mapping
	// depends on more than their own value leave it empty and are handled by
	// the translator's custom passes.
	v2 string

	// secret marks a credential. On the migrate path an env-sourced credential
	// is withheld from the written file and deferred to v2env instead; on the
	// loader path it is carried like any other env value.
	secret bool
	v2env  string
}

// allKeys returns every spelling that names the field in the v1 override
// namespace: v1 --set lookups tried config keys and env names alike.
func (f *v1Field) allKeys() []string {
	keys := make([]string, 0, len(f.keys)+len(f.env))
	keys = append(keys, f.keys...)
	keys = append(keys, f.env...)

	return keys
}

// Storage side fields: the v1 minio section describes two backends, the Milvus
// deployment storage and the backup destination, as one flat set of keys.
var (
	fMilvusProvider = &v1Field{keys: []string{"storage.storagetype", "minio.storagetype", "minio.cloudprovider"}}
	fBackupProvider = &v1Field{keys: []string{"storage.backupstoragetype", "minio.backupstoragetype"}}

	milvusStorage = &storageSide{
		prefix:   "milvus.storage",
		provider: fMilvusProvider,
		address:  &v1Field{keys: []string{"minio.address"}, env: []string{"MINIO_ADDRESS"}, v2: "milvus.storage.address"},
		port:     &v1Field{keys: []string{"minio.port"}, env: []string{"MINIO_PORT"}, v2: "milvus.storage.port"},
		region:   &v1Field{keys: []string{"minio.region"}, env: []string{"MINIO_REGION"}, v2: "milvus.storage.region"},
		useSSL:   &v1Field{keys: []string{"minio.usessl"}, env: []string{"MINIO_USE_SSL"}, v2: "milvus.storage.useSSL"},
		bucket:   &v1Field{keys: []string{"minio.bucketname"}, env: []string{"MINIO_BUCKET_NAME"}, v2: "milvus.storage.bucketName"},
		rootPath: &v1Field{keys: []string{"minio.rootpath"}, env: []string{"MINIO_ROOT_PATH"}, v2: "milvus.storage.rootPath"},

		accessKeyID:       &v1Field{keys: []string{"minio.accesskeyid"}, env: []string{"MINIO_ACCESS_KEY"}},
		secretAccessKey:   &v1Field{keys: []string{"minio.secretaccesskey"}, env: []string{"MINIO_SECRET_KEY"}, secret: true, v2env: "MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY"},
		token:             &v1Field{keys: []string{"minio.token"}, env: []string{"MINIO_TOKEN"}, secret: true, v2env: "MILVUS_STORAGE_AUTH_SESSION_TOKEN"},
		gcpCredentialJSON: &v1Field{keys: []string{"minio.gcpcredentialjson"}, env: []string{"GCP_KEY_JSON"}, secret: true, v2env: "MILVUS_STORAGE_AUTH_CREDENTIALS_FILE"},
		useIAM:            &v1Field{keys: []string{"minio.useiam"}, env: []string{"MINIO_USE_IAM"}},
		iamEndpoint:       &v1Field{keys: []string{"minio.iamendpoint"}, env: []string{"MINIO_IAM_ENDPOINT"}},
	}

	backupStorage = &storageSide{
		prefix:   "backup.storage",
		provider: fBackupProvider,
		address:  &v1Field{keys: []string{"minio.backupaddress"}, env: []string{"MINIO_BACKUP_ADDRESS"}, v2: "backup.storage.address"},
		port:     &v1Field{keys: []string{"minio.backupport"}, env: []string{"MINIO_BACKUP_PORT"}, v2: "backup.storage.port"},
		region:   &v1Field{keys: []string{"minio.backupregion"}, env: []string{"MINIO_BACKUP_REGION"}, v2: "backup.storage.region"},
		useSSL:   &v1Field{keys: []string{"minio.backupusessl"}, env: []string{"MINIO_BACKUP_USE_SSL"}, v2: "backup.storage.useSSL"},
		bucket:   &v1Field{keys: []string{"minio.backupbucketname"}, env: []string{"MINIO_BACKUP_BUCKET_NAME"}, v2: "backup.storage.bucketName"},
		rootPath: &v1Field{keys: []string{"minio.backuprootpath"}, env: []string{"MINIO_BACKUP_ROOT_PATH"}, v2: "backup.storage.rootPath"},

		accessKeyID:       &v1Field{keys: []string{"minio.backupaccesskeyid"}, env: []string{"MINIO_BACKUP_ACCESS_KEY"}},
		secretAccessKey:   &v1Field{keys: []string{"minio.backupsecretaccesskey"}, env: []string{"MINIO_BACKUP_SECRET_KEY"}, secret: true, v2env: "BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY"},
		token:             &v1Field{keys: []string{"minio.backuptoken"}, env: []string{"MINIO_BACKUP_TOKEN"}, secret: true, v2env: "BACKUP_STORAGE_AUTH_SESSION_TOKEN"},
		gcpCredentialJSON: &v1Field{keys: []string{"minio.backupgcpcredentialjson"}, env: []string{"BACKUP_GCP_KEY_JSON"}, secret: true, v2env: "BACKUP_STORAGE_AUTH_CREDENTIALS_FILE"},
		useIAM:            &v1Field{keys: []string{"minio.backupuseiam"}, env: []string{"MINIO_BACKUP_USE_IAM"}},
		iamEndpoint:       &v1Field{keys: []string{"minio.backupiamendpoint"}, env: []string{"MINIO_BACKUP_IAM_ENDPOINT"}},
	}
)

// storageSide groups the v1 fields describing one storage backend. prefix is
// the v2 config key prefix of the side.
type storageSide struct {
	prefix   string
	provider *v1Field

	address  *v1Field
	port     *v1Field
	region   *v1Field
	useSSL   *v1Field
	bucket   *v1Field
	rootPath *v1Field

	accessKeyID       *v1Field
	secretAccessKey   *v1Field
	token             *v1Field
	gcpCredentialJSON *v1Field
	useIAM            *v1Field
	iamEndpoint       *v1Field
}

// plainFields returns the side's fields that rename directly onto a v2 key.
func (s *storageSide) plainFields() []*v1Field {
	return []*v1Field{s.address, s.port, s.region, s.useSSL, s.bucket, s.rootPath}
}

// Fields with a custom mapping, handled by dedicated translator passes.
var (
	fHTTPEnabled    = &v1Field{keys: []string{"http.enabled"}}
	fTLSMode        = &v1Field{keys: []string{"milvus.tlsmode"}, env: []string{"MILVUS_TLS_MODE"}}
	fMTLSCertPath   = &v1Field{keys: []string{"milvus.mtlscertpath"}, env: []string{"MILVUS_MTLS_CERT_PATH"}, v2: "milvus.grpc.mtlsCertPath"}
	fMTLSKeyPath    = &v1Field{keys: []string{"milvus.mtlskeypath"}, env: []string{"MILVUS_MTLS_KEY_PATH"}, v2: "milvus.grpc.mtlsKeyPath"}
	fEtcdEndpoints  = &v1Field{keys: []string{"milvus.etcd.endpoints"}}
	fCrossStorage   = &v1Field{keys: []string{"minio.crossstorage"}}
	fMultipartThres = &v1Field{keys: []string{"minio.multipartcopythresholdmib"}, v2: "transfer.multipartCopyThresholdMiB"}
)

// v1Fields lists every logical v1 parameter in schema order. Plain renames
// carry their v2 target; the rest are translated by the translator's custom
// passes. The legacy-environment scan walks this list.
var v1Fields = []*v1Field{
	{keys: []string{"log.level"}, v2: "log.level"},
	{keys: []string{"log.console"}, v2: "log.console"},
	{keys: []string{"log.file.filename"}, v2: "log.file.path"},
	{keys: []string{"log.file.maxsize"}, v2: "log.file.maxSizeMiB"},
	{keys: []string{"log.file.maxdays"}, v2: "log.file.maxDays"},
	{keys: []string{"log.file.maxbackups"}, v2: "log.file.maxBackups"},

	fHTTPEnabled,
	{keys: []string{"http.debugmode"}, v2: "server.debugMode"},
	{keys: []string{"http.swaggerbasepath"}, v2: "server.swaggerBasePath"},

	{keys: []string{"milvus.address"}, env: []string{"MILVUS_ADDRESS"}, v2: "milvus.grpc.address"},
	{keys: []string{"milvus.port"}, env: []string{"MILVUS_PORT"}, v2: "milvus.grpc.port"},
	{keys: []string{"milvus.user"}, env: []string{"MILVUS_USER"}, v2: "milvus.user"},
	{keys: []string{"milvus.password"}, env: []string{"MILVUS_PASSWORD"}, v2: "milvus.password", secret: true, v2env: "MILVUS_PASSWORD"},
	fTLSMode,
	{keys: []string{"milvus.cacertpath"}, env: []string{"MILVUS_CA_CERT_PATH"}, v2: "milvus.grpc.caCertPath"},
	{keys: []string{"milvus.servername"}, env: []string{"MILVUS_SERVER_NAME"}, v2: "milvus.grpc.serverName"},
	fMTLSCertPath,
	fMTLSKeyPath,
	{keys: []string{"milvus.rpcchannelname"}, env: []string{"MILVUS_RPC_CHANNEL_NAME"}, v2: "milvus.replicate.rpcChannelName"},
	fEtcdEndpoints,
	{keys: []string{"milvus.etcd.rootpath"}, v2: "milvus.etcd.rootPath"},

	{keys: []string{"cloud.address"}, v2: "zillizCloud.endpoint"},
	{keys: []string{"cloud.apikey"}, v2: "zillizCloud.apiKey", secret: true},

	fMilvusProvider,
	milvusStorage.address, milvusStorage.port, milvusStorage.region,
	milvusStorage.useSSL, milvusStorage.bucket, milvusStorage.rootPath,
	milvusStorage.accessKeyID, milvusStorage.secretAccessKey, milvusStorage.token,
	milvusStorage.gcpCredentialJSON, milvusStorage.useIAM, milvusStorage.iamEndpoint,

	fBackupProvider,
	backupStorage.address, backupStorage.port, backupStorage.region,
	backupStorage.useSSL, backupStorage.bucket, backupStorage.rootPath,
	backupStorage.accessKeyID, backupStorage.secretAccessKey, backupStorage.token,
	backupStorage.gcpCredentialJSON, backupStorage.useIAM, backupStorage.iamEndpoint,

	fCrossStorage,
	fMultipartThres,

	{keys: []string{"backup.parallelism.copydata"}, env: []string{"BACKUP_PARALLELISM_COPYDATA"}, v2: "transfer.concurrency"},
	{keys: []string{"backup.parallelism.backupcollection"}, env: []string{"BACKUP_PARALLELISM_BACKUP_COLLECTION"}, v2: "backup.concurrency.collections"},
	{keys: []string{"backup.parallelism.backupsegment"}, v2: "backup.concurrency.segments"},
	{keys: []string{"backup.parallelism.restorecollection"}, env: []string{"BACKUP_PARALLELISM_RESTORE_COLLECTION"}, v2: "restore.concurrency.collections"},
	{keys: []string{"backup.parallelism.importjob"}, v2: "restore.concurrency.importJobs"},
	{keys: []string{"backup.keeptempfiles"}, env: []string{"BACKUP_KEEP_TEMP_FILES"}, v2: "restore.keepTempFiles"},
	{keys: []string{"backup.gcpause.enable"}, env: []string{"BACKUP_GC_PAUSE_ENABLE"}, v2: "backup.pauseGC"},
	{keys: []string{"backup.gcpause.address"}, env: []string{"BACKUP_GC_PAUSE_ADDRESS"}, v2: "milvus.management.endpoint"},
}
