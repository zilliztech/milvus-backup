// Package migrate translates a resolved v1 configuration into a v2
// configuration. Two entry points share one field mapping: Migrate, which also
// returns a report of everything that needs a human's attention and backs the
// `config migrate` command, and Translate, which the config loader uses to run
// an existing v1 file through the v2 schema.
//
// Migration renames configuration keys and environment variables; it does not
// relocate values between mechanisms. A value the operator keeps in an
// environment variable stays there: the v2 file records the v2 variable name to
// set rather than baking the secret into the file. Translate is the exception,
// since a running process needs the secret itself rather than advice about it.
package migrate

import (
	"fmt"
	"os"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg"
	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// Migrate maps a resolved v1 configuration into a complete v2 configuration and
// a report. The returned config always renders; the report carries the
// warnings, the embedded comments, the environment renames, and any validation
// problems (via Report.Err) that --strict promotes to a failure.
func Migrate(src *cfg.Config) (*v2.Config, *Report) {
	r := newReport()
	out := translate(src, r)

	scanLegacyEnv(src, r)
	r.recordValidation(out.Validate())

	return out, r
}

// Translate maps a resolved v1 configuration into a complete v2 configuration
// for the config loader, which decodes a v1 file with the v1 schema and then
// translates it, rather than teaching the v2 schema to read v1 names.
//
// It differs from Migrate in the two ways a running process needs: every value
// is carried into the result, including a secret that reached v1 through an
// environment variable, which the loader needs in hand to connect; and the v2
// validation problems are returned as an error instead of being collected for
// a human to read.
func Translate(src *cfg.Config) (*v2.Config, error) {
	out := translate(src, nil)

	if err := out.Validate(); err != nil {
		return nil, fmt.Errorf("cfg: translate v1 config to v2: %w", err)
	}

	return out, nil
}

// translate applies the v1 to v2 field mapping shared by Migrate and Translate.
// A nil report selects the loader path: nothing is reported, and secrets are
// copied into the config rather than deferred to an environment variable.
func translate(src *cfg.Config, r *Report) *v2.Config {
	out := v2.New()
	// Start from a fully defaulted baseline so the fields with no v1 source
	// report as defaulted, which the v2 validator relies on to tell an
	// inapplicable credential from one left unset.
	param.Defaults(out)

	migrateLog(src, out)
	migrateServer(src, out, r)
	migrateMilvus(src, out, r)
	migrateStorage(&src.Minio, out, r)
	migrateConcurrency(src, out)
	migrateTransfer(src, out, r)
	migrateCloud(src, out, r)

	return out
}

func migrateLog(src *cfg.Config, out *v2.Config) {
	out.Log.Level.Val = src.Log.Level.Val
	out.Log.Console.Val = src.Log.Console.Val
	out.Log.File.Path.Val = src.Log.File.Filename.Val
	out.Log.File.MaxSizeMiB.Val = src.Log.File.MaxSize.Val
	out.Log.File.MaxDays.Val = src.Log.File.MaxDays.Val
	out.Log.File.MaxBackups.Val = src.Log.File.MaxBackups.Val
}

func migrateServer(src *cfg.Config, out *v2.Config, r *Report) {
	out.Server.DebugMode.Val = src.HTTP.DebugMode.Val
	out.Server.SwaggerBasePath.Val = src.HTTP.SwaggerBasePath.Val

	// http.enabled has no v2 replacement: the API server is always available.
	if !src.HTTP.Enabled.Val {
		r.warnf("http.enabled=false was dropped in v2; the API server is always enabled")
	}
}

func migrateMilvus(src *cfg.Config, out *v2.Config, r *Report) {
	m := &src.Milvus

	out.Milvus.User.Val = m.User.Val
	mapSecret(&out.Milvus.Password, &m.Password, "MILVUS_PASSWORD", r)

	out.Milvus.Grpc.Address.Val = m.Address.Val
	out.Milvus.Grpc.Port.Val = m.Port.Val
	out.Milvus.Grpc.TLSMode.Val = tlsMode(m, r)
	out.Milvus.Grpc.CACertPath.Val = m.CACertPath.Val
	out.Milvus.Grpc.ServerName.Val = m.ServerName.Val
	out.Milvus.Grpc.MTLSCertPath.Val = m.MTLSCertPath.Val
	out.Milvus.Grpc.MTLSKeyPath.Val = m.MTLSKeyPath.Val

	// There is no v1 REST endpoint: v1 derived it from the gRPC connection.
	out.Milvus.Rest.Endpoint.Val = out.Milvus.Rest.Endpoint.Default

	out.Milvus.Management.Endpoint.Val = src.Backup.GCPause.Address.Val
	out.Milvus.Replicate.RPCChannelName.Val = m.RPCChannelName.Val

	out.Milvus.Etcd.Endpoints.Val = splitList(m.Etcd.Endpoints.Val)
	out.Milvus.Etcd.RootPath.Val = m.Etcd.RootPath.Val
}

// tlsMode maps the v1 integer TLS mode to its v2 name. v1 silently downgraded
// mutual TLS to server TLS when no client key pair was configured; v2 rejects
// it, so the downgrade is settled here to keep the output loadable.
func tlsMode(m *cfg.MilvusConfig, r *Report) string {
	switch m.TLSMode.Val {
	case 0:
		return v2.TLSDisabled
	case 1:
		return v2.TLSServer
	case 2:
		if m.MTLSCertPath.Val == "" || m.MTLSKeyPath.Val == "" {
			r.warnf("milvus.tlsMode=2 (mutual) but no client certificate/key is set; v1 used server TLS, migrated as %q", v2.TLSServer)
			r.commentKey("milvus.grpc.tlsMode", "v1 tlsMode 2 without an mTLS key pair; downgraded to server (v1 behavior)")
			return v2.TLSServer
		}
		return v2.TLSMutual
	default:
		r.warnf("milvus.tlsMode=%d is not a valid v1 TLS mode; migrated as %q", m.TLSMode.Val, v2.TLSDisabled)
		return v2.TLSDisabled
	}
}

// storageV1 gathers one v1 storage side (the Milvus deployment storage or the
// backup destination) into the shape migrateStorageSide maps from. The
// credential fields are pointers so their source (file, env, default) can be
// inspected.
type storageV1 struct {
	provider string
	address  string
	port     int
	region   string
	useSSL   bool
	bucket   string
	rootPath string

	accessKeyID       string
	secretAccessKey   *cfg.Value[string]
	sessionToken      *cfg.Value[string]
	gcpCredentialJSON *cfg.Value[string]
	useIAM            bool
	iamEndpoint       string
}

func migrateStorage(m *cfg.MinioConfig, out *v2.Config, r *Report) {
	migrateStorageSide(&out.Milvus.Storage, storageV1{
		provider:          m.StorageType.Val,
		address:           m.Address.Val,
		port:              m.Port.Val,
		region:            m.Region.Val,
		useSSL:            m.UseSSL.Val,
		bucket:            m.BucketName.Val,
		rootPath:          m.RootPath.Val,
		accessKeyID:       m.AccessKeyID.Val,
		secretAccessKey:   &m.SecretAccessKey,
		sessionToken:      &m.Token,
		gcpCredentialJSON: &m.GcpCredentialJSON,
		useIAM:            m.UseIAM.Val,
		iamEndpoint:       m.IAMEndpoint.Val,
	}, "milvus", r)

	migrateStorageSide(&out.Backup.Storage, storageV1{
		provider:    m.BackupStorageType.Val,
		address:     m.BackupAddress.Val,
		port:        m.BackupPort.Val,
		region:      m.BackupRegion.Val,
		useSSL:      m.BackupUseSSL.Val,
		bucket:      m.BackupBucketName.Val,
		rootPath:    m.BackupRootPath.Val,
		accessKeyID: m.BackupAccessKeyID.Val,
		// An unset backup secret inherits the primary storage secret in v1, so
		// its true origin — including "came from an environment variable" — is
		// the primary's. gcpCredentialJSON is not inherited, so it stays direct.
		secretAccessKey:   effective(&m.BackupSecretAccessKey, &m.SecretAccessKey),
		sessionToken:      effective(&m.BackupToken, &m.Token),
		gcpCredentialJSON: &m.BackupGcpCredentialJSON,
		useIAM:            m.BackupUseIAM.Val,
		iamEndpoint:       m.BackupIAMEndpoint.Val,
	}, "backup", r)

	// v1 let an unset backup root path inherit a customized Milvus storage root
	// path, putting backup data under the Milvus root. v2 keeps them
	// independent; the migration preserves the v1 location by writing it out
	// explicitly, and flags the case an operator is most likely to be surprised
	// by: a custom milvus root path silently adopted for backups.
	if m.BackupRootPath.Used.Kind == param.SourceDefault && m.RootPath.Used.Kind != param.SourceDefault {
		r.warnf("backup root path %q was inherited from the milvus storage root path in v1; v2 keeps them independent and now sets it explicitly", m.BackupRootPath.Val)
	}
}

func migrateStorageSide(out *v2.StorageConfig, in storageV1, side string, r *Report) {
	provider := canonicalProvider(in.provider)
	out.Provider.Val = provider
	out.Address.Val = in.address
	out.Port.Val = in.port
	out.Region.Val = in.region
	out.UseSSL.Val = in.useSSL
	out.BucketName.Val = in.bucket
	out.RootPath.Val = in.rootPath

	switch {
	case provider == v2.ProviderLocal:
		// Local storage is a directory: no endpoint, no credentials.
	case provider == v2.ProviderAzure:
		// v1 overloaded the access key ID as the Azure account name.
		out.AccountName.Val = in.accessKeyID
		out.Auth.Type.Val = v2.AuthSharedKey
		mapSecret(&out.Auth.AccountKey, in.secretAccessKey, storageEnv(side, "AUTH_ACCOUNT_KEY"), r)
	case provider == v2.ProviderGCPNative:
		out.Auth.Type.Val = v2.AuthServiceAccount
		mapSecret(&out.Auth.CredentialsFile, in.gcpCredentialJSON, storageEnv(side, "AUTH_CREDENTIALS_FILE"), r)
		r.warnf("%s.auth.credentialsFile expects a path to the service account JSON file; verify it is not inline JSON", storagePrefix(side))
	case in.useIAM:
		out.Auth.Type.Val = v2.AuthIAM
		out.Auth.Endpoint.Val = in.iamEndpoint
	default:
		out.Auth.Type.Val = v2.AuthStatic
		out.Auth.AccessKeyID.Val = in.accessKeyID
		mapSecret(&out.Auth.SecretAccessKey, in.secretAccessKey, storageEnv(side, "AUTH_SECRET_ACCESS_KEY"), r)
		mapSecret(&out.Auth.SessionToken, in.sessionToken, storageEnv(side, "AUTH_SESSION_TOKEN"), r)
	}
}

func migrateConcurrency(src *cfg.Config, out *v2.Config) {
	p := &src.Backup.Parallelism
	out.Backup.Concurrency.Collections.Val = p.BackupCollection.Val
	out.Backup.Concurrency.Segments.Val = p.BackupSegment.Val
	out.Backup.PauseGC.Val = src.Backup.GCPause.Enable.Val

	out.Restore.Concurrency.Collections.Val = p.RestoreCollection.Val
	out.Restore.Concurrency.ImportJobs.Val = p.ImportJob.Val
	out.Restore.KeepTempFiles.Val = src.Backup.KeepTempFiles.Val
}

func migrateTransfer(src *cfg.Config, out *v2.Config, r *Report) {
	if src.Minio.CrossStorage.Val {
		out.Transfer.Mode.Val = v2.TransferStreaming
	} else {
		out.Transfer.Mode.Val = v2.TransferAuto
		r.commentKey("transfer.mode", "v1 minio.crossStorage=false mapped to auto")
		// auto only diverges from v1 when the two backends differ: for the same
		// backend both do a storage-side copy, so warning then would be noise.
		if !sameBackend(&out.Milvus.Storage, &out.Backup.Storage) {
			r.warnf("minio.crossStorage=false mapped to transfer.mode=auto, but milvus and backup storage differ; auto streams between them where v1 could attempt direct copy — verify this is intended")
		}
	}

	out.Transfer.Concurrency.Val = src.Backup.Parallelism.CopyData.Val
	out.Transfer.MultipartCopyThresholdMiB.Val = src.Minio.MultipartCopyThresholdMiB.Val
}

// sameBackend reports whether two storage configs name the same backend, i.e.
// the same provider reached at the same endpoint. Buckets and root paths may
// differ within one backend.
func sameBackend(a, b *v2.StorageConfig) bool {
	return a.Provider.Val == b.Provider.Val &&
		a.Address.Val == b.Address.Val &&
		a.Port.Val == b.Port.Val &&
		a.Region.Val == b.Region.Val &&
		a.UseSSL.Val == b.UseSSL.Val &&
		a.AccountName.Val == b.AccountName.Val
}

func migrateCloud(src *cfg.Config, out *v2.Config, r *Report) {
	out.Cloud.Endpoint.Val = src.Cloud.Address.Val
	mapSecret(&out.Cloud.APIKey, &src.Cloud.APIKey, "ZILLIZ_CLOUD_API_KEY", r)
}

// effective returns the field whose source decides how a v1 backup secret is
// migrated. An unset backup secret inherits the primary storage secret in v1,
// so a secret that reached the backup side only by inheriting an env-supplied
// primary is still treated as env-supplied, not written into the file.
func effective(field, primary *cfg.Value[string]) *cfg.Value[string] {
	if field.Used.Kind == param.SourceDefault {
		return primary
	}

	return field
}

// mapSecret copies a v1 secret into a v2 field, unless the v1 value came from an
// environment variable: then it leaves the v2 field at its default and records
// that the secret must be supplied through the v2 variable instead of being
// written into the file.
//
// That withholding only makes sense for a file someone is about to read. On the
// loader path (a nil report) the secret is always copied, because a running
// process needs the resolved value to connect.
func mapSecret(out, in *cfg.Value[string], v2env string, r *Report) {
	if r != nil && in.Used.Kind == param.SourceEnv {
		r.comment(out, "set via env "+v2env)
		r.deferToEnv(out)
		r.warnf("%s is supplied via environment variable %s in v1; set %s in your v2 deployment (its value was not written to the file)",
			configKey(out), strings.ToUpper(in.Used.Key), v2env)
		return
	}
	out.Val = in.Val
}

// scanLegacyEnv reports the v1 environment variables that are set in the current
// environment and were renamed in v2, so a deployment carrying them over does
// not silently lose the value (the v2 loader does not read v1 variables).
func scanLegacyEnv(src *cfg.Config, r *Report) {
	seen := make(map[string]bool)
	param.Walk(src, func(_ string, f param.Field) {
		for _, env := range f.EnvNames() {
			if seen[env] {
				continue
			}
			seen[env] = true
			if _, ok := os.LookupEnv(env); !ok {
				continue
			}
			if to, legacy := v2.Migration(env); legacy {
				r.EnvRenames = append(r.EnvRenames, EnvRename{From: env, To: to})
			}
		}
	})
}

// canonicalProvider folds the v1 provider spelling aliases into the single v2
// name for each provider.
func canonicalProvider(p string) string {
	switch strings.ToLower(p) {
	case cfg.CloudProviderAli, cfg.CloudProviderAlibaba, cfg.CloudProviderAliCloud, cfg.CloudProviderAliyun:
		return v2.ProviderAliyun
	case cfg.CloudProviderTencentShort, cfg.CloudProviderTencent:
		return v2.ProviderTencent
	default:
		return strings.ToLower(p)
	}
}

// splitList turns the v1 comma-separated endpoint string into the v2 list form,
// dropping blanks so "a, b," yields [a b].
func splitList(s string) []string {
	items := make([]string, 0, strings.Count(s, ",")+1)
	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}

	return items
}

// storagePrefix is the v2 config key prefix of one storage side.
func storagePrefix(side string) string {
	if side == "backup" {
		return "backup.storage"
	}

	return "milvus.storage"
}

// storageEnv builds the v2 environment variable name for a storage credential,
// e.g. MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY or BACKUP_STORAGE_AUTH_ACCOUNT_KEY.
func storageEnv(side, suffix string) string {
	if side == "backup" {
		return "BACKUP_STORAGE_" + suffix
	}

	return "MILVUS_STORAGE_" + suffix
}

func configKey(f param.Field) string {
	if keys := f.ConfigKeys(); len(keys) > 0 {
		return keys[0]
	}

	return ""
}
