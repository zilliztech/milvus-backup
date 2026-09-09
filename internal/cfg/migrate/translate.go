package migrate

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
	"github.com/zilliztech/milvus-backup/internal/log"
)

// A translator carries a v1 source across the schema boundary, producing the
// v2 source that v2.LoadFrom resolves. The version boundary moves with it:
// instead of resolving the v1 schema and copying the result field by field,
// the flattened v1 key map is translated into a flattened v2 key map, and the
// v2 schema does all resolution.
//
// The translator runs in one of two modes, selected by the report: with a
// report (the `config migrate` command) env-sourced secrets are withheld from
// the written file and deferred to the v2 environment variable, while every
// other env value is baked into the translated file. Without one (the config
// loader) every env value is carried too, since a running process needs the
// resolved credentials to connect.
type translator struct {
	src    *param.Source
	report *Report // nil on the loader path

	file     map[string]param.Input
	override map[string]string

	// consumed records the v1 keys the translation recognized, so the
	// leftovers can be warned about (file) or passed through (overrides).
	consumedFile     map[string]bool
	consumedOverride map[string]bool

	// crossStorageAuto records that the v1 crossStorage flag translated to
	// transfer.mode=auto, so the backend-divergence warning is not raised when
	// auto came from somewhere else, such as a v2-spelled --set.
	crossStorageAuto bool
}

func newTranslator(src *param.Source, report *Report) *translator {
	return &translator{
		src:              src,
		report:           report,
		file:             map[string]param.Input{},
		override:         map[string]string{},
		consumedFile:     map[string]bool{},
		consumedOverride: map[string]bool{},
	}
}

// layer identifies which of the v1 precedence layers a hit came from. It
// decides the provenance kind the translated entry carries, so a value that
// arrived through a v1 environment variable still says so after the rename.
type layer int

const (
	layerOverride layer = iota
	layerEnv
	layerFile
)

func (l layer) kind() param.SourceKind {
	switch l {
	case layerOverride:
		return param.SourceOverride
	case layerEnv:
		return param.SourceV1Env
	default:
		return param.SourceV1ConfigFile
	}
}

// hit is one resolved occurrence of a v1 field: the raw value, the layer it
// came from, and the key or variable name it was spelled with.
type hit struct {
	value any
	layer layer
	key   string
}

func (t *translator) translate() *param.Source {
	for _, f := range v1Fields {
		if f.v2 == "" {
			continue // handled by a custom pass below
		}
		if h := t.gather(f); h != nil {
			t.emit(f.v2, h, f.secret, f.v2env)
		}
	}

	t.translateHTTPEnabled()
	t.translateTLSMode()
	t.translateEtcdEndpoints()
	t.translateStorage()
	t.translateCrossStorage()
	t.injectBackupRootPath()
	t.declareV2()
	t.finishLeftovers()

	return param.NewTranslatedSource(t.src.ConfigFilePath(), t.file, t.override)
}

// gather finds the winning occurrence of f across the v1 precedence layers:
// override > env > config file, and within a layer the schema's declared key
// order. Every occurrence is marked consumed, not only the winner, so a key
// shadowed by a higher layer is not reported as unknown. It returns nil when
// nothing sets the field.
func (t *translator) gather(f *v1Field) *hit {
	var winner *hit

	for _, key := range f.allKeys() {
		if v, ok := t.src.OverrideValue(key); ok {
			t.consumedOverride[strings.ToLower(key)] = true
			if winner == nil {
				winner = &hit{value: v, layer: layerOverride, key: key}
			}
		}
	}
	for _, name := range f.env {
		if v, ok := t.src.EnvValue(name); ok && winner == nil {
			winner = &hit{value: v, layer: layerEnv, key: strings.ToLower(name)}
		}
	}
	for _, key := range f.keys {
		if v, ok := t.src.ConfigFileValue(key); ok {
			t.consumedFile[strings.ToLower(key)] = true
			if winner == nil {
				winner = &hit{value: v, layer: layerFile, key: key}
			}
		}
	}

	return winner
}

// emit writes one translated entry into the target key space. Every entry
// lands in the config file layer: the translation has already settled the
// precedence per v1 field, so the v2 resolution finds each value exactly
// where its target key says, and the entry carries the layer the value came
// from as its provenance kind.
//
// An env-layer hit on the migrate path is the exception: a secret is withheld
// from the written file and deferred to its v2 environment variable, since
// the file is something the operator keeps.
func (t *translator) emit(target string, h *hit, secret bool, v2env string) {
	if h.layer == layerEnv && t.report != nil && secret {
		t.deferEnv(target, v2env, h.key)
		return
	}

	t.file[strings.ToLower(target)] = param.Input{Value: h.value, Kind: h.layer.kind(), SourceKey: h.key}
}

// deferEnv records that a credential stayed in an environment variable: the
// written file keeps the field at its default with a comment naming the v2
// variable to set, and validation is told the emptiness is deliberate. The
// variable name is spelled the way the operator would export it, upper-cased,
// though it is carried lower-cased as a provenance key.
func (t *translator) deferEnv(target, v2env, v1name string) {
	t.report.commentKey(target, "set via env "+v2env)
	t.report.deferKey(target)
	t.report.warnf("%s is supplied via environment variable %s in v1; set %s in your v2 deployment (its value was not written to the file)",
		target, strings.ToUpper(v1name), v2env)
}

// warn routes a translation finding to the migration report when there is
// one, and to the log on the loader path, where nobody reads a report.
func (t *translator) warn(format string, args ...any) {
	if t.report != nil {
		t.report.warnf(format, args...)
		return
	}
	log.Warn(fmt.Sprintf(format, args...))
}

// translateHTTPEnabled drops http.enabled, which has no v2 replacement: the
// API server is always available.
func (t *translator) translateHTTPEnabled() {
	h := t.gather(fHTTPEnabled)
	if h != nil && !asBool(h.value) {
		t.warn("http.enabled=false was dropped in v2; the API server is always enabled")
	}
}

// translateTLSMode maps the v1 integer TLS mode to its v2 name. v1 silently
// downgraded mutual TLS to server TLS when no client key pair was configured;
// v2 rejects that combination, so the downgrade is settled here to keep the
// output loadable.
func (t *translator) translateTLSMode() {
	h := t.gather(fTLSMode)
	if h == nil {
		return
	}

	emit := func(mode string) {
		t.emit("milvus.grpc.tlsMode", &hit{value: mode, layer: h.layer, key: h.key}, false, "")
	}

	mode, err := asInt(h.value)
	if err != nil {
		t.warn("milvus.tlsMode=%v is not a valid v1 TLS mode; migrated as %q", h.value, v2.TLSDisabled)
		emit(v2.TLSDisabled)
		return
	}

	switch mode {
	case 0:
		emit(v2.TLSDisabled)
	case 1:
		emit(v2.TLSServer)
	case 2:
		if t.gatherNonEmpty(fMTLSCertPath) && t.gatherNonEmpty(fMTLSKeyPath) {
			emit(v2.TLSMutual)
			return
		}
		t.warn("milvus.tlsMode=2 (mutual) but no client certificate/key is set; v1 used server TLS, migrated as %q", v2.TLSServer)
		if t.report != nil {
			t.report.commentKey("milvus.grpc.tlsMode", "v1 tlsMode 2 without an mTLS key pair; downgraded to server (v1 behavior)")
		}
		emit(v2.TLSServer)
	default:
		t.warn("milvus.tlsMode=%d is not a valid v1 TLS mode; migrated as %q", mode, v2.TLSDisabled)
		emit(v2.TLSDisabled)
	}
}

// gatherNonEmpty reports whether f is set to a non-empty value anywhere. The
// v1 defaults for the mTLS paths are empty, so this matches the v1 check the
// TLS downgrade performed on resolved values.
func (t *translator) gatherNonEmpty(f *v1Field) bool {
	h := t.gather(f)

	return h != nil && asString(h.value) != ""
}

// translateEtcdEndpoints turns the v1 comma-separated endpoint string into
// the v2 list form. v1 held one string whichever layer the value came from,
// and the translated source resolves through the config file stage, which
// only accepts the list shape — so the split happens here for every layer.
func (t *translator) translateEtcdEndpoints() {
	h := t.gather(fEtcdEndpoints)
	if h == nil {
		return
	}

	if s, ok := h.value.(string); ok {
		items := make([]any, 0, strings.Count(s, ",")+1)
		for _, item := range strings.Split(s, ",") {
			if item = strings.TrimSpace(item); item != "" {
				items = append(items, item)
			}
		}
		h = &hit{value: items, layer: h.layer, key: h.key}
	}

	t.emit("milvus.etcd.endpoints", h, false, "")
}

// translateCrossStorage maps the v1 boolean onto the v2 transfer mode. The
// warning the false spelling can earn needs the resolved backends to judge,
// so it is settled by Migrate after resolution rather than here.
func (t *translator) translateCrossStorage() {
	h := t.gather(fCrossStorage)
	if h == nil {
		// v1 defaulted to false, so an unmigrated file reads as auto too.
		t.crossStorageAuto = true
		if t.report != nil {
			t.report.commentKey("transfer.mode", "v1 minio.crossStorage=false mapped to auto")
		}
		return
	}

	mode := v2.TransferAuto
	if asBool(h.value) {
		mode = v2.TransferStreaming
	} else {
		t.crossStorageAuto = true
		if t.report != nil {
			t.report.commentKey("transfer.mode", "v1 minio.crossStorage=false mapped to auto")
		}
	}
	t.emit("transfer.mode", &hit{value: mode, layer: h.layer, key: h.key}, false, "")
}

// sideState is what one storage side settled on, so the backup side can
// compare against what it would inherit from the Milvus side.
type sideState struct {
	provider string // canonical provider spelling, "" when nothing set it
	useIAM   bool
	auth     string // effective v2 auth type, "" for local storage
}

// translateStorage translates both storage sides, the Milvus deployment
// storage first so the backup side can tell what it inherits.
func (t *translator) translateStorage() {
	milvus := t.storageSide(milvusStorage, nil, nil)
	t.storageSide(backupStorage, milvusStorage, &milvus)
}

// storageSide translates one storage side: the provider spelling, the plain
// endpoint fields, and the credential fields, whose v2 homes depend on the
// provider and authentication type the side settles on. inherit is the Milvus
// side's field set and up its outcome when translating the backup side, both
// nil otherwise: v1 defaulted an unset backup credential to the primary one,
// so a backup field with no hit of its own falls back to the Milvus hit —
// except the GCP credentials JSON, the one credential v1 did not inherit.
func (t *translator) storageSide(s *storageSide, inherit *storageSide, up *sideState) sideState {
	var st sideState

	var providerHit *hit
	if h := t.gather(s.provider); h != nil {
		st.provider = canonicalProvider(asString(h.value))
		providerHit = &hit{value: st.provider, layer: h.layer, key: h.key}
		t.emit(s.prefix+".provider", providerHit, false, "")
	} else if up != nil {
		st.provider = up.provider
	}

	var iamHit *hit
	if h := t.gather(s.useIAM); h != nil {
		st.useIAM = asBool(h.value)
		iamHit = h
	} else if up != nil {
		st.useIAM = up.useIAM
	}

	st.auth = effectiveAuth(st.provider, st.useIAM)

	// auth.type is emitted only when it differs from what resolution would
	// inherit: the Milvus side inherits the v2 default, the backup side the
	// Milvus side's value. The emission carries the key that drove the
	// decision as its origin — one always exists when the outcome diverges.
	inherited := v2.AuthStatic
	if up != nil {
		inherited = up.auth
	}
	if st.auth != "" && st.auth != inherited {
		driver := providerHit
		if driver == nil {
			driver = iamHit
		}
		t.emit(s.prefix+".auth.type", &hit{value: st.auth, layer: driver.layer, key: driver.key}, false, "")
	}

	for _, f := range s.plainFields() {
		if h := t.gather(f); h != nil {
			t.emit(f.v2, h, f.secret, f.v2env)
		}
	}

	// Credentials route by the settled auth type; a credential that does not
	// apply to it is dropped, as v1 dropped it.
	fallback := func(f *v1Field) *v1Field {
		if inherit == nil {
			return nil
		}
		switch f {
		case s.accessKeyID:
			return inherit.accessKeyID
		case s.secretAccessKey:
			return inherit.secretAccessKey
		case s.token:
			return inherit.token
		case s.iamEndpoint:
			return inherit.iamEndpoint
		default:
			return nil
		}
	}
	switch st.auth {
	case v2.AuthStatic:
		t.emitCredential(s, "auth.accessKeyID", s.accessKeyID, fallback(s.accessKeyID), "")
		t.emitCredential(s, "auth.secretAccessKey", s.secretAccessKey, fallback(s.secretAccessKey), "AUTH_SECRET_ACCESS_KEY")
		t.emitCredential(s, "auth.sessionToken", s.token, fallback(s.token), "AUTH_SESSION_TOKEN")
	case v2.AuthSharedKey:
		// v1 overloaded the access key ID as the Azure account name and the
		// secret access key as the account key.
		t.emitCredential(s, "accountName", s.accessKeyID, fallback(s.accessKeyID), "")
		t.emitCredential(s, "auth.accountKey", s.secretAccessKey, fallback(s.secretAccessKey), "AUTH_ACCOUNT_KEY")
	case v2.AuthDefault:
		// Azure with v1's useIAM: DefaultAzureCredential, so no key exists to
		// migrate, but the account name still builds the blob service URL.
		t.emitCredential(s, "accountName", s.accessKeyID, fallback(s.accessKeyID), "")
	case v2.AuthServiceAccount:
		t.emitCredential(s, "auth.credentialsFile", s.gcpCredentialJSON, nil, "AUTH_CREDENTIALS_FILE")
		t.warn("%s.auth.credentialsFile expects a path to the service account JSON file; verify it is not inline JSON", s.prefix)
	case v2.AuthIAM:
		t.emitCredential(s, "auth.endpoint", s.iamEndpoint, fallback(s.iamEndpoint), "")
	}

	return st
}

// emitCredential carries one credential field to its v2 key, falling back to
// the Milvus side's hit when the backup side has none of its own. envSuffix
// names the credential within the side's v2 environment namespace; a field
// with an empty one is never deferred, matching v1, which wrote non-secret
// values out regardless of where they came from.
func (t *translator) emitCredential(s *storageSide, suffix string, f, fallback *v1Field, envSuffix string) {
	h := t.gather(f)
	if h == nil && fallback != nil {
		h = t.gather(fallback)
	}
	if h == nil {
		return
	}

	t.emit(s.prefix+"."+suffix, h, envSuffix != "", credentialEnv(s, envSuffix))
}

// injectBackupRootPath preserves the one backup-side default v2 deliberately
// changed: v1 let an unset backup root path inherit the Milvus storage root
// path — always, since the v1 Milvus root path defaults to "files" rather
// than empty — while v2 keeps the two independent and defaults to "backup".
// The inherited location is written into the translated source explicitly,
// carrying the origin of the Milvus root path it was inherited from.
func (t *translator) injectBackupRootPath() {
	if t.gather(backupStorage.rootPath) != nil {
		return
	}

	// v1 settled an unset backup root path as cmp.Or(default, milvus root
	// path, "backup"): it inherited whatever the Milvus root path resolved
	// to, unless that was empty, in which case it landed on "backup". All
	// three outcomes are written out explicitly so backups keep landing
	// where v1 put them.
	if h := t.gather(milvusStorage.rootPath); h != nil && asString(h.value) != "" {
		t.emit(backupStorage.rootPath.v2, h, false, "")
		t.warn("backup root path %q was inherited from the milvus storage root path in v1; v2 keeps them independent and now sets it explicitly",
			asString(h.value))
		return
	}

	// Nothing set either root path: v1 still inherited, landing on the v1
	// default of the Milvus root path ("files", from the retired v1 schema).
	// A Milvus root path explicitly set empty fell through cmp.Or to
	// "backup". Either way the value is a pure v1 default, not a hit.
	if h := t.gather(milvusStorage.rootPath); h != nil {
		t.settle(backupStorage.rootPath.v2, "backup")
		return
	}

	t.settle(backupStorage.rootPath.v2, "files")
}

// declareV2 stamps the discriminator v2.LoadFrom requires onto the translated
// source. The translation vouches for the result being v2-shaped, so it
// declares v2 on the translated file's behalf.
func (t *translator) declareV2() {
	t.settle(v2.VersionKey, v2.Version)
}

// settle writes a value the translation worked out itself rather than read
// from a v1 source: a v1 default the translated source must carry explicitly.
// The key is lower-cased the way emit writes its entries, which is the only
// spelling the source looks keys up by.
func (t *translator) settle(key, value string) {
	t.file[strings.ToLower(key)] = param.Input{Value: value, Kind: param.SourceDefault}
}

// finishLeftovers handles the v1 keys no pass consumed. A key the v1 schema
// never declared is dropped with a warning, matching v1's silent ignore but
// saying so; a declared key that does not apply to the settled configuration —
// a credential belonging to another auth type — is ignored silently, as v1
// ignored it. Unknown --set keys pass through untouched: a v2-spelled override
// is legal against a v1 file, and a genuinely unknown one is warned about by
// the v2 loader.
func (t *translator) finishLeftovers() {
	for _, key := range t.src.ConfigFileKeys() {
		if key == strings.ToLower(v2.VersionKey) || t.consumedFile[key] || v1FileKeys[key] {
			continue
		}
		t.warn("cfg: unknown v1 config file key %q, ignoring it", key)
	}

	for _, key := range t.src.OverrideKeys() {
		if t.consumedOverride[strings.ToLower(key)] {
			continue
		}
		if v, ok := t.src.OverrideValue(key); ok {
			t.override[key] = v
		}
	}
}

// effectiveAuth settles the v2 authentication type of a storage side from the
// provider and the v1 useIAM flag, in the order v1 applied them: the provider
// decides first, and useIAM only speaks for the S3-compatible providers.
func effectiveAuth(provider string, useIAM bool) string {
	switch {
	case provider == v2.ProviderLocal:
		// Local storage is a directory: no credentials, no auth type.
		return ""
	case provider == v2.ProviderAzure:
		if useIAM {
			// v1's useIAM for Azure meant DefaultAzureCredential (managed
			// identity / workload identity).
			return v2.AuthDefault
		}
		return v2.AuthSharedKey
	case provider == v2.ProviderGCPNative:
		return v2.AuthServiceAccount
	case useIAM:
		return v2.AuthIAM
	default:
		return v2.AuthStatic
	}
}

// canonicalProvider folds the v1 provider spelling aliases into the single v2
// name for each provider.
func canonicalProvider(p string) string {
	switch strings.ToLower(p) {
	case "ali", "alibaba", "alicloud", "aliyun":
		return v2.ProviderAliyun
	case "tc", "tencent":
		return v2.ProviderTencent
	default:
		return strings.ToLower(p)
	}
}

// credentialEnv builds the v2 environment variable name for a storage
// credential, e.g. MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY.
func credentialEnv(s *storageSide, suffix string) string {
	if suffix == "" {
		return ""
	}
	if s.prefix == "backup.storage" {
		return "BACKUP_STORAGE_" + suffix
	}

	return "MILVUS_STORAGE_" + suffix
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	return fmt.Sprint(v)
}

// asBool reads a v1 boolean from whichever form its source carries: a decoded
// YAML value, or the string form --set and environment variables hold.
func asBool(v any) bool {
	switch vv := v.(type) {
	case bool:
		return vv
	case string:
		b, _ := strconv.ParseBool(vv)
		return b
	default:
		return false
	}
}

// asInt reads a v1 integer from whichever form its source carries.
func asInt(v any) (int, error) {
	switch vv := v.(type) {
	case int:
		return vv, nil
	case int64:
		return int(vv), nil
	case float64:
		return int(vv), nil
	case string:
		return strconv.Atoi(vv)
	default:
		return 0, fmt.Errorf("cfg: want integer, got %T", v)
	}
}
