package v2

import (
	"cmp"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/cfg/param"
)

var (
	providers = []string{
		ProviderLocal, ProviderMinio, ProviderS3, ProviderAWS,
		ProviderGCP, ProviderGCPNative, ProviderAzure,
		ProviderAliyun, ProviderTencent, ProviderHwc,
	}

	transferModes = []string{TransferAuto, TransferDirect, TransferStreaming}

	tlsModes = []string{TLSDisabled, TLSServer, TLSMutual}

	// v1TLSModes is what v1 called each mode, kept only to explain the value a
	// config copied from a v1 file still carries.
	v1TLSModes = map[string]string{"0": TLSDisabled, "1": TLSServer, "2": TLSMutual}

	// authTypes lists the authentication types each provider accepts. Anything
	// not listed is rejected rather than silently ignored, because a credential
	// a provider cannot use is always a configuration mistake.
	authTypes = map[string][]string{
		ProviderAzure:     {AuthSharedKey, AuthDefault},
		ProviderGCPNative: {AuthServiceAccount, AuthDefault},
	}
	// s3AuthTypes applies to every S3-compatible provider.
	s3AuthTypes = []string{AuthStatic, AuthIAM, AuthDefault}
)

// field is a resolved parameter that can report the key it was declared with
// and whether anything set it explicitly.
type field interface {
	ConfigKeys() []string
	IsDefault() bool
}

// keyOf returns the config file key a parameter is spelled with, for use in
// error messages.
func keyOf(f field) string { return cmp.Or(f.ConfigKeys()...) }

// Validate reports every problem in the resolved configuration at once, so a
// broken file takes one round trip to fix rather than one per mistake.
func (c *Config) Validate() error {
	return errors.Join(
		c.Milvus.validate(),
		c.Milvus.Storage.validate(),
		c.Backup.Storage.validate(),
		c.Backup.validate(),
		c.Restore.validate(),
		c.Transfer.validate(),
	)
}

func (c *MilvusConfig) validate() error {
	errs := []error{validatePort(&c.Grpc.Port), validateTLSMode(&c.Grpc.TLSMode)}

	// v1 quietly downgraded mutual TLS to server TLS when the client key pair
	// was missing. v2 is a new schema, so the deprecation is settled here.
	if c.Grpc.TLSMode.Val == TLSMutual && (c.Grpc.MTLSCertPath.Val == "" || c.Grpc.MTLSKeyPath.Val == "") {
		errs = append(errs, fmt.Errorf("cfg: %s and %s are required when %s is %q",
			keyOf(&c.Grpc.MTLSCertPath), keyOf(&c.Grpc.MTLSKeyPath), keyOf(&c.Grpc.TLSMode), TLSMutual))
	}

	errs = append(errs, validateEndpoint(&c.Rest.Endpoint), validateEndpoint(&c.Management.Endpoint))

	if len(c.Etcd.Endpoints.Val) == 0 {
		errs = append(errs, fmt.Errorf("cfg: %s: at least one endpoint is required", cmp.Or(c.Etcd.Endpoints.Keys...)))
	}

	return errors.Join(errs...)
}

func (c *BackupConfig) validate() error {
	return errors.Join(
		validatePositive(&c.Concurrency.Collections),
		validatePositive(&c.Concurrency.Segments),
	)
}

func (c *RestoreConfig) validate() error {
	return errors.Join(
		validatePositive(&c.Concurrency.Collections),
		validatePositive(&c.Concurrency.ImportJobs),
	)
}

func (c *TransferConfig) validate() error {
	return errors.Join(
		validateEnum(&c.Mode, transferModes),
		validatePositive(&c.Concurrency),
		validatePositive(&c.MultipartCopyThresholdMiB),
	)
}

func (c *StorageConfig) validate() error {
	if err := validateEnum(&c.Provider, providers); err != nil {
		// Without a provider, nothing below can be judged.
		return err
	}

	provider := c.Provider.Val
	if provider == ProviderLocal {
		// Local storage is a directory: it has no endpoint and no credentials.
		return nil
	}

	errs := []error{validatePort(&c.Port)}

	switch {
	case provider == ProviderAzure && c.AccountName.Val == "":
		errs = append(errs, fmt.Errorf("cfg: %s is required for the azure provider", keyOf(&c.AccountName)))
	case provider != ProviderAzure && !c.AccountName.IsDefault():
		errs = append(errs, fmt.Errorf("cfg: %s only applies to the azure provider, but %s is %q",
			keyOf(&c.AccountName), keyOf(&c.Provider), provider))
	}

	errs = append(errs, c.Auth.validate(provider))

	return errors.Join(errs...)
}

func (c *StorageAuthConfig) validate(provider string) error {
	allowed, ok := authTypes[provider]
	if !ok {
		allowed = s3AuthTypes
	}
	if err := validateEnum(&c.Type, allowed); err != nil {
		return fmt.Errorf("%w (provider %s)", err, provider)
	}

	var (
		static  = []field{&c.AccessKeyID, &c.SecretAccessKey, &c.SessionToken}
		azure   = []field{&c.AccountKey}
		gcp     = []field{&c.CredentialsFile}
		iam     = []field{&c.Endpoint}
		errs    []error
		applies []field
	)

	switch c.Type.Val {
	case AuthStatic:
		applies = static
		errs = append(errs, validateRequired(&c.AccessKeyID), validateRequired(&c.SecretAccessKey))
	case AuthSharedKey:
		applies = azure
		errs = append(errs, validateRequired(&c.AccountKey))
	case AuthServiceAccount:
		applies = gcp
		errs = append(errs, validateRequired(&c.CredentialsFile))
	case AuthIAM:
		// The endpoint is optional: AWS and GCP read instance metadata from a
		// well-known address, while MinIO needs one configured.
		applies = iam
	case AuthDefault:
		// The provider SDK resolves credentials on its own.
	}

	all := slices.Concat(static, azure, gcp, iam)
	for _, f := range all {
		if slices.Contains(applies, f) || f.IsDefault() {
			continue
		}
		errs = append(errs, fmt.Errorf("cfg: %s does not apply when %s is %q", keyOf(f), keyOf(&c.Type), c.Type.Val))
	}

	return errors.Join(errs...)
}

// validateTLSMode names the v2 spelling when the value is one of the numbers
// v1 copied from the Milvus server side parameter.
func validateTLSMode(val *param.Value[string]) error {
	err := validateEnum(val, tlsModes)
	if err == nil {
		return nil
	}

	if mode, ok := v1TLSModes[val.Val]; ok {
		return fmt.Errorf("cfg: %s: %q is the v1 spelling of this mode, write it as %q", keyOf(val), val.Val, mode)
	}

	return err
}

// validateEnum normalises val to the canonical spelling of the allowed value
// it matches, so "SharedKey" and "sharedKey" name the same thing.
func validateEnum(val *param.Value[string], allowed []string) error {
	for _, a := range allowed {
		if strings.EqualFold(val.Val, a) {
			val.Val = a
			return nil
		}
	}

	return fmt.Errorf("cfg: %s: invalid value %q, want one of: %s", keyOf(val), val.Val, strings.Join(allowed, ", "))
}

func validateRequired(val *param.Value[string]) error {
	if val.Val != "" {
		return nil
	}

	return fmt.Errorf("cfg: %s is required", keyOf(val))
}

func validatePositive[T int | int64](val *param.Value[T]) error {
	if val.Val > 0 {
		return nil
	}

	return fmt.Errorf("cfg: %s: invalid value %d, want a positive number", keyOf(val), val.Val)
}

func validatePort(val *param.Value[int]) error {
	if port := val.Val; port < 1 || port > 65535 {
		return fmt.Errorf("cfg: %s: invalid port %d, want 1-65535", keyOf(val), port)
	}

	return nil
}

// validateEndpoint accepts an empty endpoint, which means "derive it" or "not
// used", and otherwise requires an absolute http(s) URL.
func validateEndpoint(val *param.Value[string]) error {
	if val.Val == "" {
		return nil
	}

	u, err := url.Parse(val.Val)
	if err != nil {
		return fmt.Errorf("cfg: %s: invalid URL %q: %w", keyOf(val), val.Val, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("cfg: %s: invalid URL %q, want an absolute http(s) URL such as http://localhost:9091", keyOf(val), val.Val)
	}

	return nil
}
