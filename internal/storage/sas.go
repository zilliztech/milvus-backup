package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	v2 "github.com/zilliztech/milvus-backup/internal/cfg/v2"
)

// _sourceSASTTL is how long a minted source SAS stays valid. Milvus keeps the
// spec holding it until the export or restore job goes terminal, so the token
// has to outlive the whole copy window — the same budget the streamed-copy SAS
// in azure.go works with.
const _sourceSASTTL = 48 * time.Hour

// CrossAccountAzure reports whether a snapshot-format copy from src to dest
// reads an Azure blob that lives in another storage account. The accounts are
// compared, not the endpoints: the same account reached through two endpoints
// (a private link and the public one) is still one account, whose reads need no
// SAS — Milvus rejects a SAS there as an input error.
func CrossAccountAzure(src, dest Config) bool {
	return src.Provider == v2.ProviderAzure && dest.Provider == v2.ProviderAzure &&
		src.Credential.AzureAccountName != dest.Credential.AzureAccountName
}

// ResolveSourceSAS fills cfg.SourceSAS for a cross-account Azure copy reading
// from cfg's account: the operator-provided token when there is one, else one
// minted from cfg's own credential. Minting happens once per task, and the
// token only ever leaves inside the extfs handed to Milvus.
func ResolveSourceSAS(ctx context.Context, cfg Config) (Config, error) {
	if cfg.SourceSAS != "" {
		cfg.SourceSAS = strings.TrimPrefix(strings.TrimSpace(cfg.SourceSAS), "?")
		return cfg, nil
	}

	token, err := mintSourceSAS(ctx, cfg)
	if err != nil {
		return cfg, fmt.Errorf("storage: mint source sas for %s: %w", cfg.Credential.AzureAccountName, err)
	}
	cfg.SourceSAS = token

	return cfg, nil
}

// mintSourceSAS signs a container-scoped read SAS for cfg's bucket. A shared
// key signs a service SAS locally; IAM has to go through a user delegation key,
// which needs Entra credentials and one network call. The granted permissions
// are what a copy source is read with: list and read the one container, nothing
// else.
func mintSourceSAS(ctx context.Context, cfg Config) (string, error) {
	// Shared-key signing is a local HMAC, so the start time absorbs clock skew
	// the same way the user-delegation path in azure.go does.
	now := time.Now().Add(-10 * time.Second)
	expiry := now.Add(_sourceSASTTL)
	values := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now,
		ExpiryTime:    expiry,
		Permissions:   to.Ptr(sas.ContainerPermissions{Read: true, List: true}).String(),
		ContainerName: cfg.Bucket,
	}

	switch cfg.Credential.Type {
	case Static:
		cred, err := azblob.NewSharedKeyCredential(cfg.Credential.AK, cfg.Credential.SK)
		if err != nil {
			return "", fmt.Errorf("storage: new azure shared key credential: %w", err)
		}
		queryParams, err := values.SignWithSharedKey(cred)
		if err != nil {
			return "", fmt.Errorf("storage: sign source sas: %w", err)
		}
		return queryParams.Encode(), nil
	case IAM:
		// A user delegation SAS is the only kind an IAM identity can mint; it
		// asks the service for a delegation key first, which is the one call
		// that needs the context.
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return "", fmt.Errorf("storage: new default azure credential: %w", err)
		}
		endpoint := strings.TrimSuffix(cfg.Endpoint, ":443")
		svc, err := service.NewClient(
			fmt.Sprintf("https://%s.blob.%s", cfg.Credential.AzureAccountName, endpoint), cred, nil)
		if err != nil {
			return "", fmt.Errorf("storage: new azure service client: %w", err)
		}

		info := service.KeyInfo{
			Start:  to.Ptr(now.Format(sas.TimeFormat)),
			Expiry: to.Ptr(expiry.Format(sas.TimeFormat)),
		}
		udc, err := svc.GetUserDelegationCredential(ctx, info, nil)
		if err != nil {
			return "", fmt.Errorf("storage: get user delegation credential: %w", err)
		}

		queryParams, err := values.SignWithUserDelegation(udc)
		if err != nil {
			return "", fmt.Errorf("storage: sign source sas: %w", err)
		}
		return queryParams.Encode(), nil
	default:
		return "", fmt.Errorf("storage: minting a source sas needs a shared key or iam credential, not %s", cfg.Credential.Type)
	}
}
