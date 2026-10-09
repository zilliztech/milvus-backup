package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/zilliztech/milvus-backup/internal/cfg"
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
	return src.Provider == cfg.ProviderAzure && dest.Provider == cfg.ProviderAzure &&
		src.Credential.AzureAccountName != dest.Credential.AzureAccountName
}

// ResolveSourceSAS fills storeCfg.SourceSAS for a cross-account Azure copy
// reading from storeCfg's account: the operator-provided token when there is
// one, else one minted from storeCfg's own credential. Minting happens once
// per task, and the token only ever leaves inside the extfs handed to Milvus.
func ResolveSourceSAS(ctx context.Context, storeCfg Config) (Config, error) {
	if storeCfg.SourceSAS != "" {
		storeCfg.SourceSAS = strings.TrimPrefix(strings.TrimSpace(storeCfg.SourceSAS), "?")
		return storeCfg, nil
	}

	token, err := mintSourceSAS(ctx, storeCfg)
	if err != nil {
		return storeCfg, fmt.Errorf("storage: mint source sas for %s: %w", storeCfg.Credential.AzureAccountName, err)
	}
	storeCfg.SourceSAS = token

	return storeCfg, nil
}

// mintSourceSAS signs a container-scoped read SAS for storeCfg's bucket. A
// shared key signs a service SAS locally; IAM has to go through a user
// delegation key, which needs Entra credentials and one network call. The
// granted permissions are what a copy source is read with: list and read the
// one container, nothing else.
func mintSourceSAS(ctx context.Context, storeCfg Config) (string, error) {
	// Shared-key signing is a local HMAC, so the start time absorbs clock skew
	// the same way the user-delegation path in azure.go does.
	now := time.Now().Add(-10 * time.Second)
	expiry := now.Add(_sourceSASTTL)
	values := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now,
		ExpiryTime:    expiry,
		Permissions:   new(sas.ContainerPermissions{Read: true, List: true}).String(),
		ContainerName: storeCfg.Bucket,
	}

	switch storeCfg.Credential.Type {
	case Static:
		cred, err := azblob.NewSharedKeyCredential(storeCfg.Credential.AK, storeCfg.Credential.SK)
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
		endpoint := strings.TrimSuffix(storeCfg.Endpoint, ":443")
		svc, err := service.NewClient(
			fmt.Sprintf("https://%s.blob.%s", storeCfg.Credential.AzureAccountName, endpoint), cred, nil)
		if err != nil {
			return "", fmt.Errorf("storage: new azure service client: %w", err)
		}

		info := service.KeyInfo{
			Start:  new(now.Format(sas.TimeFormat)),
			Expiry: new(expiry.Format(sas.TimeFormat)),
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
		return "", fmt.Errorf("storage: minting a source sas needs a shared key or iam credential, not %s", storeCfg.Credential.Type)
	}
}
