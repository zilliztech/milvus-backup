package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_Storage(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "UnknownProvider",
			yaml:    "milvus:\n  storage:\n    provider: ceph\n",
			wantErr: `milvus.storage.provider: invalid value "ceph"`,
		},
		{
			name:    "AzureRequiresAccountName",
			yaml:    "milvus:\n  storage:\n    provider: azure\n    auth:\n      type: sharedKey\n      accountKey: key\n",
			wantErr: "milvus.storage.accountName is required for the azure provider",
		},
		{
			name:    "AccountNameOnlyAppliesToAzure",
			yaml:    "milvus:\n  storage:\n    provider: s3\n    accountName: account\n",
			wantErr: "milvus.storage.accountName only applies to the azure provider",
		},
		{
			name:    "AzureRejectsStaticAuth",
			yaml:    "milvus:\n  storage:\n    provider: azure\n    accountName: account\n    auth:\n      type: static\n",
			wantErr: `milvus.storage.auth.type: invalid value "static", want one of: sharedKey, default`,
		},
		{
			name:    "SharedKeyRequiresAccountKey",
			yaml:    "milvus:\n  storage:\n    provider: azure\n    accountName: account\n    auth:\n      type: sharedKey\n",
			wantErr: "milvus.storage.auth.accountKey is required",
		},
		{
			name:    "ServiceAccountRequiresCredentialsFile",
			yaml:    "milvus:\n  storage:\n    provider: gcpnative\n    auth:\n      type: serviceAccount\n",
			wantErr: "milvus.storage.auth.credentialsFile is required",
		},
		{
			name:    "StaticRejectsAccountKey",
			yaml:    "milvus:\n  storage:\n    auth:\n      type: static\n      accountKey: key\n",
			wantErr: `milvus.storage.auth.accountKey does not apply when milvus.storage.auth.type is "static"`,
		},
		{
			name:    "DefaultRejectsStaticKeys",
			yaml:    "milvus:\n  storage:\n    auth:\n      type: default\n      accessKeyID: ak\n",
			wantErr: `milvus.storage.auth.accessKeyID does not apply when milvus.storage.auth.type is "default"`,
		},
		{
			name:    "PortOutOfRange",
			yaml:    "milvus:\n  storage:\n    port: 70000\n",
			wantErr: "milvus.storage.port: invalid port 70000",
		},
		{
			name:    "BackupStorageIsValidatedToo",
			yaml:    "backup:\n  storage:\n    provider: ceph\n",
			wantErr: `backup.storage.provider: invalid value "ceph"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeYAML(t, tt.yaml), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidate_StorageAcceptsProviderShapes(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "AzureSharedKey", yaml: "milvus:\n  storage:\n    provider: azure\n    accountName: account\n    auth:\n      type: sharedKey\n      accountKey: key\n"},
		{name: "AzureDefault", yaml: "milvus:\n  storage:\n    provider: azure\n    accountName: account\n    auth:\n      type: default\n"},
		{name: "GCPNativeServiceAccount", yaml: "milvus:\n  storage:\n    provider: gcpnative\n    auth:\n      type: serviceAccount\n      credentialsFile: /gcp.json\n"},
		{name: "IAMWithoutEndpoint", yaml: "milvus:\n  storage:\n    provider: aws\n    auth:\n      type: iam\n"},
		{name: "IAMWithEndpoint", yaml: "milvus:\n  storage:\n    provider: minio\n    auth:\n      type: iam\n      endpoint: http://iam:8080\n"},
		// Local storage is a directory: no endpoint, no credentials.
		{name: "Local", yaml: "backup:\n  storage:\n    provider: local\n    rootPath: /data/backup\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeYAML(t, tt.yaml), nil)
			assert.NoError(t, err)
		})
	}
}

// An auth type is matched case-insensitively and stored in its canonical
// spelling, so the resolved value is safe to switch on.
func TestValidate_NormalizesEnums(t *testing.T) {
	c, err := Load(writeYAML(t, "milvus:\n  storage:\n    provider: AZURE\n    accountName: account\n    auth:\n      type: sharedkey\n      accountKey: key\ntransfer:\n  mode: STREAMING\n"), nil)
	require.NoError(t, err)

	assert.Equal(t, ProviderAzure, c.Milvus.Storage.Provider.Val)
	assert.Equal(t, AuthSharedKey, c.Milvus.Storage.Auth.Type.Val)
	assert.Equal(t, TransferStreaming, c.Transfer.Mode.Val)
}

func TestValidate_Milvus(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "UnknownTLSMode",
			wantErr: `milvus.grpc.tlsMode: invalid value "strict", want one of: disabled, server, mutual`,
			yaml:    "milvus:\n  grpc:\n    tlsMode: strict\n",
		},
		{
			// v1 spelled the modes as numbers, copied from the Milvus server
			// side parameter. They mean nothing in v2.
			name:    "V1NumericTLSMode",
			wantErr: `milvus.grpc.tlsMode: "2" is the v1 spelling of this mode, write it as "mutual"`,
			yaml:    "milvus:\n  grpc:\n    tlsMode: 2\n",
		},
		{
			// v1 quietly downgraded to server TLS here.
			name:    "MutualTLSRequiresClientKeyPair",
			wantErr: `milvus.grpc.mtlsCertPath and milvus.grpc.mtlsKeyPath are required when milvus.grpc.tlsMode is "mutual"`,
			yaml:    "milvus:\n  grpc:\n    tlsMode: mutual\n    caCertPath: /ca.pem\n",
		},
		{
			name:    "ManagementEndpointMustBeURL",
			yaml:    "milvus:\n  management:\n    endpoint: localhost:9091\n",
			wantErr: "milvus.management.endpoint: invalid URL",
		},
		{
			name:    "EtcdEndpointsMustNotBeEmpty",
			yaml:    "milvus:\n  etcd:\n    endpoints: []\n",
			wantErr: "milvus.etcd.endpoints: at least one endpoint is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeYAML(t, tt.yaml), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidate_Concurrency(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "BackupCollections",
			yaml:    "backup:\n  concurrency:\n    collections: 0\n",
			wantErr: "backup.concurrency.collections: invalid value 0",
		},
		{
			name:    "RestoreImportJobs",
			yaml:    "restore:\n  concurrency:\n    importJobs: -1\n",
			wantErr: "restore.concurrency.importJobs: invalid value -1",
		},
		{
			name:    "TransferConcurrency",
			yaml:    "transfer:\n  concurrency: 0\n",
			wantErr: "transfer.concurrency: invalid value 0",
		},
		{
			name:    "TransferMode",
			yaml:    "transfer:\n  mode: copy\n",
			wantErr: `transfer.mode: invalid value "copy", want one of: auto, direct, streaming`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeYAML(t, tt.yaml), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
