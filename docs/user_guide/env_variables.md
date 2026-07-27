# Milvus Backup: Environment Variables

Credentials can be set through an environment variable, which keeps them out of the configuration file. Every other parameter is named by its configuration key alone, and `--set` overrides it for a single run.

## How to use environment variables

Set them in your shell before running Milvus Backup:

```bash
export MILVUS_PASSWORD=Milvus
./milvus-backup server
```

A value is resolved from the first source that provides it:

```text
--set  >  environment variable  >  config file  >  default
```

`--set` takes a config key, and for the credentials below either spelling: `--set MILVUS_USER=root` and `--set milvus.user=root` do the same thing. Anything else has to be spelled as its config key — `--set milvus.grpc.port=29530`.

`milvus-backup config show` prints where each resolved value actually came from.

## Why credentials only

A credential is the one value that has to come from outside the file. It arrives from a Kubernetes Secret, a CI secret store, or an operator, none of which want to template a configuration file to deliver it.

Every other parameter is safer without a variable. Kubernetes injects one for every Service in the namespace, so a Service named `milvus` becomes `MILVUS_PORT=tcp://10.0.0.1:19530` in every pod — exactly the name a connection parameter would claim, and an environment variable outranks the configuration file. Milvus Backup used to read `MILVUS_PORT`, and the injected value silently replaced the configured one, which was reported twice as a connection failure ([#197](https://github.com/zilliztech/milvus-backup/issues/197), [#617](https://github.com/zilliztech/milvus-backup/issues/617)). Credential names are not at risk, because the injected names always end in `_PORT`, `_HOST` or `_SERVICE_*`.

## Supported environment variables

Environment variables belong to the schema version the configuration file is written in. The names below apply to a file that declares `configVersion: v2`; a file written before that is read with the older schema and keeps the older names, which covered far more parameters. `milvus-backup config migrate` converts the file and reports every variable that has to be replaced.

### Milvus

| Config key | Environment variable |
|------------|----------------------|
| `milvus.user` | `MILVUS_USER` |
| `milvus.password` | `MILVUS_PASSWORD` |

### Storage

Both storage sections take the same credentials: `MILVUS_STORAGE_` describes the storage the Milvus deployment uses, and `BACKUP_STORAGE_` the backup destination. Anything the backup side does not set is inherited from the Milvus side.

| Config key | Environment variable |
|------------|----------------------|
| `milvus.storage.accountName` | `MILVUS_STORAGE_ACCOUNT_NAME` |
| `milvus.storage.auth.accessKeyID` | `MILVUS_STORAGE_AUTH_ACCESS_KEY_ID` |
| `milvus.storage.auth.secretAccessKey` | `MILVUS_STORAGE_AUTH_SECRET_ACCESS_KEY` |
| `milvus.storage.auth.sessionToken` | `MILVUS_STORAGE_AUTH_SESSION_TOKEN` |
| `milvus.storage.auth.accountKey` | `MILVUS_STORAGE_AUTH_ACCOUNT_KEY` |
| `milvus.storage.auth.credentialsFile` | `MILVUS_STORAGE_AUTH_CREDENTIALS_FILE` |
| `backup.storage.accountName` | `BACKUP_STORAGE_ACCOUNT_NAME` |
| `backup.storage.auth.accessKeyID` | `BACKUP_STORAGE_AUTH_ACCESS_KEY_ID` |
| `backup.storage.auth.secretAccessKey` | `BACKUP_STORAGE_AUTH_SECRET_ACCESS_KEY` |
| `backup.storage.auth.sessionToken` | `BACKUP_STORAGE_AUTH_SESSION_TOKEN` |
| `backup.storage.auth.accountKey` | `BACKUP_STORAGE_AUTH_ACCOUNT_KEY` |
| `backup.storage.auth.credentialsFile` | `BACKUP_STORAGE_AUTH_CREDENTIALS_FILE` |

`*_ACCOUNT_NAME` is the Azure storage account the account key belongs to. `*_AUTH_CREDENTIALS_FILE` is the path to the Google Cloud service account JSON file, not its contents.

Which credentials apply is decided by `auth.type` in the configuration file. Naming one the chosen type does not use is rejected rather than ignored:

| Auth type | Credentials it uses |
|-----------|---------------------|
| `static` | `*_AUTH_ACCESS_KEY_ID` and `*_AUTH_SECRET_ACCESS_KEY`, optionally `*_AUTH_SESSION_TOKEN` |
| `sharedKey` | `*_ACCOUNT_NAME` and `*_AUTH_ACCOUNT_KEY` (Azure) |
| `serviceAccount` | `*_AUTH_CREDENTIALS_FILE` (Google Cloud) |
| `iam` | none — optionally `auth.endpoint` in the configuration file |
| `default` | none — resolved by the provider SDK |

### Zilliz Cloud

| Config key | Environment variable |
|------------|----------------------|
| `zillizCloud.apiKey` | `ZILLIZ_CLOUD_API_KEY` |

## Everything else

The remaining parameters live in the configuration file. See [configs/backup.yaml](../../configs/backup.yaml) for the full list with defaults and comments. To change one for a single run, pass its config key to `--set`:

```bash
./milvus-backup --set milvus.grpc.address=milvus-proxy --set transfer.concurrency=64 list
```
