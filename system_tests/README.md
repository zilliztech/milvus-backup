# Backup and restore system tests

This suite validates Milvus Backup through its public REST API and verifies the
restored data through PyMilvus. It owns test behavior, datasets, assertions, and
test evidence. Environment provisioning belongs to the CI job repository.

## Layout

```text
system_tests/
├── backup_restore/
│   ├── clients/          # Milvus Backup REST API adapters
│   ├── datasets/         # Deterministic source data builders
│   ├── verification/     # Source-to-target verification
│   └── test_*.py         # User-visible backup and restore workflows
├── unit/                 # Fast tests for the system-test support code
├── pytest.ini
└── requirements.txt
```

The suite emits standard pytest/JUnit results. Allure attachments contain the
route metadata, exact Milvus Git commits, and backup/restore responses. Tokens
and storage credentials are never attached.

## Environment contract

The required variables are:

- `BACKUP_TEST_SOURCE_MILVUS_URI`, `BACKUP_TEST_TARGET_MILVUS_URI`
- `BACKUP_TEST_BACKUP_API_URI`, `BACKUP_TEST_RESTORE_API_URI`
- `BACKUP_TEST_SOURCE_TOKEN`, `BACKUP_TEST_TARGET_TOKEN`
- `BACKUP_TEST_ENVIRONMENT`
- `BACKUP_TEST_SOURCE_STORAGE`, `BACKUP_TEST_BACKUP_STORAGE`,
  `BACKUP_TEST_TARGET_STORAGE`
- `BACKUP_TEST_CREDENTIAL_MODE`

`BACKUP_TEST_BACKUP_FORMAT` defaults to `binlog`. Use `binlog` for routes whose
source and target object stores have different endpoints, because this path can
stream objects through Milvus Backup. Use `snapshot` only when the target
Milvus server can read or server-side copy the exported snapshot bundle.

Timeouts and retention behavior are controlled by
`BACKUP_TEST_BACKUP_TIMEOUT_SECONDS`, `BACKUP_TEST_RESTORE_TIMEOUT_SECONDS`,
`BACKUP_TEST_POLL_INTERVAL_SECONDS`, and
`BACKUP_TEST_KEEP_ARTIFACTS_ON_FAILURE`.

## Run

Create the isolated Python environment from the repository root:

```bash
uv venv -p 3.12 .venv
uv pip install --python .venv/bin/python \
  --index-strategy unsafe-best-match \
  -r system_tests/requirements.txt
source .venv/bin/activate
pytest -c system_tests/pytest.ini system_tests/unit -v
pytest -c system_tests/pytest.ini \
  system_tests/backup_restore/test_collection_restore.py \
  --junitxml=artifacts/junit/backup-restore.xml \
  --alluredir=allure-results
```

Add new storage providers by supplying another environment route. Provider
credentials and Kubernetes manifests must remain outside this repository.
