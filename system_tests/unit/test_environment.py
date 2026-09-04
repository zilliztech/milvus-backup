from backup_restore.environment import BackupRestoreEnvironment


def test_environment_loads_endpoints_and_safe_route_metadata():
    environment = BackupRestoreEnvironment.from_mapping(
        {
            "BACKUP_TEST_SOURCE_MILVUS_URI": "http://source-milvus:19530",
            "BACKUP_TEST_TARGET_MILVUS_URI": "http://target-milvus:19530",
            "BACKUP_TEST_BACKUP_API_URI": "http://backup-server:8080",
            "BACKUP_TEST_RESTORE_API_URI": "http://restore-server:8080",
            "BACKUP_TEST_SOURCE_TOKEN": "source-secret",
            "BACKUP_TEST_TARGET_TOKEN": "target-secret",
            "BACKUP_TEST_ENVIRONMENT": "minio-to-minio",
            "BACKUP_TEST_SOURCE_STORAGE": "minio",
            "BACKUP_TEST_BACKUP_STORAGE": "minio",
            "BACKUP_TEST_TARGET_STORAGE": "minio",
            "BACKUP_TEST_CREDENTIAL_MODE": "static",
        }
    )

    assert environment.source.uri == "http://source-milvus:19530"
    assert environment.source.token == "source-secret"
    assert environment.target.uri == "http://target-milvus:19530"
    assert environment.target.token == "target-secret"
    assert environment.backup_api_uri == "http://backup-server:8080/api/v1"
    assert environment.restore_api_uri == "http://restore-server:8080/api/v1"
    assert environment.safe_metadata() == {
        "environment": "minio-to-minio",
        "source_storage": "minio",
        "backup_storage": "minio",
        "target_storage": "minio",
        "credential_mode": "static",
        "source_milvus_uri": "http://source-milvus:19530",
        "target_milvus_uri": "http://target-milvus:19530",
        "backup_api_uri": "http://backup-server:8080/api/v1",
        "restore_api_uri": "http://restore-server:8080/api/v1",
    }
