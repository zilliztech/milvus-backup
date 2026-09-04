import pytest
from backup_restore.environment import (
    BackupRestoreEnvironment,
    EnvironmentConfigurationError,
)


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
            "BACKUP_TEST_BACKUP_TIMEOUT_SECONDS": "120",
            "BACKUP_TEST_RESTORE_TIMEOUT_SECONDS": "240",
            "BACKUP_TEST_POLL_INTERVAL_SECONDS": "2.5",
            "BACKUP_TEST_KEEP_ARTIFACTS_ON_FAILURE": "true",
        }
    )

    assert environment.source.uri == "http://source-milvus:19530"
    assert environment.source.token == "source-secret"
    assert environment.target.uri == "http://target-milvus:19530"
    assert environment.target.token == "target-secret"
    assert environment.backup_api_uri == "http://backup-server:8080/api/v1"
    assert environment.restore_api_uri == "http://restore-server:8080/api/v1"
    assert environment.backup_timeout_seconds == 120
    assert environment.restore_timeout_seconds == 240
    assert environment.poll_interval_seconds == 2.5
    assert environment.keep_artifacts_on_failure is True
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


def test_environment_reports_all_missing_values_without_exposing_tokens():
    with pytest.raises(EnvironmentConfigurationError) as error:
        BackupRestoreEnvironment.from_mapping(
            {
                "BACKUP_TEST_SOURCE_MILVUS_URI": "http://source-milvus:19530",
                "BACKUP_TEST_SOURCE_TOKEN": "must-not-appear",
            }
        )

    message = str(error.value)
    assert "BACKUP_TEST_TARGET_MILVUS_URI" in message
    assert "BACKUP_TEST_TARGET_TOKEN" in message
    assert "BACKUP_TEST_RESTORE_API_URI" in message
    assert "must-not-appear" not in message
