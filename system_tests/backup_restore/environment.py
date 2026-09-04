from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

_REQUIRED_VALUES = (
    "BACKUP_TEST_SOURCE_MILVUS_URI",
    "BACKUP_TEST_TARGET_MILVUS_URI",
    "BACKUP_TEST_BACKUP_API_URI",
    "BACKUP_TEST_RESTORE_API_URI",
    "BACKUP_TEST_SOURCE_TOKEN",
    "BACKUP_TEST_TARGET_TOKEN",
    "BACKUP_TEST_ENVIRONMENT",
    "BACKUP_TEST_SOURCE_STORAGE",
    "BACKUP_TEST_BACKUP_STORAGE",
    "BACKUP_TEST_TARGET_STORAGE",
    "BACKUP_TEST_CREDENTIAL_MODE",
)


class EnvironmentConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class MilvusEndpoint:
    uri: str
    token: str


@dataclass(frozen=True)
class BackupRestoreEnvironment:
    source: MilvusEndpoint
    target: MilvusEndpoint
    backup_api_uri: str
    restore_api_uri: str
    name: str
    source_storage: str
    backup_storage: str
    target_storage: str
    credential_mode: str
    backup_timeout_seconds: float
    restore_timeout_seconds: float
    poll_interval_seconds: float
    keep_artifacts_on_failure: bool

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> BackupRestoreEnvironment:
        missing = [name for name in _REQUIRED_VALUES if not values.get(name)]
        if missing:
            raise EnvironmentConfigurationError(
                f"missing backup restore test environment values: {', '.join(missing)}"
            )

        return cls(
            source=MilvusEndpoint(
                uri=values["BACKUP_TEST_SOURCE_MILVUS_URI"],
                token=values["BACKUP_TEST_SOURCE_TOKEN"],
            ),
            target=MilvusEndpoint(
                uri=values["BACKUP_TEST_TARGET_MILVUS_URI"],
                token=values["BACKUP_TEST_TARGET_TOKEN"],
            ),
            backup_api_uri=_normalize_api_uri(values["BACKUP_TEST_BACKUP_API_URI"]),
            restore_api_uri=_normalize_api_uri(values["BACKUP_TEST_RESTORE_API_URI"]),
            name=values["BACKUP_TEST_ENVIRONMENT"],
            source_storage=values["BACKUP_TEST_SOURCE_STORAGE"],
            backup_storage=values["BACKUP_TEST_BACKUP_STORAGE"],
            target_storage=values["BACKUP_TEST_TARGET_STORAGE"],
            credential_mode=values["BACKUP_TEST_CREDENTIAL_MODE"],
            backup_timeout_seconds=float(
                values.get("BACKUP_TEST_BACKUP_TIMEOUT_SECONDS", "900")
            ),
            restore_timeout_seconds=float(
                values.get("BACKUP_TEST_RESTORE_TIMEOUT_SECONDS", "1800")
            ),
            poll_interval_seconds=float(
                values.get("BACKUP_TEST_POLL_INTERVAL_SECONDS", "5")
            ),
            keep_artifacts_on_failure=values.get(
                "BACKUP_TEST_KEEP_ARTIFACTS_ON_FAILURE", "false"
            ).lower()
            == "true",
        )

    def safe_metadata(self) -> dict[str, str]:
        return {
            "environment": self.name,
            "source_storage": self.source_storage,
            "backup_storage": self.backup_storage,
            "target_storage": self.target_storage,
            "credential_mode": self.credential_mode,
            "source_milvus_uri": self.source.uri,
            "target_milvus_uri": self.target.uri,
            "backup_api_uri": self.backup_api_uri,
            "restore_api_uri": self.restore_api_uri,
        }


def _normalize_api_uri(uri: str) -> str:
    normalized = uri.rstrip("/")
    return normalized if normalized.endswith("/api/v1") else f"{normalized}/api/v1"
