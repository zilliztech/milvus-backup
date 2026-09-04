import os

import pytest
from pymilvus import MilvusClient

from backup_restore.clients.backup_api import BackupApi
from backup_restore.environment import (
    BackupRestoreEnvironment,
    EnvironmentConfigurationError,
)


@pytest.fixture(scope="session")
def backup_restore_environment() -> BackupRestoreEnvironment:
    try:
        return BackupRestoreEnvironment.from_mapping(os.environ)
    except EnvironmentConfigurationError as error:
        raise pytest.UsageError(str(error)) from error


@pytest.fixture
def source_milvus(backup_restore_environment):
    client = MilvusClient(
        uri=backup_restore_environment.source.uri,
        token=backup_restore_environment.source.token,
    )
    yield client
    client.close()


@pytest.fixture
def target_milvus(backup_restore_environment):
    client = MilvusClient(
        uri=backup_restore_environment.target.uri,
        token=backup_restore_environment.target.token,
    )
    yield client
    client.close()


@pytest.fixture
def backup_api(backup_restore_environment):
    return BackupApi(backup_restore_environment.backup_api_uri)


@pytest.fixture
def restore_api(backup_restore_environment):
    return BackupApi(backup_restore_environment.restore_api_uri)
