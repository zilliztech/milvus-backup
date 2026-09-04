import json
from typing import Any

import allure

from backup_restore.environment import BackupRestoreEnvironment


def record_environment(environment: BackupRestoreEnvironment) -> None:
    metadata = environment.safe_metadata()
    allure.dynamic.parent_suite("Milvus Backup system tests")
    allure.dynamic.suite("Backup and restore")
    allure.dynamic.sub_suite(environment.name)
    for name, value in metadata.items():
        allure.dynamic.parameter(name, value)
    attach_json("backup restore environment", metadata)


def attach_json(name: str, value: Any) -> None:
    allure.attach(
        json.dumps(value, indent=2, sort_keys=True, default=str),
        name=name,
        attachment_type=allure.attachment_type.JSON,
    )
