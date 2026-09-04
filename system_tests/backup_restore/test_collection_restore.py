from uuid import uuid4

import allure
import pytest

from backup_restore.allure_evidence import attach_json, record_environment
from backup_restore.datasets.default_collection import prepare_default_collection
from backup_restore.verification.collection import assert_collection_restored


@allure.epic("Milvus Backup")
@allure.feature("Cross-storage restore")
@pytest.mark.system
def test_collection_can_be_restored_to_another_milvus(
    backup_restore_environment,
    source_milvus,
    target_milvus,
    backup_api,
    restore_api,
):
    run_id = uuid4().hex[:12]
    source_collection = f"backup_e2e_source_{run_id}"
    target_collection = f"backup_e2e_target_{run_id}"
    backup_name = f"backup_e2e_{run_id}"
    record_environment(backup_restore_environment)

    with allure.step("Record source and target Milvus versions"):
        source_version = source_milvus.get_server_version(detail=True)
        target_version = target_milvus.get_server_version(detail=True)
        attach_json("source Milvus version", source_version)
        attach_json("target Milvus version", target_version)
        allure.dynamic.parameter("source_milvus_commit", source_version["git_commit"])
        allure.dynamic.parameter("target_milvus_commit", target_version["git_commit"])

    with allure.step("Prepare source collection"):
        dataset = prepare_default_collection(
            source_milvus,
            collection_name=source_collection,
            row_count=1000,
            dimension=8,
        )

    with allure.step("Create backup and wait for completion"):
        backup = backup_api.create_backup_and_wait(
            backup_name=backup_name,
            collection_names=[source_collection],
            backup_format=backup_restore_environment.backup_format,
            request_id=f"backup-{run_id}",
            timeout_seconds=backup_restore_environment.backup_timeout_seconds,
            poll_interval_seconds=backup_restore_environment.poll_interval_seconds,
        )
        attach_json("completed backup", backup)

    with allure.step("Restore backup to target Milvus"):
        restore = restore_api.restore_backup_and_wait(
            backup_name=backup_name,
            collection_renames={source_collection: target_collection},
            request_id=f"restore-{run_id}",
            timeout_seconds=backup_restore_environment.restore_timeout_seconds,
            poll_interval_seconds=backup_restore_environment.poll_interval_seconds,
        )
        attach_json("completed restore", restore)

    with allure.step("Verify restored collection"):
        assert_collection_restored(
            source=source_milvus,
            target=target_milvus,
            source_collection=source_collection,
            target_collection=target_collection,
            query_vector=dataset.rows[0]["vector"],
            expected_row_count=len(dataset.rows),
        )
