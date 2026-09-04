from backup_restore.clients.backup_api import BackupApi


class StubResponse:
    def __init__(self, body):
        self._body = body
        self.status_code = 200
        self.text = repr(body)

    def json(self):
        return self._body


class ScriptedSession:
    def __init__(self, responses):
        self._responses = iter(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return StubResponse(next(self._responses))


def test_create_backup_waits_until_the_backup_is_available():
    session = ScriptedSession(
        [
            {"code": 0, "requestId": "backup-request-1"},
            {"code": 0, "data": {"state_code": 1, "progress": 50}},
            {
                "code": 0,
                "data": {
                    "state_code": 2,
                    "name": "backup-1",
                    "collection_backups": [{"collection_name": "source_collection"}],
                },
            },
        ]
    )

    result = BackupApi(
        "http://backup-server:8080/api/v1",
        session=session,
    ).create_backup_and_wait(
        backup_name="backup-1",
        collection_names=["source_collection"],
        request_id="backup-request-1",
        timeout_seconds=10,
        poll_interval_seconds=0,
    )

    assert result["data"]["state_code"] == 2
    assert result["data"]["collection_backups"] == [
        {"collection_name": "source_collection"}
    ]
    assert session.requests == [
        (
            "POST",
            "http://backup-server:8080/api/v1/create",
            {
                "json": {
                    "async": True,
                    "backup_name": "backup-1",
                    "collection_names": ["source_collection"],
                },
                "timeout": 30,
                "headers": {"request_id": "backup-request-1"},
            },
        ),
        (
            "GET",
            "http://backup-server:8080/api/v1/get_backup",
            {
                "params": {"backup_name": "backup-1"},
                "timeout": 30,
                "headers": {"request_id": "backup-request-1"},
            },
        ),
        (
            "GET",
            "http://backup-server:8080/api/v1/get_backup",
            {
                "params": {"backup_name": "backup-1"},
                "timeout": 30,
                "headers": {"request_id": "backup-request-1"},
            },
        ),
    ]


def test_restore_backup_waits_until_the_target_collection_is_available():
    session = ScriptedSession(
        [
            {
                "code": 0,
                "requestId": "restore-request-1",
                "data": {"id": "restore-1", "state_code": 0},
            },
            {
                "code": 0,
                "data": {"id": "restore-1", "state_code": 1, "progress": 70},
            },
            {
                "code": 0,
                "data": {"id": "restore-1", "state_code": 2, "progress": 100},
            },
        ]
    )

    result = BackupApi(
        "http://restore-server:8080/api/v1",
        session=session,
    ).restore_backup_and_wait(
        backup_name="backup-1",
        collection_renames={"source_collection": "restored_collection"},
        request_id="restore-request-1",
        timeout_seconds=10,
        poll_interval_seconds=0,
    )

    assert result["data"]["state_code"] == 2
    assert session.requests == [
        (
            "POST",
            "http://restore-server:8080/api/v1/restore",
            {
                "json": {
                    "async": True,
                    "backup_name": "backup-1",
                    "collection_names": ["source_collection"],
                    "collection_renames": {"source_collection": "restored_collection"},
                    "restoreIndex": True,
                },
                "timeout": 30,
                "headers": {"request_id": "restore-request-1"},
            },
        ),
        (
            "GET",
            "http://restore-server:8080/api/v1/get_restore",
            {
                "params": {"id": "restore-1"},
                "timeout": 30,
                "headers": {"request_id": "restore-request-1"},
            },
        ),
        (
            "GET",
            "http://restore-server:8080/api/v1/get_restore",
            {
                "params": {"id": "restore-1"},
                "timeout": 30,
                "headers": {"request_id": "restore-request-1"},
            },
        ),
    ]


def test_delete_backup_removes_only_the_named_backup():
    session = ScriptedSession([{"code": 0, "msg": "success"}])

    BackupApi(
        "http://backup-server:8080/api/v1",
        session=session,
    ).delete_backup(
        backup_name="backup-1",
        request_id="delete-request-1",
    )

    assert session.requests == [
        (
            "DELETE",
            "http://backup-server:8080/api/v1/delete",
            {
                "params": {"backup_name": "backup-1"},
                "timeout": 30,
                "headers": {"request_id": "delete-request-1"},
            },
        )
    ]
