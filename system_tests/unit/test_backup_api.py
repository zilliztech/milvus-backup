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
