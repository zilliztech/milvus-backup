from __future__ import annotations

import time
from typing import Any

import requests


class BackupApiError(RuntimeError):
    pass


class BackupApi:
    def __init__(
        self,
        api_uri: str,
        *,
        session: Any | None = None,
        request_timeout_seconds: float = 30,
    ):
        self._api_uri = api_uri.rstrip("/")
        self._session = session or requests.Session()
        self._request_timeout_seconds = request_timeout_seconds

    def create_backup_and_wait(
        self,
        *,
        backup_name: str,
        collection_names: list[str],
        request_id: str,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> dict[str, Any]:
        headers = {"request_id": request_id}
        self._request(
            "POST",
            "/create",
            headers=headers,
            json={
                "async": True,
                "backup_name": backup_name,
                "collection_names": collection_names,
            },
        )

        deadline = time.monotonic() + timeout_seconds
        while True:
            response = self._request(
                "GET",
                "/get_backup",
                headers=headers,
                params={"backup_name": backup_name},
            )
            state_code = response.get("data", {}).get("state_code")
            if state_code == 2:
                return response
            if state_code in (3, 4):
                detail = response.get("data", {}).get("errorMessage") or response.get(
                    "msg", "backup failed"
                )
                raise BackupApiError(f"backup {backup_name} failed: {detail}")
            if time.monotonic() >= deadline:
                raise BackupApiError(
                    f"backup {backup_name} timed out after {timeout_seconds:g} seconds"
                )
            time.sleep(poll_interval_seconds)

    def restore_backup_and_wait(
        self,
        *,
        backup_name: str,
        collection_renames: dict[str, str],
        request_id: str,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> dict[str, Any]:
        headers = {"request_id": request_id}
        submitted = self._request(
            "POST",
            "/restore",
            headers=headers,
            json={
                "async": True,
                "backup_name": backup_name,
                "collection_names": list(collection_renames),
                "collection_renames": collection_renames,
                "restoreIndex": True,
            },
        )
        restore_id = submitted["data"]["id"]

        deadline = time.monotonic() + timeout_seconds
        while True:
            response = self._request(
                "GET",
                "/get_restore",
                headers=headers,
                params={"id": restore_id},
            )
            state_code = response.get("data", {}).get("state_code")
            if state_code == 2:
                return response
            if state_code in (3, 4):
                detail = response.get("data", {}).get("errorMessage") or response.get(
                    "msg", "restore failed"
                )
                raise BackupApiError(f"restore {restore_id} failed: {detail}")
            if time.monotonic() >= deadline:
                raise BackupApiError(
                    f"restore {restore_id} timed out after {timeout_seconds:g} seconds"
                )
            time.sleep(poll_interval_seconds)

    def delete_backup(self, *, backup_name: str, request_id: str) -> None:
        self._request(
            "DELETE",
            "/delete",
            headers={"request_id": request_id},
            params={"backup_name": backup_name},
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._session.request(
            method,
            f"{self._api_uri}{path}",
            timeout=self._request_timeout_seconds,
            **kwargs,
        )
        if response.status_code != 200:
            raise BackupApiError(
                f"{method} {path} returned HTTP {response.status_code}: {response.text}"
            )

        body = response.json()
        if "code" in body and body["code"] != 0:
            raise BackupApiError(
                f"{method} {path} failed with code {body.get('code')}: "
                f"{body.get('msg', response.text)}"
            )
        return body
