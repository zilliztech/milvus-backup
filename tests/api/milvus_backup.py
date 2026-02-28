import json
import requests
import time
import uuid
from utils.util_log import test_log as log
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from requests.exceptions import ConnectionError


def logger_request_response(
    response, url, tt, headers, data, str_data, str_response, method
):
    if len(data) > 2000:
        data = data[:1000] + "..." + data[-1000:]
    try:
        if response.status_code == 200:
            if ("code" in response.json() and response.json()["code"] == 200) or (
                "Code" in response.json() and response.json()["Code"] == 0
            ):
                log.debug(
                    f"\nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {str_data}, \nresponse: {str_response}"
                )
            else:
                log.debug(
                    f"\nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}"
                )
        else:
            log.debug(
                f"method: \nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}"
            )
    except Exception as e:
        log.debug(
            f"method: \nmethod: {method}, \nurl: {url}, \ncost time: {tt}, \nheader: {headers}, \npayload: {data}, \nresponse: {response.text}, \nerror: {e}"
        )


class Requests:
    def __init__(self, url=None):
        self.url = url
        self.headers = {
            "Content-Type": "application/json",
            "RequestId": str(uuid.uuid1()),
        }

    def update_headers(self):
        headers = {"Content-Type": "application/json", "RequestId": str(uuid.uuid1())}
        return headers

    # retry when request failed caused by network or server error
    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def post(self, url, headers=None, data=None, params=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + "..." + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.post(url, headers=headers, data=data, params=params)
        tt = time.time() - t0
        str_response = (
            response.text[:200] + "..." + response.text[-200:]
            if len(response.text) > 400
            else response.text
        )
        logger_request_response(
            response, url, tt, headers, data, str_data, str_response, "post"
        )
        return response

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def get(self, url, headers=None, params=None, data=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + "..." + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        if data is None or data == "null":
            response = requests.get(url, headers=headers, params=params)
        else:
            response = requests.get(url, headers=headers, params=params, data=data)
        tt = time.time() - t0
        str_response = (
            response.text[:200] + "..." + response.text[-200:]
            if len(response.text) > 400
            else response.text
        )
        logger_request_response(
            response, url, tt, headers, data, str_data, str_response, "get"
        )
        return response

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def put(self, url, headers=None, data=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + "..." + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.put(url, headers=headers, data=data)
        tt = time.time() - t0
        str_response = (
            response.text[:200] + "..." + response.text[-200:]
            if len(response.text) > 400
            else response.text
        )
        logger_request_response(
            response, url, tt, headers, data, str_data, str_response, "put"
        )
        return response

    @retry(retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(3))
    def delete(self, url, headers=None, data=None, params=None):
        headers = headers if headers is not None else self.update_headers()
        data = json.dumps(data)
        str_data = data[:200] + "..." + data[-200:] if len(data) > 400 else data
        t0 = time.time()
        response = requests.delete(url, headers=headers, data=data, params=params)
        tt = time.time() - t0
        str_response = (
            response.text[:200] + "..." + response.text[-200:]
            if len(response.text) > 400
            else response.text
        )
        logger_request_response(
            response, url, tt, headers, data, str_data, str_response, "delete"
        )
        return response


class MilvusBackupClient(Requests):
    def __init__(self, endpoint):
        super().__init__(url=endpoint)
        self.endpoint = endpoint
        self.headers = self.update_headers()
        self.backup_list = []

    def update_headers(self):
        headers = {"Content-Type": "application/json", "RequestId": str(uuid.uuid1())}
        return headers

    def hello(self):
        url = f"{self.endpoint}/hello"
        response = self.get(url, headers=self.update_headers())
        return response.json()

    def create_backup(self, payload):
        url = f"{self.endpoint}/create"
        response = self.post(url, headers=self.update_headers(), data=payload)
        name = payload.get("backup_name", None)
        if name is not None:
            self.backup_list.append(name)
        return response.json()

    def list_backup(self):
        url = f"{self.endpoint}/list"
        response = self.get(url, headers=self.update_headers())
        return response.json()

    def get_backup(self, backup_name):
        url = f"{self.endpoint}/get_backup"
        response = self.get(
            url, headers=self.update_headers(), params={"backup_name": backup_name}
        )
        return response.json()

    def delete_backup(self, backup_name):
        url = f"{self.endpoint}/delete"
        response = self.delete(
            url, headers=self.update_headers(), params={"backup_name": backup_name}
        )
        return response.json()

    def restore_backup(self, payload):
        url = f"{self.endpoint}/restore"
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def restore_secondary(self, payload):
        url = f"{self.endpoint}/restore_secondary"
        response = self.post(url, headers=self.update_headers(), data=payload)
        return response.json()

    def get_restore(self, id):
        url = f"{self.endpoint}/get_restore"
        response = self.get(url, headers=self.update_headers(), params={"id": id})
        return response.json()

    def wait_create_backup_complete(self, backup_name, timeout=120):
        """wait create backup complete"""
        start_time = time.time()
        end_time = time.time()
        ready = False
        res = self.get_backup(backup_name)
        while not ready and (end_time - start_time < timeout):
            state_code = res["data"].get("state_code", 1)
            if state_code == 1 or state_code == 0:
                time.sleep(5)
                res = self.get_backup(backup_name)
                end_time = time.time()
            elif state_code == 2:
                log.info(
                    f"backup {backup_name} is ready in {end_time - start_time} seconds"
                )
                return True
            else:
                log.error(
                    f"get backup {backup_name} failed in unknown state {state_code}"
                )
                return False
        end_time = time.time()
        log.info(f"backup {backup_name} is timeout in {end_time - start_time} seconds")
        return False

    def wait_restore_complete(self, id, timeout=120):
        """wait restore complete"""
        start_time = time.time()
        end_time = time.time()
        ready = False
        res = self.get_restore(id)
        while not ready and end_time - start_time < timeout:
            state_code = res["data"].get("state_code", 1)
            if state_code == 1 or state_code == 0:
                time.sleep(5)
                res = self.get_restore(id)
                end_time = time.time()
            elif state_code == 2:
                log.info(f"restore {id} is ready in {end_time - start_time} seconds")
                return True
            else:
                log.error(f"get restore {id} failed in unknown state {state_code}")
                return False
        end_time = time.time()
        log.info(f"get restore {id} is timeout in {end_time - start_time} seconds")
        return False


if __name__ == "__main__":
    client = MilvusBackupClient("http://localhost:8080/api/v1")
    res = client.hello()
    print(res)
    # res = client.create_backup({"backup_name": "test_api"})
    print(res)
    # res = client.list_backup()
    # print(res)
    print("restore backup")
    res = client.restore_backup({"backup_name": "test_api"})
    print(res)
