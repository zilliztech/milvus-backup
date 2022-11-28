import json
import time
from utils.util_log import test_log as log
from decorest import GET, POST, DELETE
from decorest import HttpStatus, RestClient
from decorest import accept, body, content, endpoint, form
from decorest import header, multipart, on, query, stream, timeout


class MilvusBackupClient(RestClient):

    @GET("hello")
    @on(200, lambda r: r.json())
    def hello(self):
        """test the service is active"""

    @POST("create")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def create_backup(self, payload):
        """Create a backup for the cluster"""

    @GET("list")
    @on(200, lambda r: r.json())
    def list_backup(self):
        """List all backups"""
        """list backups"""

    @GET("get_backup")
    @query("backup_name")
    @on(200, lambda r: r.json())
    def get_backup(self, backup_name):
        """Get a backup"""

    @DELETE("delete")
    @query("backup_name")
    @on(200, lambda r: r.json())
    def delete_backup(self, backup_name):
        """Delete a backup"""

    @POST("restore")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def restore_backup(self, payload):
        """Restore a backup"""

    @GET("get_restore")
    @query("id")
    @on(200, lambda r: r.json())
    def get_restore(self, id):
        """Get restore backup"""

    def wait_create_backup_complete(self, backup_name, timeout=120):
        """wait create backup complete"""
        start_time = time.time()
        end_time = time.time()
        ready = False
        res = self.get_backup(backup_name)
        while not ready and end_time - start_time < timeout:
            state_code = res["data"].get("state_code", 1)
            if state_code == 1:
                time.sleep(5)
                res = self.get_backup(backup_name)
                end_time = time.time()
            elif state_code == 2:
                log.info(f"backup {backup_name} is ready in {end_time - start_time} seconds")
                return True
            else:
                log.error(f"get backup {backup_name} failed in unknown state {state_code}")
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
            if state_code == 1:
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

