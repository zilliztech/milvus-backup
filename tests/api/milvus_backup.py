import json
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
    @on(200, lambda r: r.json())
    def get_restore(self):
        """Get restore backup"""


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

