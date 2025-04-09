import time
import pytest
import random
from pymilvus import MilvusClient, DataType, CollectionSchema
from pymilvus.exceptions import MilvusException
from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from utils.util_log import test_log as log

prefix = "restore_backup"
backup_prefix = "backup"
suffix = "_bak"


@pytest.mark.tags(CaseLabel.RBAC)
class TestRestoreBackupWithRbac(TestcaseBase):
    """Test cases for RBAC backup and restore functionality"""

    def setup_method(self, method):
        """Setup method that runs before each test"""
        self.collection_names = []
        self.users = []
        self.roles = []
        self.privilege_groups = []

    def teardown_method(self, method):
        """Cleanup method that runs after each test"""
        # Clean up collections
        log.info(f"clean roles")
        self.clean_rbac(self.milvus_client)



    @staticmethod
    def setup_rbac_environment(client: MilvusClient):
        """Setup RBAC environment with users, roles, and privileges"""
        user_name = cf.gen_unique_str("user_")
        client.create_user(user_name=user_name, password="P@ssw0rd")
        role_name = cf.gen_unique_str("role_")
        client.create_role(role_name=role_name)
        privilege_group_name = cf.gen_unique_str("privilege_group_")
        client.create_privilege_group(group_name=privilege_group_name)
        client.add_privileges_to_group(group_name=privilege_group_name, privileges=['Insert'])
        client.grant_privilege_v2(
            role_name=role_name,
            privilege=privilege_group_name,
            collection_name = '*',
            db_name = '*',
        )
        client.grant_role(user_name=user_name, role_name=role_name)
        return user_name, role_name, privilege_group_name

    @staticmethod
    def clean_rbac(client: MilvusClient):
        build_in_role = ['admin', 'db_ro', 'db_rw', 'public']
        build_in_user = ['root']
        for role in client.list_roles():
            if role in build_in_role:
                continue
            privileges = client.describe_role(role_name=role)["privileges"]
            for privilege in privileges:
                client.revoke_privilege_v2(
                    **privilege,
                    collection_name='*',
                )
            client.drop_role(role)

        for pg in client.list_privilege_groups():
            client.drop_privilege_group(group_name=pg["privilege_group"])
        for user in client.list_users():
            if user in build_in_user:
                continue
            client.drop_user(user_name=user)

    @staticmethod
    def list_rbac(client: MilvusClient):
        for role in client.list_roles():
            log.info(f"role: {role}")

        for pg in client.list_privilege_groups():
            log.info(f"privilege group: {pg}")
        for user in client.list_users():
            log.info(f"user: {user}")

    @staticmethod
    def verify_rbac_restore(client, users, roles, privilege_groups):
        """Verify RBAC configuration after restore"""
        # Verify users
        restored_users = client.list_users()
        for user in users:
            assert user in restored_users, f"User {user} not restored"

        # Verify roles
        restored_roles = client.list_roles()
        for role in roles:
            assert role in restored_roles, f"Role {role} not restored"

        # Verify privilege groups
        for group in privilege_groups:
            group_info = client.list_privilege_groups()
            privilege_list = [pg["privilege_group"] for pg in group_info]
            assert group in privilege_list, \
                f"Privilege group {group} not restored"



    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_back_with_rbac(
        self,
    ):
        """Test backup and restore with RBAC configuration"""

        # Setup RBAC environment
        user_name, role_name, privilege_group_name = self.setup_rbac_environment(self.milvus_client)
        self.users.extend([user_name])
        self.roles.extend([role_name])
        self.privilege_groups.extend([privilege_group_name])

        # Create a simple collection for testing
        collection_name = f"{prefix}_{cf.gen_unique_str()}"
        self.collection_names.extend([collection_name, collection_name + suffix])
        dim = 8
        nb = 100
        schema: CollectionSchema = MilvusClient.create_schema(
            auto_id=False,
            enable_dynamic_field=False
        )
        schema.add_field(
            field_name="id",
            datatype=DataType.INT64,
            is_primary=True
        )
        schema.add_field(
            field_name="vector",
            datatype=DataType.FLOAT_VECTOR,
            dim=dim
        )

        self.milvus_client.create_collection(collection_name=collection_name, schema=schema)
        # Create backup
        backup_name = f"{backup_prefix}_{cf.gen_unique_str()}"
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
            "rbac": True
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        assert res["msg"] == "success"

        # Drop RBAC
        self.clean_rbac(self.milvus_client)
        # List RBAC
        log.info("List RBAC after drop and before restore, expected to be only build in parts")
        self.list_rbac(self.milvus_client)

        # Restore backup
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": suffix,
            "useV2Restore": True,
            "rbac": True
        }
        t0 = time.time()
        res = self.client.restore_backup(payload)
        log.info(f"restore_backup: {res}")
        t1 = time.time()
        log.info(f"restore cost time: {t1 - t0}")
        assert res["msg"] == "success"

        # Verify RBAC configuration
        log.info("list RBAC after restore, expected to be the same as before drop")
        self.list_rbac(self.milvus_client)
        self.verify_rbac_restore(self.milvus_client, [user_name], [role_name], [privilege_group_name])

