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

    @staticmethod
    def verify_role_grants(client, role_name, expected_grants):
        """Verify a role has the expected grants restored.

        expected_grants: list of dicts. Common keys are object_name, privilege,
        db_name and object_type. Only keys present in an expected dict are
        compared, so callers can omit fields whose value depends on server-side
        inference (e.g. object_type for grant_privilege_v2).
        """
        actual = client.describe_role(role_name=role_name).get("privileges", [])
        for exp in expected_grants:
            matched = []
            for g in actual:
                ok = True
                for key, want in exp.items():
                    # describe_role omits db_name from the dict when empty,
                    # treat a missing key as empty string for comparison.
                    got = g.get(key, "")
                    if got != want:
                        ok = False
                        break
                if ok:
                    matched.append(g)
            assert matched, (
                f"Grant not restored on role {role_name}: expected={exp}, "
                f"actual={actual}"
            )

    @staticmethod
    def verify_privilege_group_content(client, group_name, expected_privileges):
        """Verify a privilege group exists and contains the expected privileges."""
        groups = client.list_privilege_groups()
        target = next(
            (g for g in groups if g["privilege_group"] == group_name),
            None,
        )
        assert target is not None, (
            f"Privilege group {group_name} not found, got={groups}"
        )
        actual = set(target.get("privileges", ()))
        missing = set(expected_privileges) - actual
        assert not missing, (
            f"Privilege group {group_name} missing privileges {missing}, "
            f"actual={actual}"
        )

    @staticmethod
    def verify_user_roles(client, user_name, expected_roles):
        """Verify a user is bound to the expected roles."""
        info = client.describe_user(user_name=user_name)
        actual = set(info.get("roles", ()))
        missing = set(expected_roles) - actual
        assert not missing, (
            f"User {user_name} missing role bindings {missing}, "
            f"actual={actual}"
        )



    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_backup_with_rbac(
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

        # Verify privilege group content is restored, not just the name
        self.verify_privilege_group_content(
            self.milvus_client,
            group_name=privilege_group_name,
            expected_privileges=['Insert'],
        )

        # Verify the grant on the custom role is restored with original wildcards.
        # object_type is intentionally not asserted because grant_privilege_v2
        # lets the server infer it from the privilege.
        self.verify_role_grants(
            self.milvus_client,
            role_name=role_name,
            expected_grants=[{
                "object_name": "*",
                "privilege": privilege_group_name,
                "db_name": "*",
            }],
        )

        # Verify the user is still bound to the custom role
        self.verify_user_roles(
            self.milvus_client,
            user_name=user_name,
            expected_roles=[role_name],
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_restore_backup_with_wildcard_privilege(
        self,
    ):
        """Regression test for restoring grants with the AnyWord ('*') privilege.

        The Milvus admin role uses '*' as its privilege name (AnyWord, meaning
        "any privilege"), and users may also grant '*' to custom roles. The
        OperatePrivilege code path on the Milvus server handles this correctly,
        but the RestoreRBAC path historically did not, producing either a
        `privilege group [] does not exist` error on 2.4.x / 2.5.x or silently
        storing it as `PrivilegeGroup*` on master. This test exercises the
        broken path: it grants '*' on a custom role, backs it up, drops the
        role so the dedup in conv.Grants cannot hide the grant, then restores
        and asserts the wildcard grant comes back exactly as written.
        """

        # Setup: create user + role and grant '*' directly to the role
        user_name = cf.gen_unique_str("user_")
        self.milvus_client.create_user(user_name=user_name, password="P@ssw0rd")
        role_name = cf.gen_unique_str("role_")
        self.milvus_client.create_role(role_name=role_name)
        self.milvus_client.grant_privilege_v2(
            role_name=role_name,
            privilege='*',
            collection_name='*',
            db_name='*',
        )
        self.milvus_client.grant_role(user_name=user_name, role_name=role_name)
        self.users.extend([user_name])
        self.roles.extend([role_name])

        # Sanity check: confirm the grant landed in the source cluster before
        # we attempt to back it up. If the server starts rejecting '*' on
        # OperatePrivilege in the future, this assertion turns the failure
        # into a clear signal instead of a confusing restore-time error.
        self.verify_role_grants(
            self.milvus_client,
            role_name=role_name,
            expected_grants=[{
                "object_name": "*",
                "privilege": "*",
                "db_name": "*",
            }],
        )

        # Create a throwaway collection so the backup payload has something
        # to point at; the test only cares about the RBAC half.
        collection_name = f"{prefix}_{cf.gen_unique_str()}"
        self.collection_names.extend([collection_name, collection_name + suffix])
        dim = 8
        schema: CollectionSchema = MilvusClient.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
        )
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.milvus_client.create_collection(collection_name=collection_name, schema=schema)

        # Create backup with rbac=true
        backup_name = f"{backup_prefix}_{cf.gen_unique_str()}"
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_names": [collection_name],
            "rbac": True,
        }
        res = self.client.create_backup(payload)
        log.info(f"create backup response: {res}")
        assert res["msg"] == "success"

        # Drop the custom role/user so the restore path actually has to
        # re-insert the '*' grant (otherwise conv.Grants would dedup it
        # against the still-existing one and skip the broken path entirely).
        self.clean_rbac(self.milvus_client)
        log.info("List RBAC after drop and before restore, expected to be only build in parts")
        self.list_rbac(self.milvus_client)

        # Restore backup
        payload = {
            "async": False,
            "backup_name": backup_name,
            "collection_suffix": suffix,
            "useV2Restore": True,
            "rbac": True,
        }
        t0 = time.time()
        res = self.client.restore_backup(payload)
        log.info(f"restore_backup: {res}")
        t1 = time.time()
        log.info(f"restore cost time: {t1 - t0}")
        assert res["msg"] == "success"

        # Verify the wildcard grant survived backup -> drop -> restore.
        # Two failure modes this catches:
        #   1. Loud failure: the restore call above already returned non-success
        #      because RestoreRBAC raised `privilege group [] does not exist`.
        #   2. Silent corruption: restore claimed success but the grant got
        #      written as `PrivilegeGroup*` instead of `*`, in which case
        #      describe_role will return something other than '*' here.
        log.info("list RBAC after restore, expected to be the same as before drop")
        self.list_rbac(self.milvus_client)
        self.verify_rbac_restore(self.milvus_client, [user_name], [role_name], [])
        self.verify_role_grants(
            self.milvus_client,
            role_name=role_name,
            expected_grants=[{
                "object_name": "*",
                "privilege": "*",
                "db_name": "*",
            }],
        )
        self.verify_user_roles(
            self.milvus_client,
            user_name=user_name,
            expected_roles=[role_name],
        )

