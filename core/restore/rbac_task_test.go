package restore

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestBackupGrantKey(t *testing.T) {
	grant := &backuppb.GrantEntity{
		Role:       &backuppb.RoleEntity{Name: "role.name"},
		Object:     &backuppb.ObjectEntity{Name: "object.name"},
		ObjectName: "objectName",
		Grantor: &backuppb.GrantorEntity{
			User:      &backuppb.UserEntity{Name: "grantor.user.name"},
			Privilege: &backuppb.PrivilegeEntity{Name: "grantor.privilege.name"},
		},
		DbName: "dbName",
	}

	grantKey := backupGrantKey(grant)
	expected := "object.name/objectName/role.name/grantor.user.name/grantor.privilege.name/dbName"
	assert.Equal(t, expected, grantKey)
}

func TestMilvusGrantKey(t *testing.T) {
	grant := &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "role.name"},
		Object:     &milvuspb.ObjectEntity{Name: "object.name"},
		ObjectName: "objectName",
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: "grantor.user.name"},
			Privilege: &milvuspb.PrivilegeEntity{Name: "grantor.privilege.name"},
		},
		DbName: "dbName",
	}

	grantKey := milvusGrantKey(grant)
	expected := "object.name/objectName/role.name/grantor.user.name/grantor.privilege.name/dbName"
	assert.Equal(t, expected, grantKey)
}

func TestRBACTask_Grants(t *testing.T) {
	bakGrants := []*backuppb.GrantEntity{
		{
			Role:       &backuppb.RoleEntity{Name: "role.name"},
			Object:     &backuppb.ObjectEntity{Name: "object.name"},
			ObjectName: "objectName",
			Grantor: &backuppb.GrantorEntity{
				User:      &backuppb.UserEntity{Name: "grantor.user.name"},
				Privilege: &backuppb.PrivilegeEntity{Name: "grantor.privilege.name"},
			},
			DbName: "dbName",
		},
		{
			Role:       &backuppb.RoleEntity{Name: "role1.name"}, // different role
			Object:     &backuppb.ObjectEntity{Name: "object.name"},
			ObjectName: "objectName",
			Grantor: &backuppb.GrantorEntity{
				User:      &backuppb.UserEntity{Name: "grantor.user.name"},
				Privilege: &backuppb.PrivilegeEntity{Name: "grantor.privilege.name"},
			},
			DbName: "dbName",
		},
	}

	curGrants := []*milvuspb.GrantEntity{
		{
			Role:       &milvuspb.RoleEntity{Name: "role.name"},
			Object:     &milvuspb.ObjectEntity{Name: "object.name"},
			ObjectName: "objectName",
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "grantor.user.name"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "grantor.privilege.name"},
			},
			DbName: "dbName",
		},
	}

	rt := &RBACTask{bakRBAC: &backuppb.RBACMeta{Grants: bakGrants}}
	restoreGrants := rt.grants(curGrants)
	assert.Len(t, restoreGrants, 1)
}

func TestRBACTask_Roles(t *testing.T) {
	bakRoles := []*backuppb.RoleEntity{
		{Name: "role.name"},
		{Name: "role1.name"},
	}

	curRoles := []*milvuspb.RoleEntity{
		{Name: "role.name"},
	}

	rt := &RBACTask{bakRBAC: &backuppb.RBACMeta{Roles: bakRoles}}
	restoreRoles := rt.roles(curRoles)
	assert.Len(t, restoreRoles, 1)
	assert.Equal(t, "role1.name", restoreRoles[0].Name)
}

func TestRBACTask_Users(t *testing.T) {
	bakUsers := []*backuppb.UserInfo{
		{User: "user.user"},
		{User: "user1.user"},
	}

	curUsers := []*milvuspb.UserInfo{
		{User: "user.user"},
	}

	rt := &RBACTask{bakRBAC: &backuppb.RBACMeta{Users: bakUsers}}
	restoreUsers := rt.users(curUsers)
	assert.Len(t, restoreUsers, 1)
	assert.Equal(t, "user1.user", restoreUsers[0].User)
}

func TestRBACTask_PrivilegeGroups(t *testing.T) {
	bakPG := []*backuppb.PrivilegeGroupInfo{{
		GroupName: "group1",
		Privileges: []*backuppb.PrivilegeEntity{
			{Name: "privilege1"},
			{Name: "privilege2"},
		},
	}, {
		GroupName: "group2",
		Privileges: []*backuppb.PrivilegeEntity{
			{Name: "privilege3"},
			{Name: "privilege4"},
		},
	}}

	curPrivilegeGroups := []*milvuspb.PrivilegeGroupInfo{{
		GroupName: "group1",
		Privileges: []*milvuspb.PrivilegeEntity{
			{Name: "privilege1"},
			{Name: "privilege2"},
		},
	}}

	rt := &RBACTask{bakRBAC: &backuppb.RBACMeta{PrivilegeGroups: bakPG}}
	restorePG := rt.privilegeGroups(curPrivilegeGroups)
	assert.Len(t, restorePG, 1)
	assert.Equal(t, "group2", restorePG[0].GroupName)
	assert.Equal(t, 2, len(restorePG[0].Privileges))
	assert.Equal(t, "privilege3", restorePG[0].Privileges[0].Name)
	assert.Equal(t, "privilege4", restorePG[0].Privileges[1].Name)
}
