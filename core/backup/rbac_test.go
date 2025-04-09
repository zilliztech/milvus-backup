package backup

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestRBACTask_Users(t *testing.T) {
	curUser := []*milvuspb.UserInfo{{
		User:     "user1",
		Password: "password1",
		Roles:    []*milvuspb.RoleEntity{{Name: "role1"}},
	}, {
		User:     "user2",
		Password: "password2",
		Roles:    []*milvuspb.RoleEntity{{Name: "role2"}},
	}}

	rt := &RBACTask{}
	bakUsers := rt.users(curUser)

	expect := []*backuppb.UserInfo{{
		User:     "user1",
		Password: "password1",
		Roles:    []*backuppb.RoleEntity{{Name: "role1"}},
	}, {
		User:     "user2",
		Password: "password2",
		Roles:    []*backuppb.RoleEntity{{Name: "role2"}},
	}}

	assert.Len(t, bakUsers, len(expect))
	assert.ElementsMatch(t, bakUsers, expect)
}

func TestRBACTask_Roles(t *testing.T) {
	curRole := []*milvuspb.RoleEntity{{Name: "role1"}, {Name: "role2"}}

	rt := &RBACTask{}
	bakRoles := rt.roles(curRole)

	expect := []*backuppb.RoleEntity{{Name: "role1"}, {Name: "role2"}}

	assert.Len(t, bakRoles, len(expect))
	assert.ElementsMatch(t, bakRoles, expect)
}

func TestRBACTask_Grants(t *testing.T) {
	curGrants := []*milvuspb.GrantEntity{{
		Role:       &milvuspb.RoleEntity{Name: "role.name"},
		Object:     &milvuspb.ObjectEntity{Name: "object.name"},
		ObjectName: "objectName",
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: "grantor.user.name"},
			Privilege: &milvuspb.PrivilegeEntity{Name: "grantor.privilege.name"},
		},
		DbName: "dbName",
	}, {
		Role:       &milvuspb.RoleEntity{Name: "role1.name"},
		Object:     &milvuspb.ObjectEntity{Name: "object1.name"},
		ObjectName: "objectName1",
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: "grantor1.user.name"},
			Privilege: &milvuspb.PrivilegeEntity{Name: "grantor1.privilege.name"},
		},
		DbName: "dbName1",
	}}

	rt := &RBACTask{}
	bakGrants := rt.grants(curGrants)

	expect := []*backuppb.GrantEntity{{
		Role:       &backuppb.RoleEntity{Name: "role.name"},
		Object:     &backuppb.ObjectEntity{Name: "object.name"},
		ObjectName: "objectName",
		Grantor: &backuppb.GrantorEntity{
			User:      &backuppb.UserEntity{Name: "grantor.user.name"},
			Privilege: &backuppb.PrivilegeEntity{Name: "grantor.privilege.name"},
		},
		DbName: "dbName",
	}, {
		Role:       &backuppb.RoleEntity{Name: "role1.name"},
		Object:     &backuppb.ObjectEntity{Name: "object1.name"},
		ObjectName: "objectName1",
		Grantor: &backuppb.GrantorEntity{
			User:      &backuppb.UserEntity{Name: "grantor1.user.name"},
			Privilege: &backuppb.PrivilegeEntity{Name: "grantor1.privilege.name"},
		},
		DbName: "dbName1",
	}}

	assert.Len(t, bakGrants, len(expect))
	assert.ElementsMatch(t, bakGrants, expect)
}

func TestRBACTask_PrivilegeGroups(t *testing.T) {
	curPG := []*milvuspb.PrivilegeGroupInfo{{
		GroupName: "group1",
		Privileges: []*milvuspb.PrivilegeEntity{
			{Name: "privilege1"},
			{Name: "privilege2"},
		},
	}, {
		GroupName: "group2",
		Privileges: []*milvuspb.PrivilegeEntity{
			{Name: "privilege3"},
			{Name: "privilege4"},
		},
	}}

	rt := &RBACTask{}
	bakPG := rt.privilegeGroups(curPG)

	expect := []*backuppb.PrivilegeGroupInfo{{
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

	assert.Len(t, bakPG, len(expect))
	assert.ElementsMatch(t, bakPG, expect)
}
