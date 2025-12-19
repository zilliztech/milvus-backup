package conv

import (
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

func TestDefaultValue(t *testing.T) {
	t.Run("HasBase64", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueBase64: base64.StdEncoding.EncodeToString(bytes)}

		val, err := DefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("HasProto", func(t *testing.T) {
		defaultValue := &schemapb.ValueField{Data: &schemapb.ValueField_BoolData{BoolData: true}}
		bytes, err := proto.Marshal(defaultValue)
		assert.NoError(t, err)
		field := &backuppb.FieldSchema{DefaultValueProto: string(bytes)}

		val, err := DefaultValue(field)
		assert.NoError(t, err)
		assert.Equal(t, defaultValue.GetBoolData(), val.GetBoolData())
	})

	t.Run("WithoutDefault", func(t *testing.T) {
		field := &backuppb.FieldSchema{}

		val, err := DefaultValue(field)
		assert.NoError(t, err)
		assert.Nil(t, val)
	})
}

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

func TestGrants(t *testing.T) {
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

	restoreGrants := Grants(bakGrants, curGrants)
	assert.Len(t, restoreGrants, 1)
}

func TestRoles(t *testing.T) {
	bakRoles := []*backuppb.RoleEntity{
		{Name: "role.name"},
		{Name: "role1.name"},
	}

	curRoles := []*milvuspb.RoleEntity{
		{Name: "role.name"},
	}

	restoreRoles := Roles(bakRoles, curRoles)
	assert.Len(t, restoreRoles, 1)
	assert.Equal(t, "role1.name", restoreRoles[0].Name)
}

func TestUsers(t *testing.T) {
	bakUsers := []*backuppb.UserInfo{
		{User: "user.user"},
		{User: "user1.user"},
	}

	curUsers := []*milvuspb.UserInfo{
		{User: "user.user"},
	}

	restoreUsers := Users(bakUsers, curUsers)
	assert.Len(t, restoreUsers, 1)
	assert.Equal(t, "user1.user", restoreUsers[0].User)
}

func TestPrivilegeGroups(t *testing.T) {
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

	restorePG := PrivilegeGroups(bakPG, curPrivilegeGroups)
	assert.Len(t, restorePG, 1)
	assert.Equal(t, "group2", restorePG[0].GroupName)
	assert.Equal(t, 2, len(restorePG[0].Privileges))
	assert.Equal(t, "privilege3", restorePG[0].Privileges[0].Name)
	assert.Equal(t, "privilege4", restorePG[0].Privileges[1].Name)
}
