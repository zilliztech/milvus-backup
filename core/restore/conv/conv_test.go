package conv

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestApplyStructNullable(t *testing.T) {
	t.Run("StructNullablePropagatesToSubFields", func(t *testing.T) {
		bak := &backuppb.StructArrayFieldSchema{
			Nullable: true,
			Fields: []*backuppb.FieldSchema{
				{Name: "a", Nullable: true},
				{Name: "b", Nullable: false},
			},
		}
		fields := []*schemapb.FieldSchema{
			{Name: "a", Nullable: true},
			{Name: "b", Nullable: false},
		}

		got := ApplyStructNullable(bak, fields)
		assert.True(t, got)
		assert.True(t, fields[0].GetNullable())
		assert.True(t, fields[1].GetNullable())
	})

	t.Run("NullableSubFieldRepairsLegacyBackup", func(t *testing.T) {
		// older backups kept the propagated sub-field bits but lost the
		// struct-level flag during conversion
		bak := &backuppb.StructArrayFieldSchema{
			Fields: []*backuppb.FieldSchema{
				{Name: "a", Nullable: true},
				{Name: "b", Nullable: true},
			},
		}
		fields := []*schemapb.FieldSchema{
			{Name: "a", Nullable: true},
			{Name: "b", Nullable: true},
		}

		got := ApplyStructNullable(bak, fields)
		assert.True(t, got)
		assert.True(t, fields[0].GetNullable())
		assert.True(t, fields[1].GetNullable())
	})

	t.Run("NotNullableKeepsSubFieldsUntouched", func(t *testing.T) {
		bak := &backuppb.StructArrayFieldSchema{
			Fields: []*backuppb.FieldSchema{
				{Name: "a"},
				{Name: "b"},
			},
		}
		fields := []*schemapb.FieldSchema{{Name: "a"}, {Name: "b"}}

		got := ApplyStructNullable(bak, fields)
		assert.False(t, got)
		assert.False(t, fields[0].GetNullable())
		assert.False(t, fields[1].GetNullable())
	})
}

func TestStructArrayFields(t *testing.T) {
	t.Run("CarriesStructLevelNullable", func(t *testing.T) {
		bakFields := []*backuppb.StructArrayFieldSchema{{
			Name:     "clips",
			Nullable: true,
			Fields:   []*backuppb.FieldSchema{{Name: "score"}},
		}}

		structs, err := StructArrayFields(bakFields)
		require.NoError(t, err)
		require.Len(t, structs, 1)
		assert.True(t, structs[0].GetNullable())
		assert.Len(t, structs[0].GetFields(), 1)
	})

	// Backups written before StructArrayFieldSchema gained a nullable field
	// carry the server-propagated child flags without any struct-level bit,
	// and internal/meta loads them via encoding/json so a missing key reads
	// back as Nullable=false.
	t.Run("RepairsLegacyMetaLoadedFromJson", func(t *testing.T) {
		const legacyMeta = `{
			"name": "demo",
			"struct_array_fields": [{
				"fieldID": 200,
				"name": "clips",
				"fields": [
					{"name": "clips[score]", "nullable": true},
					{"name": "clips[label]", "nullable": true}
				]
			}]
		}`
		var bakSchema backuppb.CollectionSchema
		require.NoError(t, json.Unmarshal([]byte(legacyMeta), &bakSchema))
		bakStructs := bakSchema.GetStructArrayFields()
		require.Len(t, bakStructs, 1)
		require.False(t, bakStructs[0].GetNullable())

		structs, err := StructArrayFields(bakSchema.GetStructArrayFields())
		require.NoError(t, err)
		require.Len(t, structs, 1)
		assert.True(t, structs[0].GetNullable())
		for _, f := range structs[0].GetFields() {
			assert.True(t, f.GetNullable(), "sub-field %s", f.GetName())
		}
	})

	t.Run("LeavesLegacyNonNullableMetaAsIs", func(t *testing.T) {
		const legacyMeta = `{
			"name": "demo",
			"struct_array_fields": [{
				"fieldID": 201,
				"name": "clips",
				"fields": [{"name": "clips[score]"}]
			}]
		}`
		var bakSchema backuppb.CollectionSchema
		require.NoError(t, json.Unmarshal([]byte(legacyMeta), &bakSchema))

		structs, err := StructArrayFields(bakSchema.GetStructArrayFields())
		require.NoError(t, err)
		require.Len(t, structs, 1)
		assert.False(t, structs[0].GetNullable())
		assert.False(t, structs[0].GetFields()[0].GetNullable())
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
