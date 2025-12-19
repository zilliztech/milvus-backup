package conv

import (
	"encoding/base64"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
)

func DefaultValue(field *backuppb.FieldSchema) (*schemapb.ValueField, error) {
	// try to use DefaultValueBase64 first
	if field.GetDefaultValueBase64() != "" {
		bytes, err := base64.StdEncoding.DecodeString(field.GetDefaultValueBase64())
		if err != nil {
			return nil, fmt.Errorf("restore: failed to decode default value base64: %w", err)
		}
		var defaultValue schemapb.ValueField
		if err := proto.Unmarshal(bytes, &defaultValue); err != nil {
			return nil, fmt.Errorf("restore: failed to unmarshal default value: %w", err)
		}
		return &defaultValue, nil
	}

	// backward compatibility
	if field.GetDefaultValueProto() != "" {
		var defaultValue schemapb.ValueField
		err := proto.Unmarshal([]byte(field.GetDefaultValueProto()), &defaultValue)
		if err != nil {
			return nil, fmt.Errorf("restore: failed to unmarshal default value: %w", err)
		}
		return &defaultValue, nil
	}

	return nil, nil
}

func Functions(bakFuncs []*backuppb.FunctionSchema) []*schemapb.FunctionSchema {
	funcs := make([]*schemapb.FunctionSchema, 0, len(bakFuncs))
	for _, bakFunc := range bakFuncs {
		fun := &schemapb.FunctionSchema{
			Name:             bakFunc.GetName(),
			Id:               bakFunc.GetId(),
			Description:      bakFunc.GetDescription(),
			Type:             schemapb.FunctionType(bakFunc.GetType()),
			InputFieldNames:  bakFunc.GetInputFieldNames(),
			InputFieldIds:    bakFunc.GetInputFieldIds(),
			OutputFieldNames: bakFunc.GetOutputFieldNames(),
			OutputFieldIds:   bakFunc.GetOutputFieldIds(),
			Params:           pbconv.BakKVToMilvusKV(bakFunc.GetParams()),
		}
		funcs = append(funcs, fun)
	}

	return funcs
}

func Fields(bakFields []*backuppb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	fields := make([]*schemapb.FieldSchema, 0, len(bakFields))

	for _, bakField := range bakFields {
		defaultValue, err := DefaultValue(bakField)
		if err != nil {
			return nil, fmt.Errorf("conv: get default value: %w", err)
		}

		fieldRestore := &schemapb.FieldSchema{
			FieldID:          bakField.GetFieldID(),
			Name:             bakField.GetName(),
			IsPrimaryKey:     bakField.GetIsPrimaryKey(),
			AutoID:           bakField.GetAutoID(),
			Description:      bakField.GetDescription(),
			DataType:         schemapb.DataType(bakField.GetDataType()),
			TypeParams:       pbconv.BakKVToMilvusKV(bakField.GetTypeParams()),
			IndexParams:      pbconv.BakKVToMilvusKV(bakField.GetIndexParams()),
			IsDynamic:        bakField.GetIsDynamic(),
			IsPartitionKey:   bakField.GetIsPartitionKey(),
			Nullable:         bakField.GetNullable(),
			ElementType:      schemapb.DataType(bakField.GetElementType()),
			IsFunctionOutput: bakField.GetIsFunctionOutput(),
			DefaultValue:     defaultValue,
		}

		fields = append(fields, fieldRestore)
	}

	return fields, nil
}

func StructArrayFields(bakFields []*backuppb.StructArrayFieldSchema) ([]*schemapb.StructArrayFieldSchema, error) {
	structArrayFields := make([]*schemapb.StructArrayFieldSchema, 0, len(bakFields))
	for _, bakField := range bakFields {
		fields, err := Fields(bakField.GetFields())
		if err != nil {
			return nil, fmt.Errorf("conv: convert struct array fields: %w", err)
		}

		structArrayField := &schemapb.StructArrayFieldSchema{
			FieldID:     bakField.GetFieldID(),
			Name:        bakField.GetName(),
			Description: bakField.GetDescription(),
			Fields:      fields,
		}

		structArrayFields = append(structArrayFields, structArrayField)
	}

	return structArrayFields, nil
}

func Schema(bakSchema *backuppb.CollectionSchema) (*schemapb.CollectionSchema, error) {
	fields, err := Fields(bakSchema.GetFields())
	if err != nil {
		return nil, fmt.Errorf("conv: conv fields: %w", err)
	}

	functions := Functions(bakSchema.GetFunctions())

	structArrayFields, err := StructArrayFields(bakSchema.GetStructArrayFields())
	if err != nil {
		return nil, fmt.Errorf("conv: conv struct array fields: %w", err)
	}

	properties := pbconv.BakKVToMilvusKV(bakSchema.GetProperties())

	schema := &schemapb.CollectionSchema{
		Name:               bakSchema.GetName(),
		Description:        bakSchema.GetDescription(),
		Functions:          functions,
		Fields:             fields,
		EnableDynamicField: bakSchema.GetEnableDynamicField(),
		Properties:         properties,
		StructArrayFields:  structArrayFields,
	}

	return schema, nil
}

func Users(bacUsers []*backuppb.UserInfo, curUsers []*milvuspb.UserInfo) []*milvuspb.UserInfo {
	um := lo.SliceToMap(curUsers, func(user *milvuspb.UserInfo) (string, struct{}) {
		return user.User, struct{}{}
	})

	users := make([]*milvuspb.UserInfo, 0, len(bacUsers))
	for _, user := range bacUsers {
		// skip if user already exist
		if _, ok := um[user.GetUser()]; ok {
			continue
		}

		ur := lo.Map(user.GetRoles(), func(role *backuppb.RoleEntity, index int) *milvuspb.RoleEntity {
			return &milvuspb.RoleEntity{Name: role.Name}
		})
		userEntity := &milvuspb.UserInfo{
			User:     user.User,
			Password: user.Password,
			Roles:    ur,
		}
		users = append(users, userEntity)
	}

	return users
}

func Roles(bakRoles []*backuppb.RoleEntity, curRoles []*milvuspb.RoleEntity) []*milvuspb.RoleEntity {
	rm := lo.SliceToMap(curRoles, func(role *milvuspb.RoleEntity) (string, struct{}) {
		return role.Name, struct{}{}
	})

	roles := make([]*milvuspb.RoleEntity, 0, len(bakRoles))
	for _, role := range bakRoles {
		// skip if role already exist
		if _, ok := rm[role.GetName()]; ok {
			continue
		}

		roleEntity := &milvuspb.RoleEntity{
			Name: role.GetName(),
		}
		roles = append(roles, roleEntity)
	}

	return roles
}

func backupGrantKey(grant *backuppb.GrantEntity) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s",
		grant.GetObject().GetName(),
		grant.GetObjectName(),
		grant.GetRole().GetName(),
		grant.GetGrantor().GetUser().GetName(),
		grant.GetGrantor().GetPrivilege().GetName(),
		grant.GetDbName())
}

func milvusGrantKey(grant *milvuspb.GrantEntity) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s",
		grant.GetObject().GetName(),
		grant.GetObjectName(),
		grant.GetRole().GetName(),
		grant.GetGrantor().GetUser().GetName(),
		grant.GetGrantor().GetPrivilege().GetName(),
		grant.GetDbName())
}

func Grants(bakGrants []*backuppb.GrantEntity, curGrants []*milvuspb.GrantEntity) []*milvuspb.GrantEntity {
	gm := lo.SliceToMap(curGrants, func(grant *milvuspb.GrantEntity) (string, struct{}) {
		return milvusGrantKey(grant), struct{}{}
	})
	grants := make([]*milvuspb.GrantEntity, 0, len(bakGrants))
	for _, grant := range bakGrants {
		// skip if grant already exist
		if _, ok := gm[backupGrantKey(grant)]; ok {
			continue
		}

		grantEntity := &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: grant.GetRole().GetName()},
			Object:     &milvuspb.ObjectEntity{Name: grant.GetObject().GetName()},
			ObjectName: grant.GetObjectName(),
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: grant.GetGrantor().GetUser().GetName()},
				Privilege: &milvuspb.PrivilegeEntity{Name: grant.GetGrantor().GetPrivilege().GetName()},
			},
			DbName: grant.GetDbName(),
		}
		grants = append(grants, grantEntity)
	}

	return grants
}

func PrivilegeGroups(bakGroups []*backuppb.PrivilegeGroupInfo, curGroups []*milvuspb.PrivilegeGroupInfo) []*milvuspb.PrivilegeGroupInfo {
	curGroupName := lo.SliceToMap(curGroups, func(group *milvuspb.PrivilegeGroupInfo) (string, struct{}) {
		return group.GetGroupName(), struct{}{}
	})

	groups := make([]*milvuspb.PrivilegeGroupInfo, 0, len(bakGroups))
	for _, group := range bakGroups {
		if _, ok := curGroupName[group.GetGroupName()]; ok {
			continue
		}

		privileges := lo.Map(group.GetPrivileges(), func(privilege *backuppb.PrivilegeEntity, _ int) *milvuspb.PrivilegeEntity {
			return &milvuspb.PrivilegeEntity{Name: privilege.GetName()}
		})
		g := &milvuspb.PrivilegeGroupInfo{GroupName: group.GetGroupName(), Privileges: privileges}
		groups = append(groups, g)
	}

	return groups
}
