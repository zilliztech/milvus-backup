package restore

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type RBACTask struct {
	grpcCli client.Grpc

	bakRBAC *backuppb.RBACMeta
}

func (rt *RBACTask) Execute(ctx context.Context) error {
	curRBAC, err := rt.grpcCli.BackupRBAC(ctx)
	if err != nil {
		return fmt.Errorf("restore_rbac: get current rbac: %w", err)
	}

	users := rt.users(curRBAC.GetRBACMeta().GetUsers())
	roles := rt.roles(curRBAC.GetRBACMeta().GetRoles())
	grants := rt.grants(curRBAC.GetRBACMeta().GetGrants())

	rbacMeta := &milvuspb.RBACMeta{
		Users:  users,
		Roles:  roles,
		Grants: grants,
	}

	log.Info("insert rbac to milvus", zap.Int("users", len(users)),
		zap.Int("roles", len(roles)),
		zap.Int("grants", len(grants)))
	if err := rt.grpcCli.RestoreRBAC(ctx, rbacMeta); err != nil {
		return fmt.Errorf("restore_rbac: restore rbac: %w", err)
	}

	return nil
}

func NewRBACTask(grpcCli client.Grpc, bakRBAC *backuppb.RBACMeta) *RBACTask {
	return &RBACTask{
		grpcCli: grpcCli,

		bakRBAC: bakRBAC,
	}
}

func (rt *RBACTask) users(curUsers []*milvuspb.UserInfo) []*milvuspb.UserInfo {
	um := lo.SliceToMap(curUsers, func(user *milvuspb.UserInfo) (string, struct{}) {
		return user.User, struct{}{}
	})
	users := make([]*milvuspb.UserInfo, 0, len(rt.bakRBAC.GetUsers()))
	for _, user := range rt.bakRBAC.GetUsers() {
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

func (rt *RBACTask) roles(curRoles []*milvuspb.RoleEntity) []*milvuspb.RoleEntity {
	rm := lo.SliceToMap(curRoles, func(role *milvuspb.RoleEntity) (string, struct{}) {
		return role.Name, struct{}{}
	})
	roles := make([]*milvuspb.RoleEntity, 0, len(rt.bakRBAC.GetRoles()))
	for _, role := range rt.bakRBAC.GetRoles() {
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

func (rt *RBACTask) grants(curGrants []*milvuspb.GrantEntity) []*milvuspb.GrantEntity {
	gm := lo.SliceToMap(curGrants, func(grant *milvuspb.GrantEntity) (string, struct{}) {
		return milvusGrantKey(grant), struct{}{}
	})
	grants := make([]*milvuspb.GrantEntity, 0, len(rt.bakRBAC.GetGrants()))
	for _, grant := range rt.bakRBAC.GetGrants() {
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
