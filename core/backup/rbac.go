package backup

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/core/client/milvus"
	"github.com/zilliztech/milvus-backup/core/meta"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/log"
)

type RBACTask struct {
	backupID string
	meta     *meta.MetaManager

	grpcCli milvus.Grpc

	logger *zap.Logger
}

func NewRBACTask(backupID string, meta *meta.MetaManager, grpcCli milvus.Grpc) *RBACTask {
	logger := log.L().With(zap.String("backup_id", backupID))
	return &RBACTask{
		backupID: backupID,
		meta:     meta,
		grpcCli:  grpcCli,
		logger:   logger,
	}
}

func (rt *RBACTask) Execute(ctx context.Context) error {
	rt.logger.Info("start backup RBAC")

	resp, err := rt.grpcCli.BackupRBAC(ctx)
	if err != nil {
		return fmt.Errorf("backup: get RBAC info: %w", err)
	}
	rt.logger.Info("read RBAC info from milvus done",
		zap.Int("user", len(resp.GetRBACMeta().GetUsers())),
		zap.Int("role", len(resp.GetRBACMeta().GetRoles())),
		zap.Int("grant", len(resp.GetRBACMeta().GetGrants())),
		zap.Int("privilege_group", len(resp.GetRBACMeta().GetPrivilegeGroups())))

	rbacPb := &backuppb.RBACMeta{
		Users:           rt.users(resp.GetRBACMeta().GetUsers()),
		Roles:           rt.roles(resp.GetRBACMeta().GetRoles()),
		Grants:          rt.grants(resp.GetRBACMeta().GetGrants()),
		PrivilegeGroups: rt.privilegeGroups(resp.GetRBACMeta().GetPrivilegeGroups()),
	}

	rt.meta.UpdateBackup(rt.backupID, meta.SetRBACMeta(rbacPb))

	rt.logger.Info("backup RBAC done")
	return nil
}

func (rt *RBACTask) users(users []*milvuspb.UserInfo) []*backuppb.UserInfo {
	bakUsers := make([]*backuppb.UserInfo, 0, len(users))
	for _, user := range users {
		roles := lo.Map(user.GetRoles(), func(role *milvuspb.RoleEntity, index int) *backuppb.RoleEntity {
			return &backuppb.RoleEntity{Name: role.GetName()}
		})
		bakUser := &backuppb.UserInfo{
			User:     user.GetUser(),
			Password: user.GetPassword(),
			Roles:    roles,
		}
		bakUsers = append(bakUsers, bakUser)
	}

	return bakUsers
}

func (rt *RBACTask) roles(roles []*milvuspb.RoleEntity) []*backuppb.RoleEntity {
	return lo.Map(roles, func(role *milvuspb.RoleEntity, _ int) *backuppb.RoleEntity {
		return &backuppb.RoleEntity{Name: role.Name}
	})
}

func (rt *RBACTask) grants(grants []*milvuspb.GrantEntity) []*backuppb.GrantEntity {
	bakGrants := make([]*backuppb.GrantEntity, 0, len(grants))
	for _, roleGrant := range grants {
		grantBak := &backuppb.GrantEntity{
			Role: &backuppb.RoleEntity{
				Name: roleGrant.GetRole().GetName(),
			},
			Object: &backuppb.ObjectEntity{
				Name: roleGrant.GetObject().GetName(),
			},
			ObjectName: roleGrant.GetObjectName(),
			Grantor: &backuppb.GrantorEntity{
				User: &backuppb.UserEntity{
					Name: roleGrant.GetGrantor().GetUser().GetName(),
				},
				Privilege: &backuppb.PrivilegeEntity{
					Name: roleGrant.GetGrantor().GetPrivilege().GetName(),
				},
			},
			DbName: roleGrant.GetDbName(),
		}
		bakGrants = append(bakGrants, grantBak)
	}

	return bakGrants
}

func (rt *RBACTask) privilegeGroups(groups []*milvuspb.PrivilegeGroupInfo) []*backuppb.PrivilegeGroupInfo {
	bakGroups := make([]*backuppb.PrivilegeGroupInfo, 0, len(groups))
	for _, group := range groups {
		privileges := lo.Map(group.GetPrivileges(), func(privilege *milvuspb.PrivilegeEntity, _ int) *backuppb.PrivilegeEntity {
			return &backuppb.PrivilegeEntity{Name: privilege.GetName()}
		})
		bakGroup := &backuppb.PrivilegeGroupInfo{
			GroupName:  group.GetGroupName(),
			Privileges: privileges,
		}
		bakGroups = append(bakGroups, bakGroup)
	}

	return bakGroups
}
