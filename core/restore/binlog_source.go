package restore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/pbconv"
	"github.com/zilliztech/milvus-backup/internal/storage"
	"github.com/zilliztech/milvus-backup/internal/storage/mpath"
)

// _deltaDirProbeConcurrency bounds how many delta-log existence probes one
// collection's enumeration keeps in flight. The enumeration runs at plan
// build time, where one serial round trip per segment directory would sit on
// the restore's critical path.
const _deltaDirProbeConcurrency = 32

// binlogSource enumerates what one collection's binlog backup carries: the dir
// groups of every partition, leveled and L0 apart, with the backup timestamps
// the import requests ride on. It is a pure enumerator — no scheduling, no
// state reporting, no import protocol; the binlog plan drives it and decides
// the rest.
type binlogSource struct {
	collBackup *backuppb.CollectionBackupInfo
	option     *Option

	backupDir     string
	backupStorage storage.Client

	vchTimestamp struct {
		mu    sync.RWMutex
		vchTS map[string]uint64
	}

	logger *zap.Logger
}

func newBinlogSource(
	collBackup *backuppb.CollectionBackupInfo,
	option *Option,
	backupDir string,
	backupStorage storage.Client,
	logger *zap.Logger,
) *binlogSource {
	src := &binlogSource{
		collBackup:    collBackup,
		option:        option,
		backupDir:     backupDir,
		backupStorage: backupStorage,
		logger:        logger,
	}
	src.vchTimestamp.vchTS = make(map[string]uint64)
	return src
}

func (s *binlogSource) backupTS(vch string) (uint64, error) {
	if !s.option.TruncateBinlogByTs {
		return 0, nil
	}

	if len(vch) == 0 {
		return 0, fmt.Errorf("restore_collection: empty vch but truncate binlog by ts is set")
	}

	s.vchTimestamp.mu.RLock()
	// fast path, if the timestamp is already cached
	if ts, ok := s.vchTimestamp.vchTS[vch]; ok {
		s.vchTimestamp.mu.RUnlock()
		return ts, nil
	}
	s.vchTimestamp.mu.RUnlock()

	// slow path, if the timestamp is not cached, get it from backup
	posStr, ok := s.collBackup.GetChannelCheckpoints()[vch]
	if !ok {
		return 0, fmt.Errorf("restore_collection: failed to get vch %s checkpoint", vch)
	}
	pos, err := pbconv.Base64DecodeMsgPosition(posStr)
	if err != nil {
		return 0, fmt.Errorf("restore_collection: failed to decode checkpoint: %w", err)
	}
	ts := pos.GetTimestamp()

	// cache the timestamp, it is idempotence so no need to check if it is already cached.
	s.vchTimestamp.mu.Lock()
	defer s.vchTimestamp.mu.Unlock()
	s.vchTimestamp.vchTS[vch] = ts
	return ts, nil
}

func (s *binlogSource) notL0DirGroupsWithoutGroupID(part *backuppb.PartitionBackupInfo) ([]dirGroup, error) {
	if s.option.TruncateBinlogByTs {
		return nil, fmt.Errorf("restore: truncate binlog by ts is not supported if group id is not set in backup")
	}

	opts := []mpath.Option{
		mpath.CollectionID(s.collBackup.GetCollectionId()),
		mpath.PartitionID(part.GetPartitionId()),
	}
	partDir := s.buildBackupPartitionDir(part.GetSize(), opts...)

	s.logger.Info("build dir groups without group id", zap.String("partition", part.GetPartitionName()))
	return []dirGroup{{dirs: []partitionDir{partDir}}}, nil
}

type batchKey struct {
	vch string
	sv  int64
}

func (s *binlogSource) notL0DirGroupsWithGroupID(notL0Segs []*backuppb.SegmentBackupInfo) ([]dirGroup, error) {
	// group by vchannel and storage version
	segGroups := lo.GroupBy(notL0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var groups []dirGroup
	for key, segs := range segGroups {
		ts, err := s.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		dirs := make([]partitionDir, 0, len(segs))
		for _, seg := range segs {
			opts := []mpath.Option{
				mpath.CollectionID(s.collBackup.GetCollectionId()),
				mpath.PartitionID(seg.GetPartitionId()),
				mpath.GroupID(seg.GetGroupId()),
			}

			dirs = append(dirs, s.buildBackupPartitionDir(seg.GetSize(), opts...))
		}

		groups = append(groups, dirGroup{timestamp: ts, storageVersion: key.sv, dirs: dirs})
	}

	s.logger.Info("build dir groups with group id done", zap.Int("group_num", len(groups)))

	return groups, nil
}

func (s *binlogSource) notL0DirGroups(ctx context.Context, part *backuppb.PartitionBackupInfo) ([]dirGroup, error) {
	var withGroupID bool
	notL0Segs := make([]*backuppb.SegmentBackupInfo, 0, len(part.GetSegmentBackups()))
	for _, seg := range part.GetSegmentBackups() {
		if seg.IsL0 {
			continue
		}
		notL0Segs = append(notL0Segs, seg)
		if seg.GetGroupId() != 0 {
			withGroupID = true
		}
	}
	if len(notL0Segs) == 0 {
		s.logger.Info("no not L0 segments found")
		return nil, nil
	}

	var groups []dirGroup
	var err error
	if withGroupID {
		groups, err = s.notL0DirGroupsWithGroupID(notL0Segs)
	} else {
		// backward compatible old backup without group id
		groups, err = s.notL0DirGroupsWithoutGroupID(part)
	}
	if err != nil {
		return nil, err
	}

	return s.probeDeltaDirs(ctx, groups)
}

func (s *binlogSource) l0DirGroups(l0Segs []*backuppb.SegmentBackupInfo) ([]dirGroup, error) {
	segGroups := lo.GroupBy(l0Segs, func(seg *backuppb.SegmentBackupInfo) batchKey {
		return batchKey{vch: seg.GetVChannel(), sv: seg.GetStorageVersion()}
	})

	var groups []dirGroup
	for key, segs := range segGroups {
		ts, err := s.backupTS(key.vch)
		if err != nil {
			return nil, fmt.Errorf("restore_collection: get vch %s ts: %w", key.vch, err)
		}

		dirs := make([]partitionDir, 0, len(segs))
		for _, seg := range segs {
			opts := []mpath.Option{
				mpath.CollectionID(s.collBackup.GetCollectionId()),
				mpath.PartitionID(seg.GetPartitionId()),
				mpath.SegmentID(seg.GetSegmentId()),
			}

			deltaLogDir := mpath.BackupDeltaLogDir(s.backupDir, opts...)
			dirs = append(dirs, partitionDir{deltaLogDir: deltaLogDir, size: seg.GetSize()})
		}
		groups = append(groups, dirGroup{isL0: true, timestamp: ts, storageVersion: key.sv, dirs: dirs})
	}

	sortDirGroups(groups)

	return groups, nil
}

type partitionDir struct {
	insertLogDir string
	deltaLogDir  string

	size int64
}

// dirGroup is a set of partition dirs that share one import request's
// parameters: same vchannel (hence same backup timestamp) and same storage
// version, or the L0 segments of one vchannel. How the dirs of a group split
// into import jobs is decided by the import planner that consumes the group.
type dirGroup struct {
	isL0           bool
	timestamp      uint64
	storageVersion int64

	dirs []partitionDir
}

// sortDirGroups pins the group order the map-based grouping leaves open, so
// one backup meta always enumerates into the same plan.
func sortDirGroups(groups []dirGroup) {
	slices.SortFunc(groups, func(a, b dirGroup) int {
		if c := strings.Compare(a.dirs[0].insertLogDir, b.dirs[0].insertLogDir); c != 0 {
			return c
		}
		return strings.Compare(a.dirs[0].deltaLogDir, b.dirs[0].deltaLogDir)
	})
}

// buildBackupPartitionDir is the pure path half of enumerating one dir: the
// delta dir is assumed present until probeDeltaDirs finds otherwise.
func (s *binlogSource) buildBackupPartitionDir(size int64, pathOpt ...mpath.Option) partitionDir {
	return partitionDir{
		insertLogDir: mpath.BackupInsertLogDir(s.backupDir, pathOpt...),
		deltaLogDir:  mpath.BackupDeltaLogDir(s.backupDir, pathOpt...),
		size:         size,
	}
}

// probeDeltaDirs drops the delta dirs that do not exist in the backup. A dir
// the probe misses imports through its insert logs only.
func (s *binlogSource) probeDeltaDirs(ctx context.Context, groups []dirGroup) ([]dirGroup, error) {
	g, subCtx := errgroup.WithContext(ctx)
	g.SetLimit(_deltaDirProbeConcurrency)
	for gi := range groups {
		for di := range groups[gi].dirs {
			dir := &groups[gi].dirs[di]
			if dir.deltaLogDir == "" {
				continue
			}
			g.Go(func() error {
				exist, err := storage.Exist(subCtx, s.backupStorage, dir.deltaLogDir)
				if err != nil {
					return fmt.Errorf("restore_collection: check delta log exist: %w", err)
				}
				if !exist {
					dir.deltaLogDir = ""
				}
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sortDirGroups(groups)
	return groups, nil
}
