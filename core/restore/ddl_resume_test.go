package restore

import (
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
	"github.com/zilliztech/milvus-backup/internal/jobstate"
)

func TestDDLRecordsTargetCollectionID(t *testing.T) {
	for name, opt := range map[string]Option{
		"MetaOnly":       {MetaOnly: true, Resumable: true},
		"TwoPhaseCommit": {Resumable: true, Secondaries: []Secondary{{Address: "secondary"}}},
	} {
		t.Run(name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			target := collref.New("db", "coll")
			restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("job", true)
			require.NoError(t, addErr)
			tracker := restoreTracker.NewCollTracker(target, 0).DDL()
			ddl := newCollDDLTask("job", &opt, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
			cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once()
			cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(false, nil).Once()
			cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
			require.NoError(t, ddl.Execute(t.Context()))
			_, id := tracker.DDLProgress()
			assert.Equal(t, int64(100), id)
			assert.True(t, tracker.CreateTargetPrepared())
		})
	}
}

func TestDDLResumeAtFailedIndex(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	ddl := newCollDDLTask("restore", &Option{Resumable: true, SkipCreateCollection: true, DropExistIndex: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
	cli.EXPECT().ListIndex(mock.Anything, "db", "coll").Return([]*milvuspb.IndexDescription{{IndexName: "idx"}}, nil).Once()
	cli.EXPECT().DropIndex(mock.Anything, "db", "coll", "idx").Return(assert.AnError).Once()
	require.ErrorIs(t, ddl.Execute(t.Context()), assert.AnError)
	assert.Equal(t, jobstate.RestoreDDLFail, tracker.State())

	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 101}, nil).Once()
	require.ErrorContains(t, ddl.Execute(t.Context()), "identity changed")
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
	cli.EXPECT().ListIndex(mock.Anything, "db", "coll").Return(nil, nil).Once()
	require.NoError(t, ddl.Execute(t.Context()))
	require.NoError(t, ddl.Execute(t.Context()))
	assert.Equal(t, jobstate.RestoreDDLSuccess, tracker.State())
}

func TestDDLResumeCreateWithoutRecordedID(t *testing.T) {
	for _, tc := range []struct {
		name          string
		createApplied bool
		createAcked   bool
	}{
		{name: "CreateReplyLost", createApplied: true},
		{name: "DescribeFailedAfterCreate", createApplied: true, createAcked: true},
		{name: "CreateDidNotReachMilvus"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			target := collref.New("db", "coll")
			restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
			require.NoError(t, addErr)
			tracker := restoreTracker.NewCollTracker(target, 0).DDL()
			backup := &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{
				EnableDynamicField: true,
				Fields:             []*backuppb.FieldSchema{{Name: "pk", FieldID: 100}, {Name: "extra", FieldID: 102}},
			}}
			newDDL := func() *collDDLTask {
				return newCollDDLTask("restore", &Option{Resumable: true}, CollOverride{}, backup, target, cli, tracker)
			}
			cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(false, nil).Once()
			if tc.createAcked {
				cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once()
				cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(nil, assert.AnError).Once()
			} else {
				cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(assert.AnError).Once()
			}
			require.ErrorIs(t, newDDL().Execute(t.Context()), assert.AnError)
			step, id := tracker.DDLProgress()
			assert.Equal(t, int(ddlSchema), step)
			assert.True(t, tracker.CreateTargetPrepared())
			assert.Zero(t, id)

			cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(tc.createApplied, nil).Once()
			if !tc.createApplied {
				cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once()
			}
			info := &milvuspb.DescribeCollectionResponse{CollectionID: 200, Schema: &schemapb.CollectionSchema{}}
			cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(info, nil).Once()
			cli.EXPECT().AddField(mock.Anything, "db", "coll", mock.MatchedBy(func(f *schemapb.FieldSchema) bool {
				return f.GetName() == "extra"
			})).Return(nil).Once()
			require.NoError(t, newDDL().Execute(t.Context()))
			step, id = tracker.DDLProgress()
			assert.Equal(t, int(ddlDone), step)
			assert.Equal(t, int64(200), id)
		})
	}
}

func TestDDLResumeRejectsTargetBeforeCreateAttempt(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	tracker.SetFail(assert.AnError)
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(true, nil).Once()
	ddl := newCollDDLTask("restore", &Option{Resumable: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
	assert.ErrorContains(t, ddl.Execute(t.Context()), "collection already exists")
}

func TestDDLResumeDropBeforeCreate(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	newDDL := func() *collDDLTask {
		return newCollDDLTask("restore", &Option{Resumable: true, DropExistCollection: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
	}
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(false, assert.AnError).Once()
	require.ErrorIs(t, newDDL().Execute(t.Context()), assert.AnError)
	step, _ := tracker.DDLProgress()
	assert.Equal(t, int(ddlSchema), step)
	assert.False(t, tracker.CreateTargetPrepared())

	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(true, nil).Once()
	cli.EXPECT().DropCollection(mock.Anything, "db", "coll").Return(assert.AnError).Once()
	require.ErrorIs(t, newDDL().Execute(t.Context()), assert.AnError)
	assert.False(t, tracker.CreateTargetPrepared())

	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(true, nil).Once()
	cli.EXPECT().DropCollection(mock.Anything, "db", "coll").Return(nil).Once()
	cli.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(assert.AnError).Once().Run(func(mock.Arguments) {
		step, id := tracker.DDLProgress()
		assert.Equal(t, int(ddlSchema), step)
		assert.True(t, tracker.CreateTargetPrepared())
		assert.Zero(t, id)
	})
	require.ErrorIs(t, newDDL().Execute(t.Context()), assert.AnError)

	// A failed existence check or a disappearance before Describe must stop this attempt.
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(false, assert.AnError).Once()
	require.ErrorIs(t, newDDL().Execute(t.Context()), assert.AnError)
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(true, nil).Twice()
	gone := errors.New("collection not found")
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(nil, gone).Once()
	require.ErrorIs(t, newDDL().Execute(t.Context()), gone)
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 200}, nil).Once()
	require.NoError(t, newDDL().Execute(t.Context()))
	step, id := tracker.DDLProgress()
	assert.Equal(t, int(ddlDone), step)
	assert.Equal(t, int64(200), id)
}

func TestDDLResumeWithExistingCollection(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	tracker.SetDDLProgress(int(ddlSchema), 100)
	tracker.SetFail(assert.AnError)
	ddl := newCollDDLTask("restore", &Option{Resumable: true, SkipCreateCollection: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
	describe := cli.EXPECT()
	describe.DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 101}, nil).Once()
	require.ErrorContains(t, ddl.Execute(t.Context()), "identity changed")
	describe.DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
	require.NoError(t, ddl.Execute(t.Context()))
	assert.Equal(t, jobstate.RestoreDDLSuccess, tracker.State())
}

func TestDDLResumeDoesNotRecreateBoundCollection(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	tracker.SetDDLProgress(int(ddlSchema), 100)
	ddl := newCollDDLTask("restore", &Option{Resumable: true, DropExistCollection: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(false, nil).Once()
	assert.ErrorContains(t, ddl.Execute(t.Context()), "bound collection db.coll (ID 100) no longer exists")
	step, id := tracker.DDLProgress()
	assert.Equal(t, int(ddlSchema), step)
	assert.Equal(t, int64(100), id)
}

func TestDDLResumeAtIndexesWithoutDrop(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	tracker.SetDDLProgress(int(ddlIndexes), 100)
	tracker.SetFail(assert.AnError)
	ddl := newCollDDLTask("restore", &Option{Resumable: true, DropExistCollection: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tracker)
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
	require.NoError(t, ddl.Execute(t.Context()))
	assert.Equal(t, jobstate.RestoreDDLSuccess, tracker.State())
}

func TestResumeAddsOnlyMissingFields(t *testing.T) {
	cli := milvus.NewMockGrpc(t)
	target := collref.New("db", "coll")
	restoreTracker, addErr := jobstate.NewStore().AddRestoreTask("restore", true)
	require.NoError(t, addErr)
	tracker := restoreTracker.NewCollTracker(target, 0).DDL()
	tracker.SetDDLProgress(int(ddlSchema), 200)
	tracker.SetFail(assert.AnError)
	backup := &backuppb.CollectionBackupInfo{Schema: &backuppb.CollectionSchema{
		EnableDynamicField: true,
		Fields:             []*backuppb.FieldSchema{{Name: "pk", FieldID: 100}, {Name: "a", FieldID: 102}, {Name: "b", FieldID: 103}},
	}}
	ddl := newCollDDLTask("restore", &Option{Resumable: true, DropExistCollection: true}, CollOverride{}, backup, target, cli, tracker)
	info := &milvuspb.DescribeCollectionResponse{CollectionID: 200, Schema: &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{Name: "a", FieldID: 102}},
	}}
	cli.EXPECT().HasCollection(mock.Anything, "db", "coll").Return(true, nil).Twice()
	cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(info, nil).Twice()
	cli.EXPECT().AddField(mock.Anything, "db", "coll", mock.MatchedBy(func(f *schemapb.FieldSchema) bool {
		return f.GetName() == "b" && f.GetFieldID() == 103
	})).Return(assert.AnError).Once()
	require.ErrorIs(t, ddl.Execute(t.Context()), assert.AnError)
	// The server applied the request before the client saw the error.
	info.Schema.Fields = append(info.Schema.Fields, &schemapb.FieldSchema{Name: "b", FieldID: 103})
	require.NoError(t, ddl.Execute(t.Context()))
}

func TestMissingFieldsRejectsConflicts(t *testing.T) {
	field := &schemapb.FieldSchema{Name: "a", FieldID: 102, DataType: schemapb.DataType_Int64}
	for _, existing := range []*schemapb.FieldSchema{
		{Name: "a", FieldID: 103, DataType: field.DataType},
		{Name: "other", FieldID: 102, DataType: field.DataType},
		{Name: "a", FieldID: 102, DataType: schemapb.DataType_Bool},
	} {
		_, err := missingFields([]*schemapb.FieldSchema{field}, []*schemapb.FieldSchema{existing})
		assert.ErrorContains(t, err, "conflicts")
	}
}

func TestSameFieldDefinition(t *testing.T) {
	defaultValue := func() *schemapb.ValueField {
		return &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "hello"}}
	}
	t.Run("OnlyRequestPropertiesMatter", func(t *testing.T) {
		planned := &schemapb.FieldSchema{DefaultValue: defaultValue()}
		target := &schemapb.FieldSchema{
			DefaultValue: defaultValue(),
			State:        schemapb.FieldState_FieldCreating,
			TypeSchema:   &schemapb.TypeSchema{},
		}
		assert.True(t, sameFieldDefinition(target, planned))
	})

	t.Run("ParametersComparedByKeyAndValue", func(t *testing.T) {
		a := &commonpb.KeyValuePair{Key: "a", Value: "1"}
		b := &commonpb.KeyValuePair{Key: "b", Value: "2"}
		planned := &schemapb.FieldSchema{TypeParams: []*commonpb.KeyValuePair{a, b}, IndexParams: []*commonpb.KeyValuePair{a, b}}
		target := &schemapb.FieldSchema{TypeParams: []*commonpb.KeyValuePair{b, a}, IndexParams: []*commonpb.KeyValuePair{b, a}}
		assert.True(t, sameFieldDefinition(target, planned))
		changed := &commonpb.KeyValuePair{Key: "a", Value: "3"}
		target.TypeParams[1] = changed
		assert.False(t, sameFieldDefinition(target, planned))
		target.TypeParams[1] = a
		target.IndexParams[1] = changed
		assert.False(t, sameFieldDefinition(target, planned))
	})

	for name, changed := range map[string]*schemapb.FieldSchema{
		"FieldID":         {FieldID: 101},
		"Name":            {Name: "extra"},
		"PrimaryKey":      {IsPrimaryKey: true},
		"AutoID":          {AutoID: true},
		"Description":     {Description: "changed"},
		"DataType":        {DataType: schemapb.DataType_Int64},
		"Dynamic":         {IsDynamic: true},
		"PartitionKey":    {IsPartitionKey: true},
		"Nullable":        {Nullable: true},
		"ElementNullable": {ElementNullable: true},
		"RecursiveType":   {TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}},
		"ElementType":     {ElementType: schemapb.DataType_Int64},
		"FunctionOutput":  {IsFunctionOutput: true},
		"DefaultValue":    {DefaultValue: defaultValue()},
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, sameFieldDefinition(changed, &schemapb.FieldSchema{}))
		})
	}
}

func TestDDLContinuesAfterListIndexError(t *testing.T) {
	for name, resumable := range map[string]bool{"Default": false, "Resumable": true} {
		t.Run(name, func(t *testing.T) {
			cli := milvus.NewMockGrpc(t)
			target := collref.New("db", "coll")
			tr, err := jobstate.NewStore().AddRestoreTask("job", resumable)
			require.NoError(t, err)
			ddl := newCollDDLTask("job", &Option{Resumable: resumable, SkipCreateCollection: true, DropExistIndex: true}, CollOverride{}, &backuppb.CollectionBackupInfo{}, target, cli, tr.NewCollTracker(target, 0).DDL())
			if resumable {
				cli.EXPECT().DescribeCollection(mock.Anything, "db", "coll").Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Once()
			}
			cli.EXPECT().ListIndex(mock.Anything, "db", "coll").Return(nil, assert.AnError).Once()
			assert.NoError(t, ddl.Execute(t.Context()))
		})
	}
}
