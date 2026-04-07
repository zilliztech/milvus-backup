package main

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// kvGetter abstracts etcd Get for testability.
type kvGetter interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

type reader struct {
	cli      kvGetter
	rootPath string
}

func (r *reader) prefix(parts ...string) string {
	elems := append([]string{r.rootPath}, parts...)
	return path.Join(elems...)
}

// findCollection scans etcd for a collection by name, returns the full dump.
func (r *reader) findCollection(ctx context.Context, dbName, collName string) (*CollectionDump, error) {
	dbs, err := r.listDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}

	type match struct {
		info   *etcdpb.CollectionInfo
		dbName string
	}
	var matches []match

	for _, db := range dbs {
		if dbName != "" && db.GetName() != dbName {
			continue
		}

		colls, err := r.listCollections(ctx, db.GetId())
		if err != nil {
			return nil, fmt.Errorf("list collections for db %d (%s): %w", db.GetId(), db.GetName(), err)
		}

		for _, c := range colls {
			if c.GetSchema().GetName() == collName {
				matches = append(matches, match{info: c, dbName: db.GetName()})
			}
		}
	}

	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("collection %q not found in etcd (root_path=%s)", collName, r.rootPath)
	case 1:
		return r.loadCollectionDetail(ctx, matches[0].info)
	default:
		var dbNames []string
		for _, m := range matches {
			dbNames = append(dbNames, m.dbName)
		}
		return nil, fmt.Errorf("collection %q found in multiple databases %v, specify --db", collName, dbNames)
	}
}

func (r *reader) listDatabases(ctx context.Context) ([]*etcdpb.DatabaseInfo, error) {
	prefix := r.prefix("meta", "root-coord", "database", "db-info") + "/"
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var dbs []*etcdpb.DatabaseInfo
	for _, kv := range resp.Kvs {
		db := &etcdpb.DatabaseInfo{}
		if err := proto.Unmarshal(kv.Value, db); err != nil {
			continue
		}
		dbs = append(dbs, db)
	}

	if len(dbs) == 0 {
		dbs = append(dbs, &etcdpb.DatabaseInfo{
			Id:    0,
			Name:  "default",
			State: etcdpb.DatabaseState_DatabaseCreated,
		})
	}

	return dbs, nil
}

func (r *reader) listCollections(ctx context.Context, dbID int64) ([]*etcdpb.CollectionInfo, error) {
	prefix := r.prefix("meta", "root-coord", "database", "collection-info", strconv.FormatInt(dbID, 10)) + "/"
	colls, err := r.getCollections(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if len(colls) == 0 && dbID == 0 {
		prefix = r.prefix("meta", "root-coord", "collection") + "/"
		colls, err = r.getCollections(ctx, prefix)
		if err != nil {
			return nil, err
		}
	}

	return colls, nil
}

func (r *reader) getCollections(ctx context.Context, prefix string) ([]*etcdpb.CollectionInfo, error) {
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var colls []*etcdpb.CollectionInfo
	for _, kv := range resp.Kvs {
		c := &etcdpb.CollectionInfo{}
		if err := proto.Unmarshal(kv.Value, c); err != nil {
			continue
		}
		colls = append(colls, c)
	}
	return colls, nil
}

func (r *reader) loadCollectionDetail(ctx context.Context, coll *etcdpb.CollectionInfo) (*CollectionDump, error) {
	collID := coll.GetID()

	d := &CollectionDump{
		ID:                   collID,
		DBID:                 coll.GetDbId(),
		Name:                 coll.GetSchema().GetName(),
		Description:          coll.GetSchema().GetDescription(),
		State:                coll.GetState().String(),
		ShardsNum:            coll.GetShardsNum(),
		ConsistencyLevel:     coll.GetConsistencyLevel().String(),
		Properties:           kvPairsToMap(coll.GetProperties()),
		CreateTime:           coll.GetCreateTime(),
		UpdateTimestamp:      coll.GetUpdateTimestamp(),
		VirtualChannelNames:  coll.GetVirtualChannelNames(),
		PhysicalChannelNames: coll.GetPhysicalChannelNames(),
	}

	// inline schema fields
	for _, f := range coll.GetSchema().GetFields() {
		d.Fields = append(d.Fields, buildFieldDump(f))
	}

	// separately stored fields
	sepFields, err := r.listFields(ctx, collID)
	if err != nil {
		return nil, fmt.Errorf("list fields for collection %d: %w", collID, err)
	}
	mergeFields(d, sepFields)

	// partitions
	d.Partitions, err = r.listPartitions(ctx, collID)
	if err != nil {
		return nil, fmt.Errorf("list partitions for collection %d: %w", collID, err)
	}

	// indexes
	d.Indexes, err = r.listIndexes(ctx, collID)
	if err != nil {
		return nil, fmt.Errorf("list indexes for collection %d: %w", collID, err)
	}

	// functions
	d.Functions, err = r.listFunctions(ctx, collID)
	if err != nil {
		return nil, fmt.Errorf("list functions for collection %d: %w", collID, err)
	}

	// sort for deterministic comparison
	sort.Slice(d.Fields, func(i, j int) bool { return d.Fields[i].FieldID < d.Fields[j].FieldID })
	sort.Slice(d.Partitions, func(i, j int) bool { return d.Partitions[i].PartitionID < d.Partitions[j].PartitionID })
	sort.Slice(d.Indexes, func(i, j int) bool { return d.Indexes[i].IndexID < d.Indexes[j].IndexID })
	sort.Slice(d.Functions, func(i, j int) bool { return d.Functions[i].ID < d.Functions[j].ID })

	return d, nil
}

func (r *reader) listFields(ctx context.Context, collID int64) ([]*schemapb.FieldSchema, error) {
	prefix := r.prefix("meta", "root-coord", "fields", strconv.FormatInt(collID, 10)) + "/"
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var fields []*schemapb.FieldSchema
	for _, kv := range resp.Kvs {
		f := &schemapb.FieldSchema{}
		if err := proto.Unmarshal(kv.Value, f); err != nil {
			continue
		}
		fields = append(fields, f)
	}
	return fields, nil
}

func mergeFields(d *CollectionDump, sepFields []*schemapb.FieldSchema) {
	existing := make(map[int64]int, len(d.Fields))
	for i, f := range d.Fields {
		existing[f.FieldID] = i
	}

	for _, sf := range sepFields {
		if idx, ok := existing[sf.GetFieldID()]; ok {
			// update state from separately stored field
			d.Fields[idx].State = sf.GetState().String()
			continue
		}
		d.Fields = append(d.Fields, buildFieldDump(sf))
	}
}

func (r *reader) listPartitions(ctx context.Context, collID int64) ([]PartitionDump, error) {
	prefix := r.prefix("meta", "root-coord", "partitions", strconv.FormatInt(collID, 10)) + "/"
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var partitions []PartitionDump
	for _, kv := range resp.Kvs {
		p := &etcdpb.PartitionInfo{}
		if err := proto.Unmarshal(kv.Value, p); err != nil {
			continue
		}
		partitions = append(partitions, PartitionDump{
			PartitionID:      p.GetPartitionID(),
			PartitionName:    p.GetPartitionName(),
			State:            p.GetState().String(),
			CreatedTimestamp: p.GetPartitionCreatedTimestamp(),
		})
	}
	return partitions, nil
}

func (r *reader) listIndexes(ctx context.Context, collID int64) ([]IndexDump, error) {
	prefix := r.prefix("meta", "field-index", strconv.FormatInt(collID, 10)) + "/"
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var indexes []IndexDump
	for _, kv := range resp.Kvs {
		fi := &indexpb.FieldIndex{}
		if err := proto.Unmarshal(kv.Value, fi); err != nil {
			continue
		}
		info := fi.GetIndexInfo()
		indexes = append(indexes, IndexDump{
			IndexID:         info.GetIndexID(),
			IndexName:       info.GetIndexName(),
			FieldID:         info.GetFieldID(),
			TypeParams:      kvPairsToMap(info.GetTypeParams()),
			IndexParams:     kvPairsToMap(info.GetIndexParams()),
			UserIndexParams: kvPairsToMap(info.GetUserIndexParams()),
			IsAutoIndex:     info.GetIsAutoIndex(),
			State:           info.GetState().String(),
			Deleted:         fi.GetDeleted(),
		})
	}
	return indexes, nil
}

func (r *reader) listFunctions(ctx context.Context, collID int64) ([]FunctionDump, error) {
	prefix := r.prefix("meta", "root-coord", "functions", strconv.FormatInt(collID, 10)) + "/"
	resp, err := r.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var funcs []FunctionDump
	for _, kv := range resp.Kvs {
		f := &schemapb.FunctionSchema{}
		if err := proto.Unmarshal(kv.Value, f); err != nil {
			continue
		}
		funcs = append(funcs, FunctionDump{
			ID:               f.GetId(),
			Name:             f.GetName(),
			Description:      f.GetDescription(),
			Type:             f.GetType().String(),
			InputFieldIDs:    f.GetInputFieldIds(),
			InputFieldNames:  f.GetInputFieldNames(),
			OutputFieldIDs:   f.GetOutputFieldIds(),
			OutputFieldNames: f.GetOutputFieldNames(),
			Params:           kvPairsToMap(f.GetParams()),
		})
	}
	return funcs, nil
}

// helpers

func buildFieldDump(f *schemapb.FieldSchema) FieldDump {
	fd := FieldDump{
		FieldID:          f.GetFieldID(),
		Name:             f.GetName(),
		Description:      f.GetDescription(),
		DataType:         f.GetDataType().String(),
		TypeParams:       kvPairsToMap(f.GetTypeParams()),
		IndexParams:      kvPairsToMap(f.GetIndexParams()),
		IsPrimaryKey:     f.GetIsPrimaryKey(),
		AutoID:           f.GetAutoID(),
		IsPartitionKey:   f.GetIsPartitionKey(),
		IsClusteringKey:  f.GetIsClusteringKey(),
		IsFunctionOutput: f.GetIsFunctionOutput(),
		Nullable:         f.GetNullable(),
		State:            f.GetState().String(),
	}

	if f.GetElementType() != schemapb.DataType_None {
		fd.ElementType = f.GetElementType().String()
	}

	if f.GetDefaultValue() != nil {
		fd.DefaultValue = fmt.Sprintf("%v", f.GetDefaultValue())
	}

	return fd
}

func kvPairsToMap(pairs []*commonpb.KeyValuePair) map[string]string {
	if len(pairs) == 0 {
		return nil
	}
	m := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		m[kv.GetKey()] = kv.GetValue()
	}
	return m
}
