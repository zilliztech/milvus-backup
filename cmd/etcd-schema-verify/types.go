package main

// VerifyResult is the top-level JSON output.
type VerifyResult struct {
	Collection string          `json:"collection"`
	Database   string          `json:"database,omitempty"`
	Aligned    bool            `json:"aligned"`
	Source     *CollectionDump `json:"source"`
	Target     *CollectionDump `json:"target"`
	Diffs      []Diff          `json:"diffs"`
}

// Diff represents a single difference between source and target.
type Diff struct {
	Path string `json:"path"`
	Src  string `json:"src"`
	Dst  string `json:"dst"`
}

// CollectionDump is the full schema snapshot read from one etcd.
type CollectionDump struct {
	ID               int64             `json:"id"`
	DBID             int64             `json:"db_id"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	State            string            `json:"state"`
	ShardsNum        int32             `json:"shards_num"`
	ConsistencyLevel string            `json:"consistency_level"`
	Properties       map[string]string `json:"properties,omitempty"`

	Fields     []FieldDump     `json:"fields"`
	Partitions []PartitionDump `json:"partitions"`
	Indexes    []IndexDump     `json:"indexes,omitempty"`
	Functions  []FunctionDump  `json:"functions,omitempty"`

	// Display-only, skipped in comparison
	CreateTime           uint64   `json:"create_timestamp"`
	UpdateTimestamp      uint64   `json:"update_timestamp"`
	VirtualChannelNames  []string `json:"virtual_channel_names"`
	PhysicalChannelNames []string `json:"physical_channel_names"`
}

// FieldDump holds one field's metadata.
type FieldDump struct {
	FieldID          int64             `json:"field_id"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	DataType         string            `json:"data_type"`
	ElementType      string            `json:"element_type,omitempty"`
	TypeParams       map[string]string `json:"type_params,omitempty"`
	IndexParams      map[string]string `json:"index_params,omitempty"`
	IsPrimaryKey     bool              `json:"is_primary_key"`
	AutoID           bool              `json:"auto_id"`
	IsPartitionKey   bool              `json:"is_partition_key"`
	IsClusteringKey  bool              `json:"is_clustering_key"`
	IsFunctionOutput bool              `json:"is_function_output"`
	Nullable         bool              `json:"nullable"`
	DefaultValue     string            `json:"default_value,omitempty"`
	State            string            `json:"state,omitempty"`
}

// PartitionDump holds one partition's metadata.
type PartitionDump struct {
	PartitionID      int64  `json:"partition_id"`
	PartitionName    string `json:"partition_name"`
	State            string `json:"state"`
	CreatedTimestamp uint64 `json:"created_timestamp"`
}

// IndexDump holds one index's metadata.
type IndexDump struct {
	IndexID         int64             `json:"index_id"`
	IndexName       string            `json:"index_name"`
	FieldID         int64             `json:"field_id"`
	TypeParams      map[string]string `json:"type_params,omitempty"`
	IndexParams     map[string]string `json:"index_params,omitempty"`
	UserIndexParams map[string]string `json:"user_index_params,omitempty"`
	IsAutoIndex     bool              `json:"is_auto_index"`
	State           string            `json:"state"`
	Deleted         bool              `json:"deleted"`
}

// FunctionDump holds one function's metadata.
type FunctionDump struct {
	ID               int64             `json:"id"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	Type             string            `json:"type"`
	InputFieldIDs    []int64           `json:"input_field_ids"`
	InputFieldNames  []string          `json:"input_field_names"`
	OutputFieldIDs   []int64           `json:"output_field_ids"`
	OutputFieldNames []string          `json:"output_field_names"`
	Params           map[string]string `json:"params,omitempty"`
}
