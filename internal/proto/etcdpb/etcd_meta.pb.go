// Code generated by protoc-gen-go. DO NOT EDIT.
// source: etcd_meta.proto

package etcdpb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	commonpb "github.com/zilliztech/milvus-backup/internal/proto/commonpb"
	schemapb "github.com/zilliztech/milvus-backup/internal/proto/schemapb"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type IndexInfo struct {
	IndexName            string                   `protobuf:"bytes,1,opt,name=index_name,json=indexName,proto3" json:"index_name,omitempty"`
	IndexID              int64                    `protobuf:"varint,2,opt,name=indexID,proto3" json:"indexID,omitempty"`
	IndexParams          []*commonpb.KeyValuePair `protobuf:"bytes,3,rep,name=index_params,json=indexParams,proto3" json:"index_params,omitempty"`
	Deleted              bool                     `protobuf:"varint,4,opt,name=deleted,proto3" json:"deleted,omitempty"`
	CreateTime           uint64                   `protobuf:"varint,5,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                 `json:"-"`
	XXX_unrecognized     []byte                   `json:"-"`
	XXX_sizecache        int32                    `json:"-"`
}

func (m *IndexInfo) Reset()         { *m = IndexInfo{} }
func (m *IndexInfo) String() string { return proto.CompactTextString(m) }
func (*IndexInfo) ProtoMessage()    {}
func (*IndexInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{0}
}

func (m *IndexInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_IndexInfo.Unmarshal(m, b)
}
func (m *IndexInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_IndexInfo.Marshal(b, m, deterministic)
}
func (m *IndexInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_IndexInfo.Merge(m, src)
}
func (m *IndexInfo) XXX_Size() int {
	return xxx_messageInfo_IndexInfo.Size(m)
}
func (m *IndexInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_IndexInfo.DiscardUnknown(m)
}

var xxx_messageInfo_IndexInfo proto.InternalMessageInfo

func (m *IndexInfo) GetIndexName() string {
	if m != nil {
		return m.IndexName
	}
	return ""
}

func (m *IndexInfo) GetIndexID() int64 {
	if m != nil {
		return m.IndexID
	}
	return 0
}

func (m *IndexInfo) GetIndexParams() []*commonpb.KeyValuePair {
	if m != nil {
		return m.IndexParams
	}
	return nil
}

func (m *IndexInfo) GetDeleted() bool {
	if m != nil {
		return m.Deleted
	}
	return false
}

func (m *IndexInfo) GetCreateTime() uint64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

type FieldIndexInfo struct {
	FiledID              int64    `protobuf:"varint,1,opt,name=filedID,proto3" json:"filedID,omitempty"`
	IndexID              int64    `protobuf:"varint,2,opt,name=indexID,proto3" json:"indexID,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FieldIndexInfo) Reset()         { *m = FieldIndexInfo{} }
func (m *FieldIndexInfo) String() string { return proto.CompactTextString(m) }
func (*FieldIndexInfo) ProtoMessage()    {}
func (*FieldIndexInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{1}
}

func (m *FieldIndexInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FieldIndexInfo.Unmarshal(m, b)
}
func (m *FieldIndexInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FieldIndexInfo.Marshal(b, m, deterministic)
}
func (m *FieldIndexInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FieldIndexInfo.Merge(m, src)
}
func (m *FieldIndexInfo) XXX_Size() int {
	return xxx_messageInfo_FieldIndexInfo.Size(m)
}
func (m *FieldIndexInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_FieldIndexInfo.DiscardUnknown(m)
}

var xxx_messageInfo_FieldIndexInfo proto.InternalMessageInfo

func (m *FieldIndexInfo) GetFiledID() int64 {
	if m != nil {
		return m.FiledID
	}
	return 0
}

func (m *FieldIndexInfo) GetIndexID() int64 {
	if m != nil {
		return m.IndexID
	}
	return 0
}

type CollectionInfo struct {
	ID         int64                      `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Schema     *schemapb.CollectionSchema `protobuf:"bytes,2,opt,name=schema,proto3" json:"schema,omitempty"`
	CreateTime uint64                     `protobuf:"varint,3,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	// deprecate
	PartitionIDs []int64 `protobuf:"varint,4,rep,packed,name=partitionIDs,proto3" json:"partitionIDs,omitempty"`
	// deprecate
	PartitionNames       []string          `protobuf:"bytes,5,rep,name=partitionNames,proto3" json:"partitionNames,omitempty"`
	FieldIndexes         []*FieldIndexInfo `protobuf:"bytes,6,rep,name=field_indexes,json=fieldIndexes,proto3" json:"field_indexes,omitempty"`
	VirtualChannelNames  []string          `protobuf:"bytes,7,rep,name=virtual_channel_names,json=virtualChannelNames,proto3" json:"virtual_channel_names,omitempty"`
	PhysicalChannelNames []string          `protobuf:"bytes,8,rep,name=physical_channel_names,json=physicalChannelNames,proto3" json:"physical_channel_names,omitempty"`
	// deprecate
	PartitionCreatedTimestamps []uint64                  `protobuf:"varint,9,rep,packed,name=partition_created_timestamps,json=partitionCreatedTimestamps,proto3" json:"partition_created_timestamps,omitempty"`
	ShardsNum                  int32                     `protobuf:"varint,10,opt,name=shards_num,json=shardsNum,proto3" json:"shards_num,omitempty"`
	StartPositions             []*commonpb.KeyDataPair   `protobuf:"bytes,11,rep,name=start_positions,json=startPositions,proto3" json:"start_positions,omitempty"`
	ConsistencyLevel           commonpb.ConsistencyLevel `protobuf:"varint,12,opt,name=consistency_level,json=consistencyLevel,proto3,enum=milvus.proto.common.ConsistencyLevel" json:"consistency_level,omitempty"`
	Partitions                 []*PartitionInfo          `protobuf:"bytes,13,rep,name=partitions,proto3" json:"partitions,omitempty"`
	XXX_NoUnkeyedLiteral       struct{}                  `json:"-"`
	XXX_unrecognized           []byte                    `json:"-"`
	XXX_sizecache              int32                     `json:"-"`
}

func (m *CollectionInfo) Reset()         { *m = CollectionInfo{} }
func (m *CollectionInfo) String() string { return proto.CompactTextString(m) }
func (*CollectionInfo) ProtoMessage()    {}
func (*CollectionInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{2}
}

func (m *CollectionInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CollectionInfo.Unmarshal(m, b)
}
func (m *CollectionInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CollectionInfo.Marshal(b, m, deterministic)
}
func (m *CollectionInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CollectionInfo.Merge(m, src)
}
func (m *CollectionInfo) XXX_Size() int {
	return xxx_messageInfo_CollectionInfo.Size(m)
}
func (m *CollectionInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_CollectionInfo.DiscardUnknown(m)
}

var xxx_messageInfo_CollectionInfo proto.InternalMessageInfo

func (m *CollectionInfo) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *CollectionInfo) GetSchema() *schemapb.CollectionSchema {
	if m != nil {
		return m.Schema
	}
	return nil
}

func (m *CollectionInfo) GetCreateTime() uint64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

func (m *CollectionInfo) GetPartitionIDs() []int64 {
	if m != nil {
		return m.PartitionIDs
	}
	return nil
}

func (m *CollectionInfo) GetPartitionNames() []string {
	if m != nil {
		return m.PartitionNames
	}
	return nil
}

func (m *CollectionInfo) GetFieldIndexes() []*FieldIndexInfo {
	if m != nil {
		return m.FieldIndexes
	}
	return nil
}

func (m *CollectionInfo) GetVirtualChannelNames() []string {
	if m != nil {
		return m.VirtualChannelNames
	}
	return nil
}

func (m *CollectionInfo) GetPhysicalChannelNames() []string {
	if m != nil {
		return m.PhysicalChannelNames
	}
	return nil
}

func (m *CollectionInfo) GetPartitionCreatedTimestamps() []uint64 {
	if m != nil {
		return m.PartitionCreatedTimestamps
	}
	return nil
}

func (m *CollectionInfo) GetShardsNum() int32 {
	if m != nil {
		return m.ShardsNum
	}
	return 0
}

func (m *CollectionInfo) GetStartPositions() []*commonpb.KeyDataPair {
	if m != nil {
		return m.StartPositions
	}
	return nil
}

func (m *CollectionInfo) GetConsistencyLevel() commonpb.ConsistencyLevel {
	if m != nil {
		return m.ConsistencyLevel
	}
	return commonpb.ConsistencyLevel_Strong
}

func (m *CollectionInfo) GetPartitions() []*PartitionInfo {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type PartitionInfo struct {
	PartitionID               int64    `protobuf:"varint,1,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	PartitionName             string   `protobuf:"bytes,2,opt,name=partitionName,proto3" json:"partitionName,omitempty"`
	PartitionCreatedTimestamp uint64   `protobuf:"varint,3,opt,name=partition_created_timestamp,json=partitionCreatedTimestamp,proto3" json:"partition_created_timestamp,omitempty"`
	XXX_NoUnkeyedLiteral      struct{} `json:"-"`
	XXX_unrecognized          []byte   `json:"-"`
	XXX_sizecache             int32    `json:"-"`
}

func (m *PartitionInfo) Reset()         { *m = PartitionInfo{} }
func (m *PartitionInfo) String() string { return proto.CompactTextString(m) }
func (*PartitionInfo) ProtoMessage()    {}
func (*PartitionInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{3}
}

func (m *PartitionInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PartitionInfo.Unmarshal(m, b)
}
func (m *PartitionInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PartitionInfo.Marshal(b, m, deterministic)
}
func (m *PartitionInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PartitionInfo.Merge(m, src)
}
func (m *PartitionInfo) XXX_Size() int {
	return xxx_messageInfo_PartitionInfo.Size(m)
}
func (m *PartitionInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_PartitionInfo.DiscardUnknown(m)
}

var xxx_messageInfo_PartitionInfo proto.InternalMessageInfo

func (m *PartitionInfo) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}

func (m *PartitionInfo) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *PartitionInfo) GetPartitionCreatedTimestamp() uint64 {
	if m != nil {
		return m.PartitionCreatedTimestamp
	}
	return 0
}

type SegmentIndexInfo struct {
	CollectionID         int64    `protobuf:"varint,1,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID          int64    `protobuf:"varint,2,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	SegmentID            int64    `protobuf:"varint,3,opt,name=segmentID,proto3" json:"segmentID,omitempty"`
	FieldID              int64    `protobuf:"varint,4,opt,name=fieldID,proto3" json:"fieldID,omitempty"`
	IndexID              int64    `protobuf:"varint,5,opt,name=indexID,proto3" json:"indexID,omitempty"`
	BuildID              int64    `protobuf:"varint,6,opt,name=buildID,proto3" json:"buildID,omitempty"`
	EnableIndex          bool     `protobuf:"varint,7,opt,name=enable_index,json=enableIndex,proto3" json:"enable_index,omitempty"`
	CreateTime           uint64   `protobuf:"varint,8,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SegmentIndexInfo) Reset()         { *m = SegmentIndexInfo{} }
func (m *SegmentIndexInfo) String() string { return proto.CompactTextString(m) }
func (*SegmentIndexInfo) ProtoMessage()    {}
func (*SegmentIndexInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{4}
}

func (m *SegmentIndexInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SegmentIndexInfo.Unmarshal(m, b)
}
func (m *SegmentIndexInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SegmentIndexInfo.Marshal(b, m, deterministic)
}
func (m *SegmentIndexInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SegmentIndexInfo.Merge(m, src)
}
func (m *SegmentIndexInfo) XXX_Size() int {
	return xxx_messageInfo_SegmentIndexInfo.Size(m)
}
func (m *SegmentIndexInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_SegmentIndexInfo.DiscardUnknown(m)
}

var xxx_messageInfo_SegmentIndexInfo proto.InternalMessageInfo

func (m *SegmentIndexInfo) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *SegmentIndexInfo) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}

func (m *SegmentIndexInfo) GetSegmentID() int64 {
	if m != nil {
		return m.SegmentID
	}
	return 0
}

func (m *SegmentIndexInfo) GetFieldID() int64 {
	if m != nil {
		return m.FieldID
	}
	return 0
}

func (m *SegmentIndexInfo) GetIndexID() int64 {
	if m != nil {
		return m.IndexID
	}
	return 0
}

func (m *SegmentIndexInfo) GetBuildID() int64 {
	if m != nil {
		return m.BuildID
	}
	return 0
}

func (m *SegmentIndexInfo) GetEnableIndex() bool {
	if m != nil {
		return m.EnableIndex
	}
	return false
}

func (m *SegmentIndexInfo) GetCreateTime() uint64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

// TODO move to proto files of interprocess communication
type CollectionMeta struct {
	ID                   int64                      `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Schema               *schemapb.CollectionSchema `protobuf:"bytes,2,opt,name=schema,proto3" json:"schema,omitempty"`
	CreateTime           uint64                     `protobuf:"varint,3,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	SegmentIDs           []int64                    `protobuf:"varint,4,rep,packed,name=segmentIDs,proto3" json:"segmentIDs,omitempty"`
	PartitionTags        []string                   `protobuf:"bytes,5,rep,name=partition_tags,json=partitionTags,proto3" json:"partition_tags,omitempty"`
	PartitionIDs         []int64                    `protobuf:"varint,6,rep,packed,name=partitionIDs,proto3" json:"partitionIDs,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                   `json:"-"`
	XXX_unrecognized     []byte                     `json:"-"`
	XXX_sizecache        int32                      `json:"-"`
}

func (m *CollectionMeta) Reset()         { *m = CollectionMeta{} }
func (m *CollectionMeta) String() string { return proto.CompactTextString(m) }
func (*CollectionMeta) ProtoMessage()    {}
func (*CollectionMeta) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{5}
}

func (m *CollectionMeta) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CollectionMeta.Unmarshal(m, b)
}
func (m *CollectionMeta) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CollectionMeta.Marshal(b, m, deterministic)
}
func (m *CollectionMeta) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CollectionMeta.Merge(m, src)
}
func (m *CollectionMeta) XXX_Size() int {
	return xxx_messageInfo_CollectionMeta.Size(m)
}
func (m *CollectionMeta) XXX_DiscardUnknown() {
	xxx_messageInfo_CollectionMeta.DiscardUnknown(m)
}

var xxx_messageInfo_CollectionMeta proto.InternalMessageInfo

func (m *CollectionMeta) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *CollectionMeta) GetSchema() *schemapb.CollectionSchema {
	if m != nil {
		return m.Schema
	}
	return nil
}

func (m *CollectionMeta) GetCreateTime() uint64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

func (m *CollectionMeta) GetSegmentIDs() []int64 {
	if m != nil {
		return m.SegmentIDs
	}
	return nil
}

func (m *CollectionMeta) GetPartitionTags() []string {
	if m != nil {
		return m.PartitionTags
	}
	return nil
}

func (m *CollectionMeta) GetPartitionIDs() []int64 {
	if m != nil {
		return m.PartitionIDs
	}
	return nil
}

type CredentialInfo struct {
	Username string `protobuf:"bytes,1,opt,name=username,proto3" json:"username,omitempty"`
	// encrypted by bcrypt (for higher security level)
	EncryptedPassword string `protobuf:"bytes,2,opt,name=encrypted_password,json=encryptedPassword,proto3" json:"encrypted_password,omitempty"`
	Tenant            string `protobuf:"bytes,3,opt,name=tenant,proto3" json:"tenant,omitempty"`
	IsSuper           bool   `protobuf:"varint,4,opt,name=is_super,json=isSuper,proto3" json:"is_super,omitempty"`
	// encrypted by sha256 (for good performance in cache mapping)
	Sha256Password       string   `protobuf:"bytes,5,opt,name=sha256_password,json=sha256Password,proto3" json:"sha256_password,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CredentialInfo) Reset()         { *m = CredentialInfo{} }
func (m *CredentialInfo) String() string { return proto.CompactTextString(m) }
func (*CredentialInfo) ProtoMessage()    {}
func (*CredentialInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_975d306d62b73e88, []int{6}
}

func (m *CredentialInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CredentialInfo.Unmarshal(m, b)
}
func (m *CredentialInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CredentialInfo.Marshal(b, m, deterministic)
}
func (m *CredentialInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CredentialInfo.Merge(m, src)
}
func (m *CredentialInfo) XXX_Size() int {
	return xxx_messageInfo_CredentialInfo.Size(m)
}
func (m *CredentialInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_CredentialInfo.DiscardUnknown(m)
}

var xxx_messageInfo_CredentialInfo proto.InternalMessageInfo

func (m *CredentialInfo) GetUsername() string {
	if m != nil {
		return m.Username
	}
	return ""
}

func (m *CredentialInfo) GetEncryptedPassword() string {
	if m != nil {
		return m.EncryptedPassword
	}
	return ""
}

func (m *CredentialInfo) GetTenant() string {
	if m != nil {
		return m.Tenant
	}
	return ""
}

func (m *CredentialInfo) GetIsSuper() bool {
	if m != nil {
		return m.IsSuper
	}
	return false
}

func (m *CredentialInfo) GetSha256Password() string {
	if m != nil {
		return m.Sha256Password
	}
	return ""
}

func init() {
	proto.RegisterType((*IndexInfo)(nil), "milvus.proto.etcd.IndexInfo")
	proto.RegisterType((*FieldIndexInfo)(nil), "milvus.proto.etcd.FieldIndexInfo")
	proto.RegisterType((*CollectionInfo)(nil), "milvus.proto.etcd.CollectionInfo")
	proto.RegisterType((*PartitionInfo)(nil), "milvus.proto.etcd.PartitionInfo")
	proto.RegisterType((*SegmentIndexInfo)(nil), "milvus.proto.etcd.SegmentIndexInfo")
	proto.RegisterType((*CollectionMeta)(nil), "milvus.proto.etcd.CollectionMeta")
	proto.RegisterType((*CredentialInfo)(nil), "milvus.proto.etcd.CredentialInfo")
}

func init() { proto.RegisterFile("etcd_meta.proto", fileDescriptor_975d306d62b73e88) }

var fileDescriptor_975d306d62b73e88 = []byte{
	// 849 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xbc, 0x55, 0xcd, 0x6e, 0xe4, 0x44,
	0x10, 0x96, 0xc7, 0x99, 0x1f, 0xd7, 0x4c, 0x26, 0x9b, 0x06, 0x56, 0xbd, 0x61, 0x01, 0xef, 0x88,
	0x05, 0x5f, 0x36, 0x23, 0x85, 0x1f, 0x09, 0x21, 0xd0, 0x8a, 0x58, 0x2b, 0x8d, 0x80, 0xd5, 0xa8,
	0x13, 0x71, 0xe0, 0x62, 0xf5, 0xd8, 0x95, 0x4c, 0x0b, 0xbb, 0x6d, 0xb9, 0xdb, 0x81, 0xec, 0x13,
	0xf0, 0x06, 0x3c, 0x0a, 0x57, 0x2e, 0x3c, 0x0d, 0x2f, 0x81, 0xdc, 0xfe, 0x19, 0x7b, 0x66, 0xc3,
	0x91, 0x63, 0x7d, 0xd5, 0x55, 0xee, 0xaf, 0xea, 0xeb, 0xcf, 0x70, 0x82, 0x3a, 0x8c, 0x82, 0x04,
	0x35, 0x3f, 0xcf, 0xf2, 0x54, 0xa7, 0xe4, 0x34, 0x11, 0xf1, 0x5d, 0xa1, 0xaa, 0xe8, 0xbc, 0xcc,
	0x9e, 0xcd, 0xc2, 0x34, 0x49, 0x52, 0x59, 0x41, 0x67, 0x33, 0x15, 0x6e, 0x31, 0xa9, 0x8f, 0x2f,
	0xfe, 0xb6, 0xc0, 0x59, 0xc9, 0x08, 0x7f, 0x5b, 0xc9, 0x9b, 0x94, 0x7c, 0x00, 0x20, 0xca, 0x20,
	0x90, 0x3c, 0x41, 0x6a, 0xb9, 0x96, 0xe7, 0x30, 0xc7, 0x20, 0xaf, 0x79, 0x82, 0x84, 0xc2, 0xd8,
	0x04, 0x2b, 0x9f, 0x0e, 0x5c, 0xcb, 0xb3, 0x59, 0x13, 0x12, 0x1f, 0x66, 0x55, 0x61, 0xc6, 0x73,
	0x9e, 0x28, 0x6a, 0xbb, 0xb6, 0x37, 0xbd, 0x78, 0x76, 0xde, 0xbb, 0x4c, 0x7d, 0x8d, 0xef, 0xf1,
	0xfe, 0x27, 0x1e, 0x17, 0xb8, 0xe6, 0x22, 0x67, 0x53, 0x53, 0xb6, 0x36, 0x55, 0x65, 0xff, 0x08,
	0x63, 0xd4, 0x18, 0xd1, 0x23, 0xd7, 0xf2, 0x26, 0xac, 0x09, 0xc9, 0x47, 0x30, 0x0d, 0x73, 0xe4,
	0x1a, 0x03, 0x2d, 0x12, 0xa4, 0x43, 0xd7, 0xf2, 0x8e, 0x18, 0x54, 0xd0, 0xb5, 0x48, 0x70, 0xe1,
	0xc3, 0xfc, 0x95, 0xc0, 0x38, 0xda, 0x71, 0xa1, 0x30, 0xbe, 0x11, 0x31, 0x46, 0x2b, 0xdf, 0x10,
	0xb1, 0x59, 0x13, 0x3e, 0x4c, 0x63, 0xf1, 0xd7, 0x10, 0xe6, 0x97, 0x69, 0x1c, 0x63, 0xa8, 0x45,
	0x2a, 0x4d, 0x9b, 0x39, 0x0c, 0xda, 0x0e, 0x83, 0x95, 0x4f, 0xbe, 0x81, 0x51, 0x35, 0x40, 0x53,
	0x3b, 0xbd, 0x78, 0xde, 0xe7, 0x58, 0x0f, 0x77, 0xd7, 0xe4, 0xca, 0x00, 0xac, 0x2e, 0xda, 0x27,
	0x62, 0xef, 0x13, 0x21, 0x0b, 0x98, 0x65, 0x3c, 0xd7, 0xc2, 0x5c, 0xc0, 0x57, 0xf4, 0xc8, 0xb5,
	0x3d, 0x9b, 0xf5, 0x30, 0xf2, 0x09, 0xcc, 0xdb, 0xb8, 0x5c, 0x8c, 0xa2, 0x43, 0xd7, 0xf6, 0x1c,
	0xb6, 0x87, 0x92, 0x57, 0x70, 0x7c, 0x53, 0x0e, 0x25, 0x30, 0xfc, 0x50, 0xd1, 0xd1, 0xdb, 0xd6,
	0x52, 0x6a, 0xe4, 0xbc, 0x3f, 0x3c, 0x36, 0xbb, 0x69, 0x63, 0x54, 0xe4, 0x02, 0xde, 0xbb, 0x13,
	0xb9, 0x2e, 0x78, 0x1c, 0x84, 0x5b, 0x2e, 0x25, 0xc6, 0x46, 0x20, 0x8a, 0x8e, 0xcd, 0x67, 0xdf,
	0xa9, 0x93, 0x97, 0x55, 0xae, 0xfa, 0xf6, 0xe7, 0xf0, 0x38, 0xdb, 0xde, 0x2b, 0x11, 0x1e, 0x14,
	0x4d, 0x4c, 0xd1, 0xbb, 0x4d, 0xb6, 0x57, 0xf5, 0x12, 0x9e, 0xb6, 0x1c, 0x82, 0x6a, 0x2a, 0x91,
	0x99, 0x94, 0xd2, 0x3c, 0xc9, 0x14, 0x75, 0x5c, 0xdb, 0x3b, 0x62, 0x67, 0xed, 0x99, 0xcb, 0xea,
	0xc8, 0x75, 0x7b, 0xa2, 0x94, 0xb0, 0xda, 0xf2, 0x3c, 0x52, 0x81, 0x2c, 0x12, 0x0a, 0xae, 0xe5,
	0x0d, 0x99, 0x53, 0x21, 0xaf, 0x8b, 0x84, 0xac, 0xe0, 0x44, 0x69, 0x9e, 0xeb, 0x20, 0x4b, 0x95,
	0xe9, 0xa0, 0xe8, 0xd4, 0x0c, 0xc5, 0x7d, 0x48, 0xab, 0x3e, 0xd7, 0xdc, 0x48, 0x75, 0x6e, 0x0a,
	0xd7, 0x4d, 0x1d, 0x61, 0x70, 0x1a, 0xa6, 0x52, 0x09, 0xa5, 0x51, 0x86, 0xf7, 0x41, 0x8c, 0x77,
	0x18, 0xd3, 0x99, 0x6b, 0x79, 0xf3, 0x7d, 0x51, 0xd4, 0xcd, 0x2e, 0x77, 0xa7, 0x7f, 0x28, 0x0f,
	0xb3, 0x47, 0xe1, 0x1e, 0x42, 0x5e, 0x02, 0xb4, 0xdc, 0x14, 0x3d, 0x7e, 0xdb, 0xcd, 0xcc, 0xba,
	0xd6, 0xad, 0x1c, 0xca, 0x6d, 0x75, 0x6a, 0x16, 0x7f, 0x58, 0x70, 0xdc, 0xcb, 0x12, 0x17, 0xa6,
	0x1d, 0xf5, 0xd4, 0x52, 0xee, 0x42, 0xe4, 0x63, 0x38, 0xee, 0x29, 0xc7, 0x48, 0xdb, 0x61, 0x7d,
	0x90, 0x7c, 0x0b, 0xef, 0xff, 0xc7, 0x6e, 0x6a, 0x29, 0x3f, 0x79, 0x70, 0x35, 0x8b, 0xdf, 0x07,
	0xf0, 0xe8, 0x0a, 0x6f, 0x13, 0x94, 0x7a, 0xf7, 0x4a, 0x17, 0x30, 0x0b, 0x77, 0x0f, 0xae, 0xb9,
	0x5d, 0x0f, 0xdb, 0x27, 0x30, 0x38, 0x24, 0xf0, 0x14, 0x1c, 0x55, 0x77, 0xf6, 0xcd, 0x45, 0x6c,
	0xb6, 0x03, 0x2a, 0x27, 0x28, 0xe5, 0xec, 0x1b, 0x5b, 0x31, 0x4e, 0x60, 0xc2, 0xae, 0x13, 0x0c,
	0xfb, 0x86, 0x46, 0x61, 0xbc, 0x29, 0x84, 0xa9, 0x19, 0x55, 0x99, 0x3a, 0x24, 0xcf, 0x60, 0x86,
	0x92, 0x6f, 0x62, 0xac, 0x5e, 0x15, 0x1d, 0x1b, 0xa7, 0x9a, 0x56, 0x98, 0x21, 0xb6, 0xff, 0xc8,
	0x27, 0x07, 0x6e, 0xf5, 0x8f, 0xd5, 0xf5, 0x99, 0x1f, 0x51, 0xf3, 0xff, 0xdd, 0x67, 0x3e, 0x04,
	0x68, 0x27, 0xd4, 0xb8, 0x4c, 0x07, 0x21, 0xcf, 0x3b, 0x1e, 0x13, 0x68, 0x7e, 0xdb, 0x78, 0xcc,
	0x4e, 0x14, 0xd7, 0xfc, 0x56, 0x1d, 0xd8, 0xd5, 0xe8, 0xd0, 0xae, 0x16, 0x7f, 0x96, 0x6c, 0x73,
	0x8c, 0x50, 0x6a, 0xc1, 0x63, 0xb3, 0xf6, 0x33, 0x98, 0x14, 0x0a, 0xf3, 0xce, 0x6f, 0xa6, 0x8d,
	0xc9, 0x0b, 0x20, 0x28, 0xc3, 0xfc, 0x3e, 0x2b, 0xf5, 0x95, 0x71, 0xa5, 0x7e, 0x4d, 0xf3, 0xa8,
	0x96, 0xe4, 0x69, 0x9b, 0x59, 0xd7, 0x09, 0xf2, 0x18, 0x46, 0x1a, 0x25, 0x97, 0xda, 0x90, 0x74,
	0x58, 0x1d, 0x91, 0x27, 0x30, 0x11, 0x2a, 0x50, 0x45, 0x86, 0x79, 0xf3, 0x37, 0x11, 0xea, 0xaa,
	0x0c, 0xc9, 0xa7, 0x70, 0xa2, 0xb6, 0xfc, 0xe2, 0x8b, 0x2f, 0x77, 0xed, 0x87, 0xa6, 0x76, 0x5e,
	0xc1, 0x4d, 0xef, 0xef, 0xbe, 0xfe, 0xf9, 0xab, 0x5b, 0xa1, 0xb7, 0xc5, 0xa6, 0x7c, 0xc2, 0xcb,
	0x37, 0x22, 0x8e, 0xc5, 0x1b, 0x8d, 0xe1, 0x76, 0x59, 0xed, 0xe2, 0xc5, 0x86, 0x87, 0xbf, 0x14,
	0xd9, 0x52, 0x48, 0x5d, 0xde, 0x3d, 0x5e, 0x9a, 0xdd, 0x2c, 0xcb, 0x17, 0x9a, 0x6d, 0x36, 0x23,
	0x13, 0x7d, 0xf6, 0x6f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x4d, 0x71, 0xaf, 0xc4, 0xa3, 0x07, 0x00,
	0x00,
}