package core

// MilvusMeta is interface to operate milvus meta
type MilvusMeta interface {
}

// makes sure EtcdMilvusMeta implements `MilvusMeta`
var _ MilvusMeta = (*EtcdMilvusMeta)(nil)

type EtcdMilvusMeta struct {
}
