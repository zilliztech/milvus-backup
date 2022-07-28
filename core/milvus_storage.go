package core

// MilvusStorage is interface to operate milvus data
type MilvusStorage interface {
}

// makes sure MinioMilvusMeta implements `MilvusStorage`
var _ MilvusStorage = (*MinioMilvusMeta)(nil)

type MinioMilvusMeta struct {
}
