package core

import "github.com/zilliztech/milvus-backup/internal/util/paramtable"

type MilvusSource struct {
	params paramtable.ComponentParam
}

func (m *MilvusSource) GetParams() paramtable.ComponentParam {
	return m.params
}

func (m *MilvusSource) SetParams(params paramtable.ComponentParam) {
	m.params = params
}
