package core

import "github.com/zilliztech/milvus-backup/core/paramtable"

type MilvusSource struct {
	params    paramtable.BackupParams
	proxyAddr string
	//datacoordAddr string
}

func (m *MilvusSource) GetProxyAddr() string {
	return m.proxyAddr
}

//func (m *MilvusSource) GetDatacoordAddr() string {
//	return m.datacoordAddr
//}

func (m *MilvusSource) GetParams() paramtable.BackupParams {
	return m.params
}
