package core

type MilvusSource struct {
	//params    paramtable.ComponentParam
	proxyAddr string
	//datacoordAddr string
}

func (m *MilvusSource) GetProxyAddr() string {
	return m.proxyAddr
}

//func (m *MilvusSource) GetDatacoordAddr() string {
//	return m.datacoordAddr
//}

//func (m *MilvusSource) GetParams() paramtable.ComponentParam {
//	return m.params
//}
