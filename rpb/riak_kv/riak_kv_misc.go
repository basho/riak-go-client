package riak_kv

// RpbGetReq "extension" methods

func (m *RpbGetReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbGetReq) KeyIsRequired() bool {
	return true
}

// RpbPutReq "extension" methods

func (m *RpbPutReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbPutReq) KeyIsRequired() bool {
	return false
}
