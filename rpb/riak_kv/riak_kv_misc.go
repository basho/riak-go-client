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

// RpbDelReq "extension" methods

func (m *RpbDelReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbDelReq) KeyIsRequired() bool {
	return true
}
