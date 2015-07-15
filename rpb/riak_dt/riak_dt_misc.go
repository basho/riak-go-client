package riak_dt

// DtUpdateReq

func (m *DtUpdateReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *DtUpdateReq) BucketIsRequired() bool {
	return true
}

func (m *DtUpdateReq) KeyIsRequired() bool {
	return true
}
