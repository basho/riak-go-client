package riak_dt

// DtUpdateReq

func (m *DtUpdateReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *DtUpdateReq) BucketIsRequired() bool {
	return true
}

func (m *DtUpdateReq) KeyIsRequired() bool {
	return false
}

// DtFetchReq

func (m *DtFetchReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *DtFetchReq) BucketIsRequired() bool {
	return true
}

func (m *DtFetchReq) KeyIsRequired() bool {
	return true
}
