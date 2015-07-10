package riak

// RpbGetReq "extension" methods

func (m *RpbGetBucketReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbGetBucketReq) KeyIsRequired() bool {
	return false
}
