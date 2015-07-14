package riak_kv

// RpbGetReq

func (m *RpbGetReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbGetReq) BucketIsRequired() bool {
	return true
}

func (m *RpbGetReq) KeyIsRequired() bool {
	return true
}

// RpbPutReq

func (m *RpbPutReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbPutReq) BucketIsRequired() bool {
	return true
}

func (m *RpbPutReq) KeyIsRequired() bool {
	return false
}

// RpbDelReq

func (m *RpbDelReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbDelReq) BucketIsRequired() bool {
	return true
}

func (m *RpbDelReq) KeyIsRequired() bool {
	return true
}

// RpbListBucketsReq

func (m *RpbListBucketsReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbListBucketsReq) BucketIsRequired() bool {
	return false
}

func (m *RpbListBucketsReq) GetBucket() []byte {
	return nil
}

func (m *RpbListBucketsReq) KeyIsRequired() bool {
	return false
}

func (m *RpbListBucketsReq) GetKey() []byte {
	return nil
}

// RpbListKeysReq

func (m *RpbListKeysReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbListKeysReq) BucketIsRequired() bool {
	return true
}

func (m *RpbListKeysReq) KeyIsRequired() bool {
	return false
}

func (m *RpbListKeysReq) GetKey() []byte {
	return nil
}

// RpbGetBucketKeyPreflistReq

func (m *RpbGetBucketKeyPreflistReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbGetBucketKeyPreflistReq) BucketIsRequired() bool {
	return true
}

func (m *RpbGetBucketKeyPreflistReq) KeyIsRequired() bool {
	return true
}

// RpbIndexReq

func (m *RpbIndexReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbIndexReq) BucketIsRequired() bool {
	return true
}

func (m *RpbIndexReq) KeyIsRequired() bool {
	return false
}
