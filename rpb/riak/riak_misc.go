package riak

// RpbGetBucketTypeReq

func (m *RpbGetBucketTypeReq) GetKey() []byte {
	return nil
}

func (m *RpbGetBucketTypeReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbGetBucketTypeReq) BucketIsRequired() bool {
	return false
}

func (m *RpbGetBucketTypeReq) GetBucket() []byte {
	return nil
}

func (m *RpbGetBucketTypeReq) KeyIsRequired() bool {
	return false
}

// RpbGetBucketReq

func (m *RpbGetBucketReq) GetKey() []byte {
	return nil
}

func (m *RpbGetBucketReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbGetBucketReq) BucketIsRequired() bool {
	return true
}

func (m *RpbGetBucketReq) KeyIsRequired() bool {
	return false
}

// RpbSetBucketTypeReq

func (m *RpbSetBucketTypeReq) GetKey() []byte {
	return nil
}

func (m *RpbSetBucketTypeReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbSetBucketTypeReq) BucketIsRequired() bool {
	return false
}

func (m *RpbSetBucketTypeReq) GetBucket() []byte {
	return nil
}

func (m *RpbSetBucketTypeReq) KeyIsRequired() bool {
	return false
}

// RpbSetBucketReq

func (m *RpbSetBucketReq) GetKey() []byte {
	return nil
}

func (m *RpbSetBucketReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbSetBucketReq) BucketIsRequired() bool {
	return true
}

func (m *RpbSetBucketReq) KeyIsRequired() bool {
	return false
}

// RpbResetBucketReq

func (m *RpbResetBucketReq) GetKey() []byte {
	return nil
}

func (m *RpbResetBucketReq) SetType(bt []byte) {
	m.Type = bt
}

func (m *RpbResetBucketReq) BucketIsRequired() bool {
	return true
}

func (m *RpbResetBucketReq) KeyIsRequired() bool {
	return false
}
