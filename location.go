package riak

import "errors"

type Location struct {
	BucketType string
	Bucket     string
	Key        string
}

func (l *Location) GetBucketType() string {
	return l.BucketType
}

func (l *Location) SetBucketType(bucketType string) {
	l.BucketType = bucketType
}

func (l *Location) GetBucket() string {
	return l.BucketType
}

func (l *Location) GetKey() string {
	return l.Key
}

type objectLocator interface {
	GetBucketType() string
	SetBucketType(string)
	GetBucket() string
	GetKey() string
}

func validateObjectLocator(arg objectLocator) error {
	if arg.GetBucketType() == "" {
		arg.SetBucketType("default")
	}
	if arg.GetBucket() == "" {
		return errors.New("Bucket is required")
	}
	if arg.GetKey() == "" {
		return errors.New("Key is required")
	}
	return nil
}
