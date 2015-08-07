package riak

import (
	"fmt"
	rpbRiak "github.com/basho/riak-go-client/rpb/riak"
	rpbRiakKV "github.com/basho/riak-go-client/rpb/riak_kv"
	"time"
)

type Link struct {
	Bucket string
	Key    string
	Tag    string
}

type Pair struct {
	Key   string
	Value string
}

type Object struct {
	BucketType      string
	Bucket          string
	Key             string
	IsTombstone     bool
	Value           []byte
	ContentType     string
	Charset         string
	ContentEncoding string
	VTag            string
	LastModified    time.Time
	UserMeta        []*Pair
	Indexes         map[string][]string // TODO int indexes vs string
	Links           []*Link
	VClock          []byte
}

func (o *Object) HasIndexes() bool {
	return len(o.Indexes) > 0
}

func (o *Object) HasUserMeta() bool {
	return len(o.UserMeta) > 0
}

func (o *Object) HasLinks() bool {
	return len(o.Links) > 0
}

func (o *Object) AddToIntIndex(indexName string, indexValue int) {
	o.AddToIndex(indexName, fmt.Sprintf("%v", indexValue))
}

func (o *Object) AddToIndex(indexName string, indexValue string) {
	if o.Indexes == nil {
		o.Indexes = make(map[string][]string)
	}
	if o.Indexes[indexName] == nil {
		o.Indexes[indexName] = make([]string, 1)
		o.Indexes[indexName][0] = indexValue
	} else {
		o.Indexes[indexName] = append(o.Indexes[indexName], indexValue)
	}
}

func fromRpbContent(rpbContent *rpbRiakKV.RpbContent) (ro *Object, err error) {
	// NB: ro = "Riak Object"
	ro = &Object{
		IsTombstone: rpbContent.GetDeleted(),
	}

	if ro.IsTombstone {
		ro.Value = nil
	} else {
		// TODO deserialization?
		ro.Value = rpbContent.GetValue()
	}

	ro.ContentType = string(rpbContent.GetContentType())
	ro.Charset = string(rpbContent.GetCharset())
	ro.ContentEncoding = string(rpbContent.GetContentEncoding())
	ro.VTag = string(rpbContent.GetVtag())
	ro.LastModified = time.Unix(int64(rpbContent.GetLastMod()), int64(rpbContent.GetLastModUsecs()))

	rpbUserMeta := rpbContent.GetUsermeta()
	if len(rpbUserMeta) > 0 {
		ro.UserMeta = make([]*Pair, len(rpbUserMeta))
		for i, userMeta := range rpbUserMeta {
			ro.UserMeta[i] = &Pair{
				Key:   string(userMeta.Key),
				Value: string(userMeta.Value),
			}
		}
	}

	rpbIndexes := rpbContent.GetIndexes()
	if len(rpbIndexes) > 0 {
		ro.Indexes = make(map[string][]string)
		for _, index := range rpbIndexes {
			indexName := string(index.Key)
			// TODO int indexes
			indexValue := string(index.Value)
			if ro.Indexes[indexName] == nil {
				ro.Indexes[indexName] = make([]string, 1)
				ro.Indexes[indexName][0] = indexValue
			} else {
				ro.Indexes[indexName] = append(ro.Indexes[indexName], indexValue)
			}
		}
	}

	rpbLinks := rpbContent.GetLinks()
	if len(rpbLinks) > 0 {
		ro.Links = make([]*Link, len(rpbLinks))
		for i, link := range rpbLinks {
			ro.Links[i] = &Link{
				Bucket: string(link.Bucket),
				Key:    string(link.Key),
				Tag:    string(link.Tag),
			}
		}
	}

	return
}

func toRpbContent(ro *Object) (*rpbRiakKV.RpbContent, error) {
	rpbContent := &rpbRiakKV.RpbContent{
		Value:           ro.Value,
		ContentType:     []byte(ro.ContentType),
		Charset:         []byte(ro.Charset),
		ContentEncoding: []byte(ro.ContentEncoding),
	}

	if ro.HasIndexes() {
		count := 0
		for _, idxValues := range ro.Indexes {
			count += len(idxValues)
		}
		idx := 0
		rpbIndexes := make([]*rpbRiak.RpbPair, count)
		for idxName, idxValues := range ro.Indexes {
			idxNameBytes := []byte(idxName)
			for _, idxVal := range idxValues {
				pair := &rpbRiak.RpbPair{
					Key:   idxNameBytes,
					Value: []byte(idxVal),
				}
				rpbIndexes[idx] = pair
				idx++
			}
		}
		rpbContent.Indexes = rpbIndexes
	}

	if ro.HasUserMeta() {
		rpbUserMeta := make([]*rpbRiak.RpbPair, len(ro.UserMeta))
		for i, userMeta := range ro.UserMeta {
			rpbUserMeta[i] = &rpbRiak.RpbPair{
				Key:   []byte(userMeta.Key),
				Value: []byte(userMeta.Value),
			}
		}
		rpbContent.Usermeta = rpbUserMeta
	}

	if ro.HasLinks() {
		rpbLinks := make([]*rpbRiakKV.RpbLink, len(ro.Links))
		for i, link := range ro.Links {
			rpbLinks[i] = &rpbRiakKV.RpbLink{
				Bucket: []byte(link.Bucket),
				Key:    []byte(link.Key),
				Tag:    []byte(link.Tag),
			}
		}
		rpbContent.Links = rpbLinks
	}

	return rpbContent, nil
}
