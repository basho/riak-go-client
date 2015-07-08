package riak

import (
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
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
	ContentEncoding string
	LastModified    time.Time
	UserMeta        []Pair
	// TODO int indexes vs string
	Indexes map[string][]string
	Links   []Link
	VClock  []byte
}

func (o *Object) HasIndexes() bool {
	return len(o.Indexes) > 0
}

func (o *Object) HasUserMeta() bool {
	return len(o.UserMeta) > 0
}

func NewObjectFromRpbContent(rpbContent *rpbRiakKV.RpbContent) (ro *Object, err error) {
	// NB: ro = "Riak Object"
	ro = &Object{
		IsTombstone: rpbContent.GetDeleted(),
	}

	if ro.IsTombstone {
		ro.Value = nil
	} else {
		/* TODO deserialization?
				value := rpbContent.GetValue()
		        // ReturnHead will only retun metadata
		        if (convertToJs && value) {
		            ro.value = JSON.parse(value.toString('utf8'));
		        } else if (value) {
					ro.value = value.toBuffer();
		        }
		*/
		ro.Value = rpbContent.GetValue()
	}

	ro.ContentType = string(rpbContent.GetContentType())
	ro.ContentEncoding = string(rpbContent.GetContentEncoding())
	ro.LastModified = time.Unix(int64(rpbContent.GetLastMod()), int64(rpbContent.GetLastModUsecs()))

	rpbUserMeta := rpbContent.GetUsermeta()
	if len(rpbUserMeta) > 0 {
		ro.UserMeta = make([]Pair, len(rpbUserMeta))
		for i, userMeta := range rpbUserMeta {
			ro.UserMeta[i] = Pair{
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
		ro.Links = make([]Link, len(rpbLinks))
		for i, link := range rpbLinks {
			ro.Links[i] = Link{
				Bucket: string(link.Bucket),
				Key:    string(link.Key),
				Tag:    string(link.Tag),
			}
		}
	}

	return
}
