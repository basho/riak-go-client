package riak

import (
	rpbRiakKV "github.com/basho-labs/riak-go-client/rpb/riak_kv"
	"time"
)

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
	// TODO int indexes
	Indexes map[string][]string
	Links   []Link
	VClock  []byte
}

func (o *Object) HasIndexes() bool {
	return len(o.Indexes) > 0
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

	/*
	   //links
	   var pbLinks = rpbContent.getLinks();
	   if (pbLinks.length) {
	       var links = new Array(pbLinks.length);
	       var link;
	       for (i = 0; i < pbLinks.length; i++) {
	           link = {};
	           if (pbLinks[i].bucket) {
	               link.bucket = pbLinks[i].bucket.toString('utf8');
	           }
	           if (pbLinks[i].key) {
	               link.key = pbLinks[i].key.toString('utf8');
	           }
	           if (pbLinks[i].tag) {
	               link.tag = pbLinks[i].tag.toString('utf8');
	           }
	           links[i] = link;
	       }
	       ro.links = links;
	   }
	*/
	return
}

type Link struct {
	Bucket string
	Key    string
	Tag    string
}

type Pair struct {
	Key   string
	Value string
}
