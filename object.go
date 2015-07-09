package riak

import (
	rpbRiak "github.com/basho-labs/riak-go-client/rpb/riak"
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

	/*
	   var i, pair;
	   if (ro.hasIndexes()) {
	       var allIndexes = ro.getIndexes();
	       for (var indexName in allIndexes) {
	           var indexKeys = allIndexes[indexName];
	           for (i = 0; i < indexKeys.length; i++) {
	               pair = new RpbPair();
	               pair.setKey(new Buffer(indexName));
	               // The Riak API expects string values, even for _int indexes
	               pair.setValue(new Buffer(indexKeys[i].toString()));
	               rpbContent.indexes.push(pair);
	           }
	       }
	   }

	   if (ro.hasUserMeta()) {
	       var userMeta = ro.getUserMeta();
	       for (i = 0; i < userMeta.length; i++) {
	           pair = new RpbPair();
	           pair.setKey(new Buffer(userMeta[i].key));
	           pair.setValue(new Buffer(userMeta[i].value));
	           rpbContent.usermeta.push(pair);
	       }
	   }

	   if (ro.hasLinks()) {
	       var links = ro.getLinks();
	       var pbLink;
	       for (i = 0; i < links.length; i++) {
	           pbLink = new RpbLink();
	           if (links[i].bucket) {
	               pbLink.setBucket(new Buffer(links[i].bucket));
	           }
	           if (links[i].key) {
	               pbLink.setKey(new Buffer(links[i].key));
	           }
	           if (links[i].tag) {
	               pbLink.setTag(new Buffer(links[i].tag));
	           }
	           rpbContent.links.push(pbLink);
	       }
	   }
	*/

	return rpbContent, nil
}
