package main

import (
	"fmt"
	"os"
	"sync"

	riak "github.com/basho/riak-go-client"
)

/*
   Code samples from:
   http://docs.basho.com/riak/latest/dev/using/2i/

   make sure the 'indexes' bucket-type is created using the leveldb backend
*/

func main() {
	//riak.EnableDebugLogging = true

	nodeOpts := &riak.NodeOptions{
		RemoteAddress: "riak-test:10017",
	}

	var node *riak.Node
	var err error
	if node, err = riak.NewNode(nodeOpts); err != nil {
		fmt.Println(err.Error())
	}

	nodes := []*riak.Node{node}
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	cluster, err := riak.NewCluster(opts)
	if err != nil {
		fmt.Println(err.Error())
	}

	defer func() {
		if err := cluster.Stop(); err != nil {
			fmt.Println(err.Error())
		}
	}()

	if err := cluster.Start(); err != nil {
		fmt.Println(err.Error())
	}

	// ping
	ping := &riak.PingCommand{}
	if err := cluster.Execute(ping); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("ping passed")
	}

	if err := insertingObjects(cluster); err != nil {
		ErrExit(err)
	}

	if err := queryingIndexes(cluster); err != nil {
		ErrExit(err)
	}

	if err := indexingObjects(cluster); err != nil {
		ErrExit(err)
	}

	/*
		invalid_field_names,
		incorrect_data_type,
		querying_exact_match,
		querying_range,
		querying_range_with_terms,
		querying_pagination
	*/
}

func ErrExit(err error) {
	os.Stderr.WriteString(err.Error())
	os.Exit(1)
}

func printIndexQueryResults(cmd riak.Command) {
	sciq := cmd.(*riak.SecondaryIndexQueryCommand)
	for _, r := range sciq.Response.Results {
		fmt.Println("[DevUsing2i] index key:", string(r.IndexKey), "object key:", string(r.ObjectKey))
	}
}

func insertingObjects(cluster *riak.Cluster) error {
	obj := &riak.Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		BucketType:      "indexes",
		Bucket:          "users",
		Key:             "john_smith",
		Value:           []byte("…user data…"),
	}

	obj.AddToIndex("twitter_bin", "jsmith123")
	obj.AddToIndex("email_bin", "jsmith@basho.com")

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithContent(obj).
		Build()
	if err != nil {
		return err
	}

	if err := cluster.Execute(cmd); err != nil {
		return err
	}

	return nil;
}

func queryingIndexes(cluster *riak.Cluster) error {
	cmd, err := riak.NewSecondaryIndexQueryCommandBuilder().
		WithBucketType("indexes").
		WithBucket("users").
		WithIndexName("twitter_bin").
		WithIndexKey("jsmith123").
		Build()
	if err != nil {
		return err
	}

	if err := cluster.Execute(cmd); err != nil {
		return err
	}

	printIndexQueryResults(cmd)
	return nil
}

func indexingObjects(cluster *riak.Cluster) error {
	o1 := &riak.Object{
		Key:             "larry",
		Value:           []byte("My name is Larry"),
	}
	o1.AddToIndex("field1_bin", "val1")
	o1.AddToIntIndex("field2_int", 1001)

	o2 := &riak.Object{
		Key:             "Moe",
		Value:           []byte("My name is Moe"),
	}
	o2.AddToIndex("Field1_bin", "val2")
	o2.AddToIntIndex("Field2_int", 1002)

	o3 := &riak.Object{
		Key:             "curly",
		Value:           []byte("My name is Curly"),
	}
	o3.AddToIndex("FIELD1_BIN", "val3")
	o3.AddToIntIndex("FIELD2_INT", 1003)

	o4 := &riak.Object{
		Key:             "veronica",
		Value:           []byte("My name is Veronica"),
	}
	o4.AddToIndex("FIELD1_bin", "val4")
	o4.AddToIndex("FIELD1_bin", "val4")
	o4.AddToIndex("FIELD1_bin", "val4a")
	o4.AddToIndex("FIELD1_bin", "val4b")
	o4.AddToIntIndex("FIELD2_int", 1004)
	o4.AddToIntIndex("FIELD2_int", 1005)
	o4.AddToIntIndex("FIELD2_int", 1006)
	o4.AddToIntIndex("FIELD2_int", 1004)
	o4.AddToIntIndex("FIELD2_int", 1004)
	o4.AddToIntIndex("FIELD2_int", 1007)

	objs := [...]*riak.Object{o1, o2, o3, o4}

	wg := &sync.WaitGroup{}
	for _, obj := range objs {
		obj.ContentType = "text/plain"
		obj.Charset = "utf-8"
		obj.ContentEncoding = "utf-8"

		cmd, err := riak.NewStoreValueCommandBuilder().
			WithBucketType("indexes").
			WithBucket("people").
			WithContent(obj).
			Build()
		if err != nil {
			return err
		}

		args := &riak.Async{
			Command: cmd,
			Wait:    wg,
		}
		if err := cluster.ExecuteAsync(args); err != nil {
			return err
		}
	}

	wg.Wait()

	return nil;
}

/*
    function querying_pagination(async_cb) {

        function do_query(continuation) {
            var binIdxCmdBuilder = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
                .withBucketType('indexes')
                .withBucket('tweets')
                .withIndexName('hashtags_bin')
                .withRange('ri', 'ru')
                .withMaxResults(5)
                .withCallback(pagination_cb);

            if (continuation) {
                binIdxCmdBuilder.withContinuation(continuation);
            }

            client.execute(binIdxCmdBuilder.build());
        }

        var query_keys = [];
        function pagination_cb(err, rslt) {
            if (err) {
                logger.error("[DevUsing2i] pagination_cb err: '%s'", err);
                return;
            }

            if (rslt.done) {
                query_keys.forEach(function (key) {
                    logger.info("[DevUsing2i] pagination_cb 2i query key: '%s'", key);
                });
                query_keys = [];

                if (rslt.continuation) {
                    do_query(rslt.continuation);
                }

                async_cb();
            }

            if (rslt.values.length > 0) {
                Array.prototype.push.apply(query_keys,
                    rslt.values.map(function (value) {
                        return value.objectKey;
                    }));
            }
        }

        do_query();
    }

    function querying_exact_match(async_cb) {
        var f1 = function (acb) {
            var binIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
                .withBucketType('indexes')
                .withBucket('people')
                .withIndexName('field1_bin')
                .withIndexKey('val1')
                .withCallback(function (err, rslt) {
                    query_cb(err, rslt);
                    if (!rslt || rslt.done) {
                        acb();
                    }
                }).build();
            client.execute(binIdxCmd);
        };

        var f2 = function (acb) {
            var intIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
                .withBucketType('indexes')
                .withBucket('people')
                .withIndexName('field2_int')
                .withIndexKey(1001)
                .withCallback(function (err, rslt) {
                    query_cb(err, rslt);
                    if (!rslt || rslt.done) {
                        acb();
                    }
                }).build();
            client.execute(intIdxCmd);
        };

        async.parallel([f1, f2], function (err, rslts) {
            if (err) {
                logger.error("[DevUsing2i] querying_exact_match err: '%s'", err);
            }
            async_cb();
        });
    }

    function querying_range_with_terms(async_cb) {
        var binIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('tweets')
            .withIndexName('hashtags_bin')
            .withRange('rock', 'rocl')
            .withReturnKeyAndIndex(true)
            .withCallback(function (err, rslt) {
                query_cb(err, rslt);
                if (!rslt || rslt.done) {
                    async_cb();
                }
            }).build();
        client.execute(binIdxCmd);
    }   

    function querying_range(async_cb) {
        var f1 = function (acb) {
            var binIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
                .withBucketType('indexes')
                .withBucket('people')
                .withIndexName('field1_bin')
                .withRange('val2', 'val4')
                .withCallback(function (err, rslt) {
                    query_cb(err, rslt);
                    if (!rslt || rslt.done) {
                        acb();
                    }
                }).build();
            client.execute(binIdxCmd);
        };

        var f2 = function (acb) {
            var intIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
                .withBucketType('indexes')
                .withBucket('people')
                .withIndexName('field2_int')
                .withRange(1002, 1004)
                .withCallback(function (err, rslt) {
                    query_cb(err, rslt);
                    if (!rslt || rslt.done) {
                        acb();
                    }
                }).build();
            client.execute(intIdxCmd);
        };

        async.parallel([f1, f2], function (err, rslts) {
            if (err) {
                logger.error("[DevUsing2i] querying_range err: '%s'", err);
            }
            async_cb();
        });
    }

    function invalid_field_names(async_cb) {
        var cmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('people')
            .withIndexName('field2_foo')
            .withIndexKey('jsmith123')
            .withCallback(function (err, rslt) {
                query_cb(err, rslt);
                if (!rslt || rslt.done) {
                    async_cb();
                }
            }).build();
        client.execute(cmd);
    }

    function incorrect_data_type(async_cb) {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setBucketType('indexes');
        riakObj.setBucket('people');
        riakObj.setKey('larry');
        riakObj.addToIndex('field2_int', 'bar');
        try {
            client.storeValue({ value: riakObj }, function (err, rslt) {
                logger.error("[DevUsing2i] incorrect_data_type err: '%s'", err);
                async_cb();
            });
        } catch (e) {
            logger.error("[DevUsing2i] incorrect_data_type err: '%s'", e);
        }
        async_cb();
    }

    var query_keys = [];
    function query_cb(err, rslt) {
        if (err) {
            logger.error("[DevUsing2i] query_cb err: '%s'", err);
            return;
        }

        if (rslt.done) {
            query_keys.forEach(function (key) {
                logger.info("[DevUsing2i] query_cb 2i query key: '%s'", key);
            });
            query_keys = [];
        }

        if (rslt.values.length > 0) {
            Array.prototype.push.apply(query_keys,
                rslt.values.map(function (value) {
                    return value.objectKey;
                }));
        }
    }
}
*/
