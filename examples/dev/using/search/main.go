package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	riak "github.com/basho/riak-go-client"
)

/*
   Code samples from:
   http://docs.basho.com/riak/latest/dev/using/search/

   make sure these bucket-types are created:
   'animals', 'quotes', 'sports', 'cars', 'users', 'n_val_of_5'
*/

func main() {
	//riak.EnableDebugLogging = true

	nodeOpts := &riak.NodeOptions{
		RemoteAddress: "riak-test:10017",
		RequestTimeout: time.Second * 60,
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

	if err := storeIndex(cluster); err != nil {
		ErrExit(err)
	}

	if err := storeBucketProperties(cluster); err != nil {
		ErrExit(err)
	}

	if err := storeObjects(cluster); err != nil {
		ErrExit(err)
	}

	time.Sleep(time.Millisecond * 1250)

	if err := doSearchRequest(cluster); err != nil {
		ErrExit(err)
	}
}

func ErrExit(err error) {
	os.Stderr.WriteString(err.Error())
	os.Exit(1)
}

func storeIndex(cluster *riak.Cluster) error {
	cmd, err := riak.NewStoreIndexCommandBuilder().
		WithIndexName("famous").
		WithSchemaName("_yz_default").
		WithTimeout(time.Second * 30).
		Build()
	if err != nil {
		return err
	}

	return cluster.Execute(cmd)
}

func storeBucketProperties(cluster *riak.Cluster) error {
	cmd, err := riak.NewStoreBucketPropsCommandBuilder().
		WithBucketType("animals").
		WithBucket("cats").
		WithSearchIndex("famous").
		Build()
	if err != nil {
		return err
	}

	return cluster.Execute(cmd)
}

func storeObjects(cluster *riak.Cluster) error {
	o1 := &riak.Object{
		Key:             "liono",
		Value:           []byte("{\"name_s\":\"Lion-o\",\"age_i\":30,\"leader_b\":true}"),
	}
	o2 := &riak.Object{
		Key:             "cheetara",
		Value:           []byte("{\"name_s\":\"Cheetara\",\"age_i\":30,\"leader_b\":false}"),
	}
	o3 := &riak.Object{
		Key:             "snarf",
		Value:           []byte("{\"name_s\":\"Snarf\",\"age_i\":43,\"leader_b\":false}"),
	}
	o4 := &riak.Object{
		Key:             "panthro",
		Value:           []byte("{\"name_s\":\"Panthro\",\"age_i\":36,\"leader_b\":false}"),
	}

	objs := [...]*riak.Object{o1, o2, o3, o4}

	wg := &sync.WaitGroup{}
	for _, obj := range objs {
		obj.ContentType = "application/json"
		obj.Charset = "utf-8"
		obj.ContentEncoding = "utf-8"

		cmd, err := riak.NewStoreValueCommandBuilder().
			WithBucketType("animals").
			WithBucket("cats").
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

func doSearchRequest(cluster *riak.Cluster) error {
	cmd, err := riak.NewSearchCommandBuilder().
		WithIndexName("famous").
		WithQuery("name_s:Lion*").
		Build();
	if err != nil {
		return err
	}

	if err := cluster.Execute(cmd); err != nil {
		return err
	}
	
	sc := cmd.(*riak.SearchCommand)
	if json, jerr := json.MarshalIndent(sc.Response.Docs, "", "  "); jerr != nil {
		return jerr
	} else {
		fmt.Println(string(json))
	}

	return nil
}

/*
            var doc = rslt.docs.pop();
            var args = {
                bucketType: doc._yz_rt,
                bucket: doc._yz_rb,
                key: doc._yz_rk,
                convertToJs: true
            };
            client.fetchValue(args, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                logger.info("[DevUsingSearch] first doc:", rslt.values[0].value);
                do_age_search_request();
            });
        }

        var search = new Riak.Commands.YZ.Search.Builder()
            .withIndexName('famous')
            .withQuery('name_s:Lion*')
            .withCallback(search_cb)
            .build();
        client.execute(search);
    }

    function do_age_search_request() {
        function search_cb(err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("[DevUsingSearch] age search docs:", JSON.stringify(rslt.docs));
            do_and_search_request();
        }

        var search = new Riak.Commands.YZ.Search.Builder()
            .withIndexName('famous')
            .withQuery('age_i:[30 TO *]')
            .withCallback(search_cb)
            .build();
        client.execute(search);
    }

    function do_and_search_request() {
        function search_cb(err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("[DevUsingSearch] AND search docs:", JSON.stringify(rslt.docs));
            paginated_search_request();
        }

        var search = new Riak.Commands.YZ.Search.Builder()
            .withIndexName('famous')
            .withQuery('leader_b:true AND age_i:[30 TO *]')
            .withCallback(search_cb)
            .build();
        client.execute(search);
    }

    function paginated_search_request() {
        function search_cb(err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("[DevUsingSearch] paginated search docs:", JSON.stringify(rslt.docs));
            delete_search_index();
        }

        var rowsPerPage = 2;
        var page = 2;
        var start = rowsPerPage * (page - 1);

        var search = new Riak.Commands.YZ.Search.Builder()
            .withIndexName('famous')
            .withQuery('*:*')
            .withStart(start)
            .withNumRows(rowsPerPage)
            .withCallback(search_cb)
            .build();
        client.execute(search);
    }

    function delete_search_index() {
        function delete_cb(err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (rslt !== true) {
                logger.error("[DevUsingSearch] DeleteIndex false result!");
            }
            client.stop(function () {
                done();
            });
        }

        var bucketProps_cb = function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (rslt === true) {
                var deleteCmd = new Riak.Commands.YZ.DeleteIndex.Builder()
                    .withIndexName('famous')
                    .withCallback(delete_cb)
                    .build();
                client.execute(deleteCmd);
            } else {
                logger.error("[DevUsingSearch] StoreBucketProps false result!");
                client.stop(function () {
                    done();
                });
            }
        };

        var store = new Riak.Commands.KV.StoreBucketProps.Builder()
            .withBucketType("animals")
            .withBucket("cats")
            .withSearchIndex("_dont_index_")
            .withCallback(bucketProps_cb)
            .build();

        client.execute(store);
    }
}
*/
