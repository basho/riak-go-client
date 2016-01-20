package main

import (
	"fmt"
	"os"
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
		fmt.Println(err.Error())
	}
}

func ErrExit(err error) {
	os.Stderr.WriteString(err.Error())
	os.Exit(1)
}

func storeIndex(cluster *riak.Cluster) error {
	cmd, err := riak.NewStoreIndexCommandBuilder().
		WithIndexName("famous").
		WithTimeout(time.Second * 30).
		Build()
	if err != nil {
		return err
	}

	return cluster.Execute(cmd)
}

/*
var assert = require('assert');
var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingSearch(done) {
    var client = config.createClient(function (err, c) {
        create_famous_index();
    });

    function create_famous_index() {
        var storeIndex_cb = function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (rslt === true) {
                logger.info("[DevUsingSearch] famous index created with _yz_default schema.");
                store_bucket_properties();
            } else {
                logger.error("[DevUsingSearch] StoreIndex false result!");
                client.stop(function () {
                    done();
                });
            }
        };

        var store = new Riak.Commands.YZ.StoreIndex.Builder()
            .withIndexName("famous")
            .withSchemaName("_yz_default")
            .withCallback(storeIndex_cb)
            .build();

        client.execute(store);
    }

    function store_bucket_properties() {
        var bucketProps_cb = function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (rslt === true) {
                logger.info("[DevUsingSearch] cats bucket associated with famous index.");
                store_objects();
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
            .withSearchIndex("famous")
            .withCallback(bucketProps_cb)
            .build();

        client.execute(store);
    }

    function store_objects() {

        function store_cb(err, rslt, async_cb) {
            if (err) {
                throw new Error(err);
            }
            async_cb(null, rslt);
        }

        var objs = [
            [ 'liono', { name_s: 'Lion-o', age_i: 30, leader_b: true } ],
            [ 'cheetara', { name_s: 'Cheetara', age_i: 30, leader_b: false } ],
            [ 'snarf', { name_s: 'Snarf', age_i: 43, leader_b: false } ],
            [ 'panthro', { name_s: 'Panthro', age_i: 36, leader_b: false } ],
        ];

        var storeFuncs = [];
        objs.forEach(function (o) {
            var storeFunc = function (async_cb) {
                var key = o[0];
                var value = o[1];
                var riakObj = new Riak.Commands.KV.RiakObject();
                riakObj.setContentType('application/json');
                riakObj.setBucketType('animals');
                riakObj.setBucket('cats');
                riakObj.setKey(key);
                riakObj.setValue(value);
                client.storeValue({ value: riakObj }, function (err, rslt) {
                    store_cb(err, rslt, async_cb);
                });
            };
            storeFuncs.push(storeFunc);
        });

        async.parallel(storeFuncs, function (err, rslts) {
            if (err) {
                throw new Error(err);
            }
            // NB: wait to let Solr index docs
            logger.info("[DevUsingSearch] four objects stored in cats bucket and indexing.");
            setTimeout(do_search_request, 1250);
        });
    }

    function do_search_request() {
        logger.info("[DevUsingSearch] indexing complete, searching for objects.");

        function search_cb(err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("[DevUsingSearch] docs:", JSON.stringify(rslt.docs));

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
