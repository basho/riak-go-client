<!--- vim:fo=tc:tw=0:
--->

Release Notes
=============

* 1.5.1 - Following PRs addressed:
 * [Improve connection error handling](https://github.com/basho/riak-go-client/pull/48)
* 1.5.0 - Following PRs addressed:
 * [Add `FetchBucketTypePropsCommand` and `StoreBucketTypePropsCommand`](https://github.com/basho/riak-go-client/pull/42)
* 1.4.0 - Following issues / PRs addressed:
 * [Add `ResetBucketCommand`](https://github.com/basho/riak-go-client/pull/35)
 * [Legacy Counter support](https://github.com/basho/riak-go-client/pull/33)
* 1.3.0 - Following issues / PRs addressed:
 * [Add `NoDefaultNode` option to `ClusterOptions`](https://github.com/basho/riak-go-client/pull/28)
 * [`ConnectionManager` / `NodeManager` fixes](https://github.com/basho/riak-go-client/pull/25)
 * [`ConnectionManager` expiration fix](https://github.com/basho/riak-go-client/issues/23)
* 1.2.0 - Following issues / PRs addressed:
 * [Conflict resolver not being passed to Fetch/Store-ValueCommand](https://github.com/basho/riak-go-client/issues/21)
 * [Reduce exported API](https://github.com/basho/riak-go-client/pull/20)
 * [Modify ClientError to trap an inner error if necessary](https://github.com/basho/riak-go-client/pull/19)
* 1.1.0 - Following issues / PRs addressed:
 * [Issues with incrementing counters within Maps](https://github.com/basho/riak-go-client/issues/17)
 * [Extra goroutine in Execute](https://github.com/basho/riak-go-client/issues/16)
 * [Execute does not return error correctly](https://github.com/basho/riak-go-client/isues/15)
* 1.0.0 - Initial release with Riak 2.0 support.
* 1.0.0-beta1 - Initial beta release with Riak 2 support. Command queuing and retrying not implemented yet.

