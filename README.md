dull
====

Playing around with multilevel-http, node-hashring and express-swim
to build a riak-like distributed leveldb, any message passing between
cluster nodes is http-based.


```sh
$ npm install -g dull
$ mkdir -p data/node1 data/node2 data/node3
$ dull --port 3001 --path ./data/node1
$ dull --port 3002 --path ./data/node2 --join 127.0.0.1:3001
$ dull --port 3003 --path ./data/node3 --join 127.0.0.1:3001
$ curl -X PUT -d '{ "cap": { "n": 3 }  }' http://localhost:3001/dull/bucket/people
$ curl http://localhost:3001/buckets/data
$ curl -X PUT -d '{ "name": "Andrea", "lastname": "Gariboldi", age: 33 }' -H 'Content-Type: application/json' http://localhost:3002/dull/bucket/people/data/andrea
$ curl http://localhost:3002/dull/bucket/people/data/andrea
$ curl -X DELETE http://localhost:3002/dull/bucket/people/data/andrea
$ mkdir data/node4
$ dull --port 3004 --path ./data/node4
$ curl -X POST -d '127.0.0.1:3001' http://localhost:3004/gossip/join
$ curl -X DELETE http://localhost:3004/gossip/leave
$ curl http://localhost:3002/dull/bucket/people/keys
$ curl -X PUT --data-binary @examples/v8.png -H 'Content-Type: image/png' http://127.0.0.1:3001/dull/bucket/people/data/v8.png
```

## CLI arguments

### node options
* `--host <ip address>` (default: `127.0.0.1`): listen address.
* `--port <port>` (default: `3000`): listen port.
* `--path <dir>` (default: `./data`): directory to store data files.
* `--join <active node>` (default: `(empty)`): another node already in the cluster to join it.

### cap controls
* `--cap.n <n>` (default: `3`): number of replicas for a given key.
* `--cap.r <r>` (default: `2`): number of replicas that should respond to a read request, for it to be considered successful.
* `--cap.w <w>` (default: `2`): number of replicas that should respond to a write request, for it to be considered successful.

### vector clocks options
* `--vclock.small <n>` (default: `10`): number of entries to consider a vector clock small.
* `--vclock.big <n>` (default: `50`): number of entries to consider a vector clock big.
* `--vclock.young <n>` (default: `20`): max number of seconds to consider a vector clock entry young.
* `--vclock.old <n>` (default: `86400`): min number of seconds to consider a vector clock entry old.

### ring options (hashring)
* `--ring.vnode_count <n>` (default: `40`): The amount of virtual nodes per server.
* `--ring.max_cache_size <n>` (default: `5000`): We use a simple LRU cache inside the module to speed up frequent key lookups, you can customize the amount of keys that need to be cached.

### gossip options (express-swim)
* `--swim.verbose` (default: `false`): make the gossip protocol very verbose.
* `--swim.period_length` (default: `3000`): period length (in milliseconds).
* `--swim.ping_timeout` (default: `1000`): timeout of a ping request (in milliseconds).
* `--swim.failing_timeout` (default: `9000`): timeout of a suspected state before failing a node (in milliseconds).
* `--swim.message_ttl` (default: `10000`): the time a message should be kept in the message queue (in milliseconds).
* `--swim.pingreq_nodes` (default: `2`): number of random nodes to select for a ping-req.
* `--swim.tune_gossip` (default: `2`): tune maximum message retransmission (keep it "small").
* `--swim.gossip_messages` (default: `10`): max piggybacked messages per request.

## HTTP API

A node in dull is the string host:port.

### Cluster (express-swim)

#### join a node to a cluster
```
curl -X POST -d <active node> http://<joining node>/gossip/join
```

#### make a node leave the cluster
```
curl -X DELETE http://<leaving node>/gossip/leave
```

#### list active nodes in the cluster
```
curl http://<active node>/gossip/nodes
```

### Buckets (server/buckets.js)

#### create or update bucket and options
```
curl -X PUT -d <bucket options> http://<active node>/dull/bucket/<bucket name>
```

#### delete a bucket
```
curl -X DELETE http://<active node>/dull/bucket/<bucket name>
```

#### list buckets
```
curl http://<active node>/buckets/keys
curl http://<active node>/buckets/data
curl http://<active node>/buckets/values
```

### Data (server/data.js)

#### put a KV
```
curl -X PUT -d <value> http://<active node>/dull/bucket/<bucket name>/data/<key>
```

this defaults to Content-Type: application/json. You can put any other value type
like this:

```
curl -X PUT -d <value> -H 'Content-Type: <value type>' http://<active node>/dull/bucket/<bucket name>/data/<key>
```

or

```
curl -X PUT --data-binary @<file name> -H 'Content-Type: <value type>' http://127.0.0.1:3001/dull/bucket/<bucket name>/data/<key>
```

#### get a value
```
curl -v http://<active node>/dull/bucket/<bucket name>/data/<key>
```

you will get an header x-dull-vclock like this:

```
x-dull-vclock: {"127.0.0.1:3002":2,"127.0.0.1:3001":1}
```

that you should pass back updating a value like this:

```
curl -X PUT -d <value> -H 'Content-Type: <value type>' -H 'x-dull-vclock: {"127.0.0.1:3002":2,"127.0.0.1:3001":1}' http://<active node>/dull/bucket/<bucket name>/data/<key>
```

those are used in read-repair.

#### delete a KV
```
curl -X DELETE http://<active node>/dull/bucket/<bucket name>/data/<key>
```

you will get an header x-dull-vclock like this:

```
x-dull-vclock: {"127.0.0.1:3002":2,"127.0.0.1:3001":1}
```

that you should pass back if you want to recreate the value like this:

```
curl -X PUT -d <value> -H 'Content-Type: <value type>' -H 'x-dull-vclock: {"127.0.0.1:3002":2,"127.0.0.1:3001":1}' http://<active node>/dull/bucket/<bucket name>/data/<key>
```

Deleted objects are marked with a thumbstone record that may be in conflict with a put
if you don't pass in a vector-clock. This thumbstone record is also used when generating keys:
dull will return only non-deleted keys as one would expect, resolving those vector clocks.
It will return also keys that has conflicts, so that the client may have the chance to decide it.

#### list all keys known to dull
```
curl http://<active node>/dull/bucket/<bucket name>/keys
```

### custom http headers

#### x-dull-clientid

Used to identify the actor in vector clocks, if no actor is passed in
an uuid is used.

#### x-dull-vclock

used to pass vclocks between client and the server: the server will return
a vector clock in read or delete of a key, that should be used to modify the key.

#### x-dull-thumbstone

it is used to identify a deleted key sibling on a 303 response.

### Access to any node leveldb

You can check [multilevel-http](https://github.com/juliangruber/multilevel-http#http-api)

#### Buckets
```
curl http://<active node>/buckets/<multilevel-http path>
```

#### Data
```
curl http://<active node>/mnt/<bucket name>/<multilevel-http path>
```

## Things to implement

### Vclock
http://basho.com/why-vector-clocks-are-hard/
* vclock header compression

### CRDTs
http://vimeo.com/43903960
https://github.com/aphyr/meangirls
http://docs.basho.com/riak/latest/dev/using/data-types/

### Tests
https://github.com/aphyr/jepsen

### Active anti-entropy
http://en.wikipedia.org/wiki/Merkle_tree
https://github.com/c-geek/merkle

### Hinted hand-off
leveldb range of keys to push to a node when it comes back
