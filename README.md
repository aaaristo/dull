dull
====

Playing around with multilevel-http, node-hashring and express-swim
to build a riak-like distributed leveldb


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
$ nodemon index --port 3004 --path ./data/node4
$ curl -X POST -d '127.0.0.1:3001' http://localhost:3004/gossip/join
$ curl -X DELETE http://localhost:3004/gossip/leave
$ curl http://localhost:3002/dull/bucket/people/keys
$ curl -X PUT --data-binary @examples/v8.png -H 'Content-Type: image/png' http://127.0.0.1:3001/dull/bucket/people/data/v8.png
```
