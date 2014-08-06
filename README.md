dull
====

Playing around with multilevel-http and node-hashring to better understand riak


```sh
$ git clone git@github.com:aaaristo/dull.git
$ nodemon index #--port 3000 --path ./data/dull.db
$ nodemon index --port 3001 --path ./data/dull.db1
$ nodemon index --port 3002 --path ./data/dull.db2
$ nodemon index --port 3003 --path ./data/dull.db3
$ nodemon recluster
$ curl -XPUT -d '{ "name": "Andrea", "lastname": "Gariboldi", age: 33 }' -H 'Content-Type: application/json' http://localhost:3002/dull/data/andrea
$ curl http://localhost:3001/dull/data/andrea?r=2
$ curl -X DELETE http://localhost:3000/dull/data/andrea
```
