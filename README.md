dull
====

Playing around with multilevel-http and node-hashring to better understand riak


```sh
$ nodemon index #--port 3000 --path ./data/dull.db
$ nodemon index --port 3001 --path ./data/dull.db1
$ nodemon index --port 3002 --path ./data/dull.db2
$ nodemon recluster
```
