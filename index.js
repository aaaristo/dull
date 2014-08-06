var argv= require('optimist').argv, 
    levelup = require('levelup'),
    _= require('underscore'),
    HashRing = require('hashring');

var node= _.defaults(argv,{ host: '127.0.0.1', port: 3000, path: './data/dull.db', cap: { n: 3, w: 2, r: 2 } }),
    app = require('multilevel-http').server(levelup(node.path, {valueEncoding: 'json'}));

node.string= [node.host,node.port].join(':');
node.ring= new HashRing(node.string);

require('./server/ring')(app,node);
require('./server/data')(app,node);

app.listen(node.port,node.host);
console.log('listening '+node.string);
